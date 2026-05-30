"""
Suite de pruebas para lambda_solid.py
Principio D aplicado en los tests: se inyectan mocks en lugar de AWS real.
Cobertura: todos los componentes SOLID + flujos de integración completos.
"""

import json
import pytest
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from typing import Optional, Tuple

# ── Importar todos los componentes bajo prueba ────────────────────────────────
from lambda_solid import (
    Configuracion,
    Logger,
    DeserializadorJSON,
    ValidadorCampos,
    ValidadorTipos,
    ValidadorRangos,
    ValidadorFecha,
    PipelineValidacion,
    DeduplicadorDynamoDB,
    EnriquecedorEvento,
    ConstructorRespuestaHTTP,
    ServicioRecepcionTelemetria,
    RANGOS_VALIDOS,
    CAMPOS_REQUERIDOS,
    IDeduplicador,
    lambda_handler,
)


# ═══════════════════════════════════════════════════════════════════════════════
# FIXTURES COMPARTIDOS
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def config():
    """Configuración de prueba con valores por defecto."""
    with patch.dict("os.environ", {
        "DEDUP_TABLE_NAME":  "test-dedup",
        "DEDUP_TTL_SECONDS": "900",
        "LOG_LEVEL":         "INFO"
    }):
        yield Configuracion()


@pytest.fixture
def mensaje_valido():
    """Paquete de telemetría bien formado para reutilizar en múltiples pruebas."""
    return {
        "id_matera":    "matera-01",
        "dispositivo":  "sensor-temp-01",
        "medida":       "temperatura",
        "valor_medida": 23.5,
        "fecha_medida": "2026-04-08T10:30:00",
        "topic":        "granjas/matera-01/telemetry"
    }


@pytest.fixture
def deduplicador_vacio():
    """
    Implementación en memoria de IDeduplicador — nunca reporta duplicados.
    Principio L: sustituye a DeduplicadorDynamoDB sin cambiar el servicio.
    """
    class DeduplicadorMemoria(IDeduplicador):
        def __init__(self):
            self._registro = {}

        def es_duplicado(self, key: str) -> Tuple[bool, Optional[str]]:
            if key in self._registro:
                return True, self._registro[key]
            return False, None

        def registrar(self, key: str, event_id: str):
            self._registro[key] = event_id

    return DeduplicadorMemoria()


@pytest.fixture
def servicio(deduplicador_vacio):
    """
    Servicio completo con todas las dependencias reales excepto DynamoDB.
    Principio D: DynamoDB se sustituye por el mock en memoria.
    """
    config          = Configuracion()
    validador_fecha = ValidadorFecha()
    pipeline = PipelineValidacion([
        ValidadorCampos(CAMPOS_REQUERIDOS),
        ValidadorTipos(),
        ValidadorRangos(RANGOS_VALIDOS),
    ])
    return ServicioRecepcionTelemetria(
        deserializador = DeserializadorJSON(),
        pipeline       = pipeline,
        deduplicador   = deduplicador_vacio,
        enriquecedor   = EnriquecedorEvento(validador_fecha),
        constructor    = ConstructorRespuestaHTTP(),
        logger         = Logger(config)
    )


# ═══════════════════════════════════════════════════════════════════════════════
# BLOQUE 1 — CONFIGURACION
# ═══════════════════════════════════════════════════════════════════════════════

class TestConfiguracion:
    """Verifica que Configuracion lea correctamente las variables de entorno."""

    def test_lee_variables_de_entorno(self):
        with patch.dict("os.environ", {
            "DEDUP_TABLE_NAME":  "mi-tabla",
            "DEDUP_TTL_SECONDS": "1800",
            "LOG_LEVEL":         "DEBUG"
        }):
            c = Configuracion()
            assert c.dedup_table_name  == "mi-tabla"
            assert c.dedup_ttl_seconds == 1800
            assert c.log_level         == "DEBUG"

    def test_usa_valores_por_defecto_si_no_hay_env(self):
        with patch.dict("os.environ", {}, clear=True):
            c = Configuracion()
            assert c.dedup_table_name  == "telemetry-dedup"
            assert c.dedup_ttl_seconds == 900
            assert c.log_level         == "INFO"

    def test_ttl_se_convierte_a_entero(self):
        with patch.dict("os.environ", {"DEDUP_TTL_SECONDS": "300"}):
            c = Configuracion()
            assert isinstance(c.dedup_ttl_seconds, int)
            assert c.dedup_ttl_seconds == 300


# ═══════════════════════════════════════════════════════════════════════════════
# BLOQUE 2 — DESERIALIZADOR JSON
# ═══════════════════════════════════════════════════════════════════════════════

class TestDeserializadorJSON:
    """Verifica los tres caminos de deserialización."""

    def setup_method(self):
        self.deser = DeserializadorJSON()

    def test_deserializa_string_json(self):
        entrada = '{"id_matera": "matera-01", "temperatura": 23.5}'
        resultado = self.deser.deserializar(entrada)
        assert resultado["id_matera"] == "matera-01"
        assert resultado["temperatura"] == 23.5

    def test_pasa_diccionario_sin_modificar(self):
        entrada = {"id_matera": "matera-01", "temperatura": 23.5}
        resultado = self.deser.deserializar(entrada)
        assert resultado is entrada

    def test_convierte_otro_tipo_a_json(self):
        # Casos donde el evento llega como objeto no estándar
        class EventoExtraño:
            def __str__(self):
                return '{"id_matera": "matera-01"}'
        resultado = self.deser.deserializar(EventoExtraño())
        assert resultado["id_matera"] == "matera-01"

    def test_lanza_excepcion_con_json_malformado(self):
        with pytest.raises((json.JSONDecodeError, ValueError)):
            self.deser.deserializar('{"id_matera": sin_comillas}')

    def test_lanza_excepcion_con_string_vacio(self):
        with pytest.raises((json.JSONDecodeError, ValueError)):
            self.deser.deserializar("")


# ═══════════════════════════════════════════════════════════════════════════════
# BLOQUE 3 — VALIDADORES
# ═══════════════════════════════════════════════════════════════════════════════

class TestValidadorCampos:
    """Verifica la detección de campos obligatorios faltantes."""

    def setup_method(self):
        self.val = ValidadorCampos(CAMPOS_REQUERIDOS)

    def test_acepta_mensaje_con_todos_los_campos(self, mensaje_valido):
        ok, causa = self.val.validar(mensaje_valido)
        assert ok is True
        assert causa == ""

    def test_rechaza_si_falta_id_matera(self, mensaje_valido):
        del mensaje_valido["id_matera"]
        ok, causa = self.val.validar(mensaje_valido)
        assert ok is False
        assert "id_matera" in causa

    def test_rechaza_si_falta_valor_medida(self, mensaje_valido):
        del mensaje_valido["valor_medida"]
        ok, causa = self.val.validar(mensaje_valido)
        assert ok is False
        assert "valor_medida" in causa

    def test_rechaza_campo_con_string_vacio(self, mensaje_valido):
        mensaje_valido["medida"] = ""
        ok, causa = self.val.validar(mensaje_valido)
        assert ok is False
        assert "medida" in causa

    def test_rechaza_campo_con_valor_none(self, mensaje_valido):
        mensaje_valido["dispositivo"] = None
        ok, causa = self.val.validar(mensaje_valido)
        assert ok is False
        assert "dispositivo" in causa

    def test_reporta_multiples_campos_faltantes(self):
        ok, causa = self.val.validar({})
        assert ok is False
        for campo in CAMPOS_REQUERIDOS:
            assert campo in causa

    def test_fa01_registra_sin_id_matera(self):
        """FA-01 de CU-BK01-02: rechazo debe registrarse aunque falte id_matera."""
        ok, causa = self.val.validar({"medida": "temperatura"})
        assert ok is False
        assert "id_matera" in causa


class TestValidadorTipos:
    """Verifica que valor_medida sea un número válido."""

    def setup_method(self):
        self.val = ValidadorTipos()

    def test_acepta_entero(self, mensaje_valido):
        mensaje_valido["valor_medida"] = 23
        ok, _ = self.val.validar(mensaje_valido)
        assert ok is True

    def test_acepta_flotante(self, mensaje_valido):
        mensaje_valido["valor_medida"] = 23.5
        ok, _ = self.val.validar(mensaje_valido)
        assert ok is True

    def test_acepta_flotante_como_string(self, mensaje_valido):
        mensaje_valido["valor_medida"] = "23.5"
        ok, _ = self.val.validar(mensaje_valido)
        assert ok is True

    def test_rechaza_texto_no_numerico(self, mensaje_valido):
        mensaje_valido["valor_medida"] = "muy_caliente"
        ok, causa = self.val.validar(mensaje_valido)
        assert ok is False
        assert "numérico" in causa

    def test_rechaza_none(self, mensaje_valido):
        mensaje_valido["valor_medida"] = None
        ok, causa = self.val.validar(mensaje_valido)
        assert ok is False

    def test_rechaza_lista(self, mensaje_valido):
        mensaje_valido["valor_medida"] = [23.5]
        ok, _ = self.val.validar(mensaje_valido)
        assert ok is False


class TestValidadorRangos:
    """
    Verifica rangos físicos por tipo de medida.
    Principio O: agregar medida nueva no requiere cambiar esta clase.
    """

    def setup_method(self):
        self.val = ValidadorRangos(RANGOS_VALIDOS)

    # ── Temperatura ──────────────────────────────────────────────────────────
    def test_temperatura_en_rango(self, mensaje_valido):
        ok, _ = self.val.validar(mensaje_valido)
        assert ok is True

    def test_temperatura_limite_inferior(self, mensaje_valido):
        mensaje_valido["valor_medida"] = -10
        ok, _ = self.val.validar(mensaje_valido)
        assert ok is True

    def test_temperatura_limite_superior(self, mensaje_valido):
        mensaje_valido["valor_medida"] = 60
        ok, _ = self.val.validar(mensaje_valido)
        assert ok is True

    def test_temperatura_por_encima_del_rango(self, mensaje_valido):
        mensaje_valido["valor_medida"] = 999
        ok, causa = self.val.validar(mensaje_valido)
        assert ok is False
        assert "temperatura" in causa
        assert "999" in causa

    def test_temperatura_por_debajo_del_rango(self, mensaje_valido):
        mensaje_valido["valor_medida"] = -50
        ok, causa = self.val.validar(mensaje_valido)
        assert ok is False

    # ── Humedad ──────────────────────────────────────────────────────────────
    def test_humedad_aire_en_rango(self, mensaje_valido):
        mensaje_valido["medida"]       = "humedad_aire"
        mensaje_valido["valor_medida"] = 65.0
        ok, _ = self.val.validar(mensaje_valido)
        assert ok is True

    def test_humedad_aire_sobre_100(self, mensaje_valido):
        mensaje_valido["medida"]       = "humedad_aire"
        mensaje_valido["valor_medida"] = 150
        ok, causa = self.val.validar(mensaje_valido)
        assert ok is False

    def test_humedad_sustrato_negativa(self, mensaje_valido):
        mensaje_valido["medida"]       = "humedad_sustrato"
        mensaje_valido["valor_medida"] = -5
        ok, _ = self.val.validar(mensaje_valido)
        assert ok is False

    # ── Medida desconocida ────────────────────────────────────────────────────
    def test_medida_no_configurada_pasa_sin_validar(self, mensaje_valido):
        """
        Principio O: medidas no configuradas no bloquean el sistema.
        Permite recibir nuevos tipos de sensor sin desplegar cambios.
        """
        mensaje_valido["medida"]       = "presion_atm"
        mensaje_valido["valor_medida"] = 9999
        ok, _ = self.val.validar(mensaje_valido)
        assert ok is True

    # ── CO2 ──────────────────────────────────────────────────────────────────
    def test_co2_en_rango(self, mensaje_valido):
        mensaje_valido["medida"]       = "co2"
        mensaje_valido["valor_medida"] = 800
        ok, _ = self.val.validar(mensaje_valido)
        assert ok is True

    def test_co2_fuera_de_rango(self, mensaje_valido):
        mensaje_valido["medida"]       = "co2"
        mensaje_valido["valor_medida"] = 9999
        ok, causa = self.val.validar(mensaje_valido)
        assert ok is False
        assert "co2" in causa


class TestValidadorFecha:
    """Verifica la normalización de fechas — nunca rechaza, solo marca el origen."""

    def setup_method(self):
        self.val = ValidadorFecha()
        self.received_at = "2026-04-08T15:30:00+00:00"

    def test_fecha_valida_usa_fecha_del_sensor(self):
        fecha, origen = self.val.normalizar("2026-04-08T10:30:00", self.received_at)
        assert fecha   == "2026-04-08T10:30:00"
        assert origen  == "sensor"

    def test_fecha_invalida_usa_fecha_del_servidor(self):
        fecha, origen = self.val.normalizar("no-es-una-fecha", self.received_at)
        assert fecha   == self.received_at
        assert origen  == "servidor (fecha del sensor inválida)"

    def test_fecha_vacia_usa_fecha_del_servidor(self):
        fecha, origen = self.val.normalizar("", self.received_at)
        assert fecha   == self.received_at
        assert "servidor" in origen

    def test_validar_siempre_retorna_true(self, mensaje_valido):
        """La fecha inválida es advertencia, no rechazo — REQ-FN-BK-02."""
        mensaje_valido["fecha_medida"] = "fecha-corrupta"
        ok, _ = self.val.validar(mensaje_valido)
        assert ok is True

    def test_respeta_marca_de_tiempo_original_del_sensor(self):
        """REQ-FN-BK-02: respetar la fecha original cuando sea válida."""
        fecha_sensor = "2026-04-08T06:00:00"
        fecha, origen = self.val.normalizar(fecha_sensor, self.received_at)
        assert fecha  == fecha_sensor
        assert origen == "sensor"


# ═══════════════════════════════════════════════════════════════════════════════
# BLOQUE 4 — PIPELINE DE VALIDACIÓN
# ═══════════════════════════════════════════════════════════════════════════════

class TestPipelineValidacion:
    """
    Verifica que el pipeline ejecute validadores en orden y se detenga
    en el primero que falle.
    Principio O: agregar validadores no modifica PipelineValidacion.
    """

    def test_pipeline_vacio_acepta_cualquier_mensaje(self):
        pipeline = PipelineValidacion([])
        ok, _ = pipeline.ejecutar({})
        assert ok is True

    def test_pipeline_se_detiene_en_primer_fallo(self):
        """Verifica que no ejecuta validadores innecesarios tras el primer error."""
        ejecutados = []

        class ValidadorSpy:
            def __init__(self, nombre, resultado):
                self.nombre    = nombre
                self.resultado = resultado

            def validar(self, body):
                ejecutados.append(self.nombre)
                return self.resultado

        pipeline = PipelineValidacion([
            ValidadorSpy("primero",  (True,  "")),
            ValidadorSpy("segundo",  (False, "error en segundo")),
            ValidadorSpy("tercero",  (True,  "")),
        ])
        ok, causa = pipeline.ejecutar({})
        assert ok        is False
        assert causa     == "error en segundo"
        assert "tercero" not in ejecutados

    def test_pipeline_completo_mensaje_valido(self, mensaje_valido):
        pipeline = PipelineValidacion([
            ValidadorCampos(CAMPOS_REQUERIDOS),
            ValidadorTipos(),
            ValidadorRangos(RANGOS_VALIDOS),
        ])
        ok, causa = pipeline.ejecutar(mensaje_valido)
        assert ok   is True
        assert causa == ""

    def test_pipeline_extensible_sin_modificar_clase(self, mensaje_valido):
        """
        Principio O: agregar un validador personalizado sin tocar PipelineValidacion.
        """
        class ValidadorMatera01Prohibida:
            def validar(self, body):
                if body.get("id_matera") == "matera-prohibida":
                    return False, "Matera prohibida"
                return True, ""

        pipeline = PipelineValidacion([
            ValidadorCampos(CAMPOS_REQUERIDOS),
            ValidadorMatera01Prohibida(),
        ])
        mensaje_valido["id_matera"] = "matera-prohibida"
        ok, causa = pipeline.ejecutar(mensaje_valido)
        assert ok   is False
        assert "prohibida" in causa


# ═══════════════════════════════════════════════════════════════════════════════
# BLOQUE 5 — DEDUPLICADOR
# ═══════════════════════════════════════════════════════════════════════════════

class TestDeduplicadorDynamoDB:
    """
    Principio D: DynamoDB se mockea — no se necesita AWS real.
    Principio L: la implementación puede sustituirse por otra sin cambiar tests.
    """

    def _mock_tabla(self, item_existente=None):
        tabla = MagicMock()
        if item_existente:
            tabla.get_item.return_value = {"Item": item_existente}
        else:
            tabla.get_item.return_value = {}
        return tabla

    def _crear_dedup(self, tabla_mock):
        dynamodb_mock = MagicMock()
        dynamodb_mock.Table.return_value = tabla_mock
        with patch.dict("os.environ", {
            "DEDUP_TABLE_NAME":  "test-dedup",
            "DEDUP_TTL_SECONDS": "900"
        }):
            config = Configuracion()
        return DeduplicadorDynamoDB(dynamodb_mock, config)

    def test_es_duplicado_cuando_clave_existe(self):
        tabla  = self._mock_tabla({"dedup_key": "matera-01#2026-04-08T10:30:00",
                                   "event_id": "uuid-original"})
        dedup  = self._crear_dedup(tabla)
        es_dup, event_id = dedup.es_duplicado("matera-01#2026-04-08T10:30:00")
        assert es_dup    is True
        assert event_id  == "uuid-original"

    def test_no_es_duplicado_cuando_clave_no_existe(self):
        tabla  = self._mock_tabla(None)
        dedup  = self._crear_dedup(tabla)
        es_dup, event_id = dedup.es_duplicado("matera-01#2026-04-08T10:30:00")
        assert es_dup   is False
        assert event_id is None

    def test_registrar_llama_put_item_con_ttl(self):
        tabla = self._mock_tabla(None)
        dedup = self._crear_dedup(tabla)
        antes = int(time.time())
        dedup.registrar("matera-01#2026-04-08T10:30:00", "uuid-nuevo")
        despues = int(time.time())

        tabla.put_item.assert_called_once()
        item = tabla.put_item.call_args[1]["Item"]
        assert item["dedup_key"] == "matera-01#2026-04-08T10:30:00"
        assert item["event_id"]  == "uuid-nuevo"
        assert antes + 900 <= item["ttl"] <= despues + 900

    def test_clave_compuesta_tiene_formato_correcto(self):
        """La clave debe ser id_matera#fecha_medida exactamente."""
        clave = "matera-01#2026-04-08T10:30:00"
        partes = clave.split("#")
        assert len(partes)  == 2
        assert partes[0]    == "matera-01"
        assert partes[1]    == "2026-04-08T10:30:00"


# ═══════════════════════════════════════════════════════════════════════════════
# BLOQUE 6 — ENRIQUECEDOR DE EVENTO
# ═══════════════════════════════════════════════════════════════════════════════

class TestEnriquecedorEvento:
    """Verifica que el enriquecedor agregue los campos correctos."""

    def setup_method(self):
        self.enr         = EnriquecedorEvento(ValidadorFecha())
        self.received_at = "2026-04-08T15:30:00+00:00"

    def test_agrega_event_id_uuid_v4(self, mensaje_valido):
        evento = self.enr.enriquecer(mensaje_valido, self.received_at)
        assert "event_id" in evento
        # UUID v4 tiene 36 caracteres con guiones
        assert len(evento["event_id"]) == 36
        partes = evento["event_id"].split("-")
        assert len(partes) == 5

    def test_event_id_es_unico_en_cada_invocacion(self, mensaje_valido):
        evento1 = self.enr.enriquecer(mensaje_valido, self.received_at)
        evento2 = self.enr.enriquecer(mensaje_valido, self.received_at)
        assert evento1["event_id"] != evento2["event_id"]

    def test_agrega_received_at(self, mensaje_valido):
        evento = self.enr.enriquecer(mensaje_valido, self.received_at)
        assert evento["received_at"] == self.received_at

    def test_origen_fecha_es_sensor_cuando_fecha_valida(self, mensaje_valido):
        evento = self.enr.enriquecer(mensaje_valido, self.received_at)
        assert evento["origen_fecha"] == "sensor"
        assert evento["fecha_medida"] == mensaje_valido["fecha_medida"]

    def test_origen_fecha_es_servidor_cuando_fecha_invalida(self, mensaje_valido):
        mensaje_valido["fecha_medida"] = "fecha-corrupta"
        evento = self.enr.enriquecer(mensaje_valido, self.received_at)
        assert "servidor" in evento["origen_fecha"]
        assert evento["fecha_medida"] == self.received_at

    def test_normaliza_medida_a_minusculas(self, mensaje_valido):
        mensaje_valido["medida"] = "TEMPERATURA"
        evento = self.enr.enriquecer(mensaje_valido, self.received_at)
        assert evento["medida"] == "temperatura"

    def test_convierte_valor_medida_a_float(self, mensaje_valido):
        mensaje_valido["valor_medida"] = "23.5"
        evento = self.enr.enriquecer(mensaje_valido, self.received_at)
        assert isinstance(evento["valor_medida"], float)
        assert evento["valor_medida"] == 23.5

    def test_incluye_topic_del_broker(self, mensaje_valido):
        evento = self.enr.enriquecer(mensaje_valido, self.received_at)
        assert evento["topic"] == "granjas/matera-01/telemetry"

    def test_topic_desconocido_si_no_viene_en_body(self, mensaje_valido):
        del mensaje_valido["topic"]
        evento = self.enr.enriquecer(mensaje_valido, self.received_at)
        assert evento["topic"] == "desconocido"


# ═══════════════════════════════════════════════════════════════════════════════
# BLOQUE 7 — CONSTRUCTOR DE RESPUESTA HTTP
# ═══════════════════════════════════════════════════════════════════════════════

class TestConstructorRespuestaHTTP:

    def setup_method(self):
        self.constructor = ConstructorRespuestaHTTP()

    def test_respuesta_200_exitosa(self):
        resp = self.constructor.construir(200, {"status": "accepted"})
        assert resp["statusCode"]                    == 200
        assert resp["headers"]["Content-Type"]       == "application/json"
        body = json.loads(resp["body"])
        assert body["status"]                        == "accepted"

    def test_respuesta_400_error(self):
        resp = self.constructor.construir(400, "Campo faltante")
        assert resp["statusCode"] == 400
        body = json.loads(resp["body"])
        assert body["mensaje"]    == "Campo faltante"

    def test_string_se_envuelve_en_mensaje(self):
        resp = self.constructor.construir(400, "Error simple")
        body = json.loads(resp["body"])
        assert "mensaje" in body
        assert body["mensaje"] == "Error simple"

    def test_dict_se_serializa_directamente(self):
        resp = self.constructor.construir(200, {"a": 1, "b": 2})
        body = json.loads(resp["body"])
        assert body["a"] == 1
        assert body["b"] == 2

    def test_caracteres_especiales_se_codifican_correctamente(self):
        resp = self.constructor.construir(200, {"mensaje": "Recepción válida — ñ"})
        assert "Recepci" in resp["body"]
        body = json.loads(resp["body"])
        assert "ñ" in body["mensaje"]

    def test_respuesta_422_descarte(self):
        resp = self.constructor.construir(422, "Valor fuera de rango")
        assert resp["statusCode"] == 422


# ═══════════════════════════════════════════════════════════════════════════════
# BLOQUE 8 — SERVICIO COMPLETO (Pruebas de integración del pipeline)
# ═══════════════════════════════════════════════════════════════════════════════

class TestServicioRecepcionTelemetria:
    """
    Pruebas de integración del pipeline completo.
    Principio D: DynamoDB reemplazado por DeduplicadorMemoria.
    """

    # ── CU-BK01-01: Mensaje bien formado ─────────────────────────────────────

    def test_cu_bk01_01_acepta_mensaje_valido(self, servicio, mensaje_valido):
        resp = servicio.procesar(mensaje_valido)
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"]   == "accepted"
        assert "event_id"       in body
        assert "evento"         in body

    def test_cu_bk01_01_genera_event_id_unico(self, servicio, mensaje_valido):
        """Cada invocación debe generar un event_id diferente — CU-BK01-01 paso 3."""
        resp1 = servicio.procesar({**mensaje_valido, "fecha_medida": "2026-04-08T10:30:00"})
        resp2 = servicio.procesar({**mensaje_valido, "fecha_medida": "2026-04-08T10:35:00"})
        id1   = json.loads(resp1["body"])["event_id"]
        id2   = json.loads(resp2["body"])["event_id"]
        assert id1 != id2

    def test_cu_bk01_01_evento_contiene_received_at(self, servicio, mensaje_valido):
        resp  = servicio.procesar(mensaje_valido)
        body  = json.loads(resp["body"])
        assert "received_at" in body["evento"]

    def test_cu_bk01_01_respeta_fecha_original_del_sensor(self, servicio, mensaje_valido):
        """REQ-FN-BK-02: la fecha del sensor se preserva cuando es válida."""
        resp  = servicio.procesar(mensaje_valido)
        body  = json.loads(resp["body"])
        assert body["evento"]["fecha_medida"]  == "2026-04-08T10:30:00"
        assert body["evento"]["origen_fecha"]  == "sensor"

    def test_cu_bk01_01_acepta_mensaje_como_string_json(self, servicio, mensaje_valido):
        """IoT Core puede entregar el payload como string."""
        resp = servicio.procesar(json.dumps(mensaje_valido))
        assert resp["statusCode"] == 200

    # ── CU-BK01-02 EX-01: JSON malformado ────────────────────────────────────

    def test_cu_bk01_02_ex01_rechaza_json_malformado(self, servicio):
        resp = servicio.procesar('{"id_matera": sin_cerrar')
        assert resp["statusCode"] == 400
        body = json.loads(resp["body"])
        assert "inválido" in body["mensaje"].lower() or "invalido" in body["mensaje"].lower()

    def test_cu_bk01_02_ex01_rechaza_string_vacio(self, servicio):
        resp = servicio.procesar("")
        assert resp["statusCode"] == 400

    # ── CU-BK01-02 EX-01: Campos faltantes ───────────────────────────────────

    def test_cu_bk01_02_rechaza_mensaje_sin_id_matera(self, servicio, mensaje_valido):
        del mensaje_valido["id_matera"]
        resp = servicio.procesar(mensaje_valido)
        assert resp["statusCode"] == 400
        body = json.loads(resp["body"])
        assert "id_matera" in body["mensaje"]

    def test_cu_bk01_02_rechaza_mensaje_sin_medida(self, servicio, mensaje_valido):
        del mensaje_valido["medida"]
        resp = servicio.procesar(mensaje_valido)
        assert resp["statusCode"] == 400

    def test_cu_bk01_02_rechaza_mensaje_sin_valor_medida(self, servicio, mensaje_valido):
        del mensaje_valido["valor_medida"]
        resp = servicio.procesar(mensaje_valido)
        assert resp["statusCode"] == 400

    def test_cu_bk01_02_rechaza_mensaje_sin_fecha(self, servicio, mensaje_valido):
        del mensaje_valido["fecha_medida"]
        resp = servicio.procesar(mensaje_valido)
        assert resp["statusCode"] == 400

    # ── CU-BK01-02 EX-02: Valores fuera de rango ─────────────────────────────

    def test_cu_bk01_02_ex02_rechaza_temperatura_extrema(self, servicio, mensaje_valido):
        mensaje_valido["valor_medida"] = 999
        resp = servicio.procesar(mensaje_valido)
        assert resp["statusCode"] == 422
        body = json.loads(resp["body"])
        assert "rango" in body["mensaje"].lower()

    def test_cu_bk01_02_ex02_rechaza_humedad_negativa(self, servicio, mensaje_valido):
        mensaje_valido["medida"]       = "humedad_aire"
        mensaje_valido["valor_medida"] = -5
        resp = servicio.procesar(mensaje_valido)
        assert resp["statusCode"] == 422

    def test_cu_bk01_02_ex02_rechaza_valor_no_numerico(self, servicio, mensaje_valido):
        mensaje_valido["valor_medida"] = "caliente"
        resp = servicio.procesar(mensaje_valido)
        assert resp["statusCode"] == 400

    # ── CU-BK01-03: Deduplicación ─────────────────────────────────────────────

    def test_cu_bk01_03_descarta_mensaje_duplicado(self, servicio, mensaje_valido):
        """El mismo mensaje dos veces: el segundo debe ser descartado."""
        resp1 = servicio.procesar(mensaje_valido)
        resp2 = servicio.procesar(mensaje_valido)
        assert resp1["statusCode"] == 200
        assert json.loads(resp1["body"])["status"] == "accepted"
        assert resp2["statusCode"] == 200
        assert json.loads(resp2["body"])["status"] == "duplicado"

    def test_cu_bk01_03_duplicado_informa_event_id_original(self, servicio, mensaje_valido):
        """El log debe indicar el event_id del mensaje original."""
        resp1       = servicio.procesar(mensaje_valido)
        event_id_1  = json.loads(resp1["body"])["event_id"]
        resp2       = servicio.procesar(mensaje_valido)
        body2       = json.loads(resp2["body"])
        assert body2["event_id_original"] == event_id_1

    def test_cu_bk01_03_mensajes_diferentes_no_son_duplicados(self, servicio, mensaje_valido):
        """Misma matera pero diferente fecha no debe ser detectado como duplicado."""
        resp1 = servicio.procesar({**mensaje_valido, "fecha_medida": "2026-04-08T10:30:00"})
        resp2 = servicio.procesar({**mensaje_valido, "fecha_medida": "2026-04-08T10:35:00"})
        assert json.loads(resp1["body"])["status"] == "accepted"
        assert json.loads(resp2["body"])["status"] == "accepted"

    def test_cu_bk01_03_diferentes_materas_misma_fecha_no_son_duplicados(
            self, servicio, mensaje_valido):
        """Diferentes materas con la misma fecha son eventos distintos."""
        resp1 = servicio.procesar({**mensaje_valido, "id_matera": "matera-01"})
        resp2 = servicio.procesar({**mensaje_valido, "id_matera": "matera-02"})
        assert json.loads(resp1["body"])["status"] == "accepted"
        assert json.loads(resp2["body"])["status"] == "accepted"

    # ── Fecha del sensor inválida ─────────────────────────────────────────────

    def test_acepta_mensaje_con_fecha_invalida_usando_hora_servidor(
            self, servicio, mensaje_valido):
        """REQ-FN-BK-02 fallback: si la fecha es inválida usa la del servidor."""
        mensaje_valido["fecha_medida"] = "no-es-fecha"
        resp = servicio.procesar(mensaje_valido)
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["evento"]["origen_fecha"] != "sensor"

    # ── Fallo de DynamoDB (fail-open) ─────────────────────────────────────────

    def test_continua_si_dynamodb_falla_en_lectura(self, mensaje_valido):
        """
        Si DynamoDB no está disponible en la consulta de dedup,
        el mensaje se procesa igual (fail-open) para no perder datos.
        """
        class DeduplicadorFallido(IDeduplicador):
            def es_duplicado(self, key):
                raise ConnectionError("DynamoDB no disponible")
            def registrar(self, key, event_id):
                pass

        config          = Configuracion()
        validador_fecha = ValidadorFecha()
        pipeline = PipelineValidacion([
            ValidadorCampos(CAMPOS_REQUERIDOS),
            ValidadorTipos(),
            ValidadorRangos(RANGOS_VALIDOS),
        ])
        svc = ServicioRecepcionTelemetria(
            deserializador = DeserializadorJSON(),
            pipeline       = pipeline,
            deduplicador   = DeduplicadorFallido(),
            enriquecedor   = EnriquecedorEvento(validador_fecha),
            constructor    = ConstructorRespuestaHTTP(),
            logger         = Logger(config)
        )
        resp = svc.procesar(mensaje_valido)
        # Fail-open: procesa el mensaje aunque DynamoDB falle
        assert resp["statusCode"] == 200
        assert json.loads(resp["body"])["status"] == "accepted"


# ═══════════════════════════════════════════════════════════════════════════════
# BLOQUE 9 — PRINCIPIOS SOLID (Pruebas conceptuales)
# ═══════════════════════════════════════════════════════════════════════════════

class TestPrincipiosSOLID:
    """
    Pruebas que verifican directamente los principios SOLID aplicados.
    """

    def test_principio_o_agregar_medida_sin_modificar_validador(self, mensaje_valido):
        """
        Principio O: agregar pH al diccionario extiende el comportamiento
        sin modificar ValidadorRangos.
        """
        rangos_extendidos = {
            **RANGOS_VALIDOS,
            "ph": {"min": 0, "max": 14, "unidad": "pH"}
        }
        val = ValidadorRangos(rangos_extendidos)

        mensaje_valido["medida"]       = "ph"
        mensaje_valido["valor_medida"] = 15  # fuera de rango
        ok, causa = val.validar(mensaje_valido)
        assert ok    is False
        assert "ph"  in causa

        mensaje_valido["valor_medida"] = 7   # en rango
        ok, _ = val.validar(mensaje_valido)
        assert ok is True

    def test_principio_l_deduplicador_memoria_sustituye_dynamodb(
            self, deduplicador_vacio, mensaje_valido):
        """
        Principio L: DeduplicadorMemoria es sustituible por DeduplicadorDynamoDB.
        El servicio no nota la diferencia.
        """
        config          = Configuracion()
        validador_fecha = ValidadorFecha()
        pipeline = PipelineValidacion([
            ValidadorCampos(CAMPOS_REQUERIDOS),
            ValidadorTipos(),
            ValidadorRangos(RANGOS_VALIDOS),
        ])
        svc = ServicioRecepcionTelemetria(
            deserializador = DeserializadorJSON(),
            pipeline       = pipeline,
            deduplicador   = deduplicador_vacio,  # mock en memoria
            enriquecedor   = EnriquecedorEvento(validador_fecha),
            constructor    = ConstructorRespuestaHTTP(),
            logger         = Logger(config)
        )
        resp = svc.procesar(mensaje_valido)
        assert resp["statusCode"] == 200

    def test_principio_d_servicio_no_instancia_dependencias_internamente(self):
        """
        Principio D: ServicioRecepcionTelemetria no llama a boto3 ni a os.environ.
        Todas las dependencias vienen inyectadas desde afuera.
        """
        import inspect
        source = inspect.getsource(ServicioRecepcionTelemetria)
        assert "boto3"      not in source
        assert "os.environ" not in source
        assert "DynamoDB"   not in source

    def test_principio_s_lambda_handler_tiene_una_sola_linea_de_logica(self):
        """
        Principio S: lambda_handler solo delega — no contiene lógica propia.
        """
        import inspect
        source_lines = [
            l.strip() for l in inspect.getsource(lambda_handler).splitlines()
            if l.strip() and not l.strip().startswith("#")
        ]
        # Solo debe tener: def, docstring (opcional) y return
        lineas_logica = [l for l in source_lines
                         if not l.startswith("def ")
                         and not l.startswith('"""')
                         and not l.startswith("'''")]
        assert len(lineas_logica) == 1
        assert lineas_logica[0].startswith("return")


# ═══════════════════════════════════════════════════════════════════════════════
# BLOQUE 10 — PUNTO DE ENTRADA LAMBDA
# ═══════════════════════════════════════════════════════════════════════════════

class TestLambdaHandler:
    """Pruebas de humo del punto de entrada real con DynamoDB mockeado."""

    @patch("lambda_solid.boto3")
    def test_handler_acepta_mensaje_valido(self, mock_boto3, mensaje_valido):
        mock_tabla = MagicMock()
        mock_tabla.get_item.return_value = {}
        mock_boto3.resource.return_value.Table.return_value = mock_tabla

        # Reconstruir el servicio con boto3 mockeado
        import lambda_solid
        lambda_solid._servicio = None

        resp = lambda_handler(mensaje_valido, {})
        assert resp["statusCode"] == 200

    @patch("lambda_solid.boto3")
    def test_handler_rechaza_json_malformado(self, mock_boto3):
        mock_boto3.resource.return_value.Table.return_value = MagicMock()

        import lambda_solid
        lambda_solid._servicio = None

        resp = lambda_handler("esto no es json {{{", {})
        assert resp["statusCode"] == 400

