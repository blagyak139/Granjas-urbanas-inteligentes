import json
import uuid
import os
import logging
import boto3
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional, Tuple

DEDUP_TABLE_NAME   = os.environ.get("DEDUP_TABLE_NAME",  "telemetry-dedup")
DEDUP_TTL_SECONDS  = int(os.environ.get("DEDUP_TTL_SECONDS", "900"))
LOG_LEVEL          = os.environ.get("LOG_LEVEL", "INFO")
DATA_TABLE_NAME    = os.environ.get("DATA_TABLE_NAME", "telemetry-data")

logger_root = logging.getLogger()

RANGOS_VALIDOS = {
    "temperatura":      {"min": -10,    "max": 60,     "unidad": "C"},
    "humedad_aire":     {"min": 0,      "max": 100,    "unidad": "%"},
    "humedad_sustrato": {"min": 0,      "max": 100,    "unidad": "%"},
    "luminosidad":      {"min": 0,      "max": 100000, "unidad": "lux"},
    "co2":              {"min": 300,    "max": 5000,   "unidad": "ppm"},
}

CAMPOS_REQUERIDOS = ["id_matera", "dispositivo", "medida", "valor_medida", "fecha_medida"]


class Configuracion:
    def __init__(self):
        self.dedup_table_name  = os.environ.get("DEDUP_TABLE_NAME",  "telemetry-dedup")
        self.dedup_ttl_seconds = int(os.environ.get("DEDUP_TTL_SECONDS", "900"))
        self.log_level         = os.environ.get("LOG_LEVEL", "INFO")
        self.data_table_name   = os.environ.get("DATA_TABLE_NAME", "telemetry-data")


class Logger:
    def __init__(self, config):
        self._logger = logging.getLogger()
        self._logger.setLevel(getattr(logging, config.log_level, logging.INFO))

    def aceptado(self, evento):
        self._logger.info(json.dumps({**evento, "status": "accepted"}))

    def rechazado(self, id_matera, causa, detalle, received_at):
        self._logger.error(json.dumps({
            "event":       "telemetria_rechazada",
            "id_matera":   id_matera or "desconocido",
            "causa":       causa,
            "detalle":     detalle,
            "received_at": received_at,
            "status":      "rejected"
        }))

    def duplicado(self, id_matera, dedup_key, event_id_original, received_at):
        self._logger.warning(json.dumps({
            "event":             "mensaje_duplicado",
            "id_matera":         id_matera,
            "dedup_key":         dedup_key,
            "event_id_original": event_id_original,
            "received_at":       received_at
        }))

    def advertencia(self, datos):
        self._logger.warning(json.dumps(datos))

    def error(self, datos):
        self._logger.error(json.dumps(datos))


class IDeserializador(ABC):
    @abstractmethod
    def deserializar(self, event):
        pass


class IValidador(ABC):
    @abstractmethod
    def validar(self, body):
        pass


class IDeduplicador(ABC):
    @abstractmethod
    def es_duplicado(self, dedup_key):
        pass

    @abstractmethod
    def registrar(self, dedup_key, event_id):
        pass


class IEnriquecedor(ABC):
    @abstractmethod
    def enriquecer(self, body, received_at):
        pass


class IConstructorRespuesta(ABC):
    @abstractmethod
    def construir(self, codigo_http, contenido):
        pass


class IPersistencia(ABC):
    @abstractmethod
    def guardar(self, evento):
        pass


class DeserializadorJSON(IDeserializador):
    def deserializar(self, event):
        if isinstance(event, dict):
            return event
        if isinstance(event, str):
            return json.loads(event)
        return json.loads(str(event))


class ValidadorCampos(IValidador):
    def __init__(self, campos_requeridos):
        self._campos = campos_requeridos

    def validar(self, body):
        faltantes = [c for c in self._campos
                     if body.get(c) is None or body.get(c) == ""]
        if faltantes:
            return False, "Campos obligatorios faltantes: " + ", ".join(faltantes)
        return True, ""


class ValidadorTipos(IValidador):
    def validar(self, body):
        try:
            float(body.get("valor_medida", ""))
            return True, ""
        except (ValueError, TypeError):
            return False, "valor_medida no es numerico"


class ValidadorRangos(IValidador):
    def __init__(self, rangos):
        self._rangos = rangos

    def validar(self, body):
        medida = str(body.get("medida", "")).strip().lower()
        if medida not in self._rangos:
            return True, ""
        rango = self._rangos[medida]
        try:
            valor = float(body["valor_medida"])
        except (ValueError, TypeError):
            return True, ""
        if not (rango["min"] <= valor <= rango["max"]):
            causa = (medida + "=" + str(valor) + rango["unidad"] +
                     " fuera del rango valido [" +
                     str(rango["min"]) + ", " + str(rango["max"]) + "]")
            return False, causa
        return True, ""


class ValidadorFecha(IValidador):
    def validar(self, body):
        return True, ""

    def normalizar(self, fecha_str, received_at):
        try:
            datetime.fromisoformat(fecha_str.strip())
            return fecha_str.strip(), "sensor"
        except ValueError:
            return received_at, "servidor (fecha del sensor invalida)"


class PipelineValidacion:
    def __init__(self, validadores):
        self._validadores = validadores

    def ejecutar(self, body):
        for validador in self._validadores:
            es_valido, causa = validador.validar(body)
            if not es_valido:
                return False, causa
        return True, ""


class DeduplicadorDynamoDB(IDeduplicador):
    def __init__(self, dynamodb_resource, config):
        self._dynamodb = dynamodb_resource
        self._config   = config

    def es_duplicado(self, dedup_key):
        tabla     = self._dynamodb.Table(self._config.dedup_table_name)
        resultado = tabla.get_item(Key={"dedup_key": dedup_key})
        item      = resultado.get("Item")
        if item:
            return True, item.get("event_id")
        return False, None

    def registrar(self, dedup_key, event_id):
        tabla      = self._dynamodb.Table(self._config.dedup_table_name)
        ttl_expiry = int(time.time()) + self._config.dedup_ttl_seconds
        tabla.put_item(Item={
            "dedup_key": dedup_key,
            "event_id":  event_id,
            "ttl":       ttl_expiry
        })


class PersistenciaDynamoDB(IPersistencia):
    def __init__(self, dynamodb_resource, nombre_tabla):
        self._dynamodb     = dynamodb_resource
        self._nombre_tabla = nombre_tabla

    def guardar(self, evento):
        tabla = self._dynamodb.Table(self._nombre_tabla)
        tabla.put_item(Item={
            "id_matera":    evento["id_matera"],
            "fecha_medida": evento["fecha_medida"],
            "event_id":     evento["event_id"],
            "dispositivo":  evento["dispositivo"],
            "medida":       evento["medida"],
            "valor_medida": str(evento["valor_medida"]),
            "origen_fecha": evento["origen_fecha"],
            "received_at":  evento["received_at"],
            "topic":        evento.get("topic", "desconocido")
        })


class EnriquecedorEvento(IEnriquecedor):
    def __init__(self, validador_fecha):
        self._validador_fecha = validador_fecha

    def enriquecer(self, body, received_at):
        fecha_str = str(body.get("fecha_medida", "")).strip()
        fecha_medida, origen_fecha = self._validador_fecha.normalizar(
            fecha_str, received_at
        )
        return {
            "event_id":     str(uuid.uuid4()),
            "id_matera":    str(body["id_matera"]).strip(),
            "dispositivo":  str(body["dispositivo"]).strip(),
            "medida":       str(body["medida"]).strip().lower(),
            "valor_medida": float(body["valor_medida"]),
            "fecha_medida": fecha_medida,
            "origen_fecha": origen_fecha,
            "received_at":  received_at,
            "topic":        body.get("topic", "desconocido")
        }


class ConstructorRespuestaHTTP(IConstructorRespuesta):
    def construir(self, codigo_http, contenido):
        cuerpo = {"mensaje": contenido} if isinstance(contenido, str) else contenido
        return {
            "statusCode": codigo_http,
            "headers":    {"Content-Type": "application/json"},
            "body":       json.dumps(cuerpo, ensure_ascii=False)
        }


class ServicioRecepcionTelemetria:
    def __init__(self, deserializador, pipeline, deduplicador,
                 enriquecedor, constructor, logger, persistencia):
        self._deserializador = deserializador
        self._pipeline       = pipeline
        self._deduplicador   = deduplicador
        self._enriquecedor   = enriquecedor
        self._constructor    = constructor
        self._logger         = logger
        self._persistencia   = persistencia

    def procesar(self, event):
        received_at = datetime.now(timezone.utc).isoformat()

        # 1. Deserializar
        try:
            body = self._deserializador.deserializar(event)
        except (json.JSONDecodeError, ValueError) as e:
            self._logger.rechazado(
                id_matera=None,
                causa="JSON malformado",
                detalle=str(e),
                received_at=received_at
            )
            return self._constructor.construir(
                400, "Mensaje descartado: formato JSON invalido."
            )

        id_matera = body.get("id_matera")

        # 2. Validar
        es_valido, causa = self._pipeline.ejecutar(body)
        if not es_valido:
            self._logger.rechazado(
                id_matera=id_matera,
                causa=causa,
                detalle=self._truncar(body),
                received_at=received_at
            )
            codigo = 400 if "faltante" in causa or "numerico" in causa else 422
            return self._constructor.construir(codigo, "Mensaje descartado: " + causa)

        # 3. Deduplicar
        dedup_key = str(body.get("id_matera", "")).strip() + "#" + str(body.get("fecha_medida", "")).strip()
        try:
            es_dup, event_id_orig = self._deduplicador.es_duplicado(dedup_key)
        except Exception as e:
            self._logger.error({
                "event": "dedup_error", "id_matera": id_matera,
                "dedup_key": dedup_key, "error": str(e),
                "received_at": received_at
            })
            es_dup, event_id_orig = False, None

        if es_dup:
            self._logger.duplicado(id_matera, dedup_key, event_id_orig, received_at)
            return self._constructor.construir(200, {
                "status":            "duplicado",
                "mensaje":           "Mensaje identificado como duplicado y descartado.",
                "event_id_original": event_id_orig
            })

        # 4. Enriquecer
        evento = self._enriquecedor.enriquecer(body, received_at)

        # 5. Registrar en dedup
        try:
            self._deduplicador.registrar(dedup_key, evento["event_id"])
        except Exception as e:
            self._logger.error({
                "event": "dedup_write_error", "event_id": evento["event_id"],
                "id_matera": id_matera, "error": str(e)
            })

        # 6. Persistir el evento
        try:
            self._persistencia.guardar(evento)
            self._logger.aceptado({
                "event":     "telemetria_persistida",
                "event_id":  evento["event_id"],
                "id_matera": evento["id_matera"],
                "tabla":     self._persistencia._nombre_tabla
            })
        except Exception as e:
            self._logger.error({
                "event":     "persistencia_error",
                "event_id":  evento["event_id"],
                "id_matera": id_matera,
                "error":     str(e)
            })

        # 7. Log de exito
        self._logger.aceptado({
            "event":        "telemetria_aceptada",
            "event_id":     evento["event_id"],
            "id_matera":    evento["id_matera"],
            "dispositivo":  evento["dispositivo"],
            "medida":       evento["medida"],
            "valor_medida": evento["valor_medida"],
            "fecha_medida": evento["fecha_medida"],
            "received_at":  received_at
        })

        return self._constructor.construir(200, {
            "status":   "accepted",
            "event_id": evento["event_id"],
            "mensaje":  "Evento de telemetria recibido y validado correctamente.",
            "evento":   evento
        })

    @staticmethod
    def _truncar(body, max_chars=200):
        texto = json.dumps(body, ensure_ascii=False)
        return texto[:max_chars] + "..." if len(texto) > max_chars else texto


def _construir_servicio():
    config   = Configuracion()
    logger   = Logger(config)
    dynamodb = boto3.resource("dynamodb")

    validador_fecha = ValidadorFecha()
    pipeline = PipelineValidacion([
        ValidadorCampos(CAMPOS_REQUERIDOS),
        ValidadorTipos(),
        ValidadorRangos(RANGOS_VALIDOS),
    ])

    return ServicioRecepcionTelemetria(
        deserializador = DeserializadorJSON(),
        pipeline       = pipeline,
        deduplicador   = DeduplicadorDynamoDB(dynamodb, config),
        enriquecedor   = EnriquecedorEvento(validador_fecha),
        constructor    = ConstructorRespuestaHTTP(),
        logger         = logger,
        persistencia   = PersistenciaDynamoDB(dynamodb, config.data_table_name)
    )


_servicio = None

def _get_servicio():
    global _servicio
    if _servicio is None:
        _servicio = _construir_servicio()
    return _servicio


def lambda_handler(event, context):
    return _get_servicio().procesar(event)
