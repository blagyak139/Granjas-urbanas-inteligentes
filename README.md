# Granjas Urbanas Inteligentes

Backend de telemetría IoT para materas inteligentes.

## Estructura
- `backend/lambda/` — Función Lambda de recepción de telemetría
- `backend/tests/` — Pruebas unitarias con pytest

## Servicios AWS
- AWS Lambda (Python 3.12)
- AWS IoT Core
- DynamoDB (telemetry-data, telemetry-dedup)
- CloudWatch Logs