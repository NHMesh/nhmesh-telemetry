# nhmesh-telemetry

## Development

 - Install Python
 - Install Poetry

```
poetry install

poetry run nhmesh-telemetry/producer.py
poetry run nhmesh-telemetry/collector.py
```

## Docker Setup

 1. In the docker folder... copy the sample.env to telemetry.env and tweak values
 2. `docker compose up -d`

 ```
 docker compose logs
 ```

Kibana dashboard - https://localhost:5601
