# Docker Environment

This directory contains manifests to set up a telemetry collection environment.

## Getting Started

1. Copy `sample.env` to `.env` and fill in the required fields.
2. Run `docker compose up -d`.
3. Reset the passwords for the `elastic` and `kibana_system` users:

   docker exec -it <elasticsearch-container> bin/elasticsearch-reset-password -u elastic
   docker exec -it <elasticsearch-container> bin/elasticsearch-reset-password -u kibana_system

4. Create the `telemetry_write` user and role (steps to be added).
5. Update the `.env` file and restart the services.
