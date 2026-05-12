\# Issue #96 — Ejecución de Docker

&#x20;

\## Objetivo

Validar la ejecución del entorno Docker del proyecto, con foco en PostgreSQL y Streamlit.

&#x20;

\## Comandos ejecutados

&#x20;

```bash

docker compose build streamlit

docker compose up -d postgres streamlit

docker compose ps

