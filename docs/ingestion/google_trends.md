# Ingesta - Google Trends (pytrends)

## Objetivo
Descargar datos de interés en el tiempo desde Google Trends para 3 productos:
- niacinamida
- ácido hialurónico
- shampoo sin sulfatos

## Script
`src/ingestion/fetch_google_trends.py`

## Ejecución (Windows)
Desde la raíz del repo:

```powershell
py -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
py src/ingestion/fetch_google_trends.py --geo CO --timeframe "today 12-m"