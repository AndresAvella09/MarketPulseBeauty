# Monitoreo de métricas entre ejecuciones del pipeline

## Objetivo
Detectar cambios en métricas clave entre una ejecución previa y una actual del pipeline, y clasificar esos cambios como ninguno, menor o significativo.

## Métricas monitoreadas
- avg_rating: promedio de las calificaciones de reseñas.
- review_volume: número total de reseñas en la ejecución.
- health_score: puntuación calculada usando calificaciones y recomendaciones.
- pct_recommended: proporción de reseñas marcadas como recomendadas.

## Reglas de clasificación de cambios
Umbrales por defecto en src/processing/monitoring.py:
- avg_rating (absoluto): menor >= 0.05, significativo >= 0.2
- review_volume (relativo): menor >= 5%, significativo >= 20%
- health_score (absoluto): menor >= 2.0, significativo >= 5.0
- pct_recommended (absoluto): menor >= 0.05, significativo >= 0.15

El cambio relativo se calcula como abs(delta) / abs(previous). Si el valor previo es 0, cualquier valor actual distinto de cero se considera significativo.

## Salidas
Se genera un reporte JSON en data/processed/monitoring/monitoring_report.json con:
- cambios por métrica (previous, current, delta, delta_pct)
- clasificación de severidad (none, minor, significant, unknown)
- un contador resumen para revisión rápida.

Cómo ejecutar
1) Comparar dos snapshots de métricas guardadas:

    python scripts/monitor_metrics.py --previous data/processed/monitoring/metrics_prev.json --current data/processed/monitoring/metrics_current.json

2) Calcular métricas actuales a partir de un CSV de reseñas:
    python scripts/monitor_metrics.py --previous data/processed/monitoring/metrics_prev.json --current-reviews data/processed/reviews_with_sentiment.csv

Este script también guarda por defecto el snapshot de métricas actuales en data/processed/monitoring/metrics_current.json.

## Por qué esto ayuda
- Detecta regresiones silenciosas en indicadores clave del negocio.
- Señala aumentos o caídas sospechosas en el volumen de forma temprana.
- Proporciona una base simple para futuras alertas y dashboards.
