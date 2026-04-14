# Insights automáticos por producto

## Objetivo
Traducir señales de producto (calificaciones, volumen de reseñas, tendencias, puntaje de salud) en insights breves y accionables.

## Entradas
- Dataset de reseñas con columnas product_id y rating (ProductID/pd_id/product_id, Rating/rating).
- Dataset opcional de productos para nombre y marca.
- Dataset opcional de tendencias más un mapeo de product_id a trend_keyword.

## Métricas utilizadas
- avg_rating: calificación promedio por producto.
- review_volume: total de reseñas por producto.
- health_score: calculado con la fórmula existente (0-100).
- pct_recommended: proporción de IsRecommended cuando esté disponible.
- tendencias semanales para calificación, volumen de reseñas e interés de búsqueda.

## Lógica de tendencias
Para cada producto, comparar las N semanas más recientes contra las N semanas anteriores.
- rating_trend usa deltas absolutos (menor >= 0.10, significativo >= 0.20).
- volume_trend usa deltas relativos (menor >= 10%, significativo >= 20%).
- search_trend usa deltas relativos (menor >= 10%, significativo >= 20%).

## Reglas de insights (inicial)
- Calificación alta: avg_rating >= 4.5
- Calificación baja: avg_rating <= 3.2
- Health score fuerte: >= 80
- Health score bajo: <= 50
- Volumen alto: review_volume >= 200
- Volumen bajo: review_volume <= 10
- Mensajes de tendencia para calificación, volumen de reseñas e interés de búsqueda (sube/baja/estable)

## Salida
Un reporte JSON en data/processed/insights/insights.json con:
- métricas y señales de tendencia por producto
- mensajes de insights con severidad (positivo, advertencia, informativo)
- una sección de resumen para revisión rápida

## Cómo ejecutar
python scripts/generate_insights.py --reviews data/raw/csv/review_data.csv --products data/raw/csv/pd_info.csv --trends data/processed/google_trends/trends_weekly.csv --trend-map data/raw/csv/pd_info.csv