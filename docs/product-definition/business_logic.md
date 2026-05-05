# Matriz de mapeo: Preguntas de negocio, KPIs, visualizaciones y datasets

| # | Pregunta de negocio | Métrica (KPI) | Tipo de gráfico | Dataset principal |
|---|---------------------|---------------|-----------------|------------------|
| 1 | ¿Qué productos combinan mejor rating y volumen? | Rating promedio, Volumen de reviews | Tabla ranking | reviews procesadas (data/processed/reviews.parquet) |
| 2 | ¿Qué familias concentran más recomendación? | % de recomendación (IsRecommended) | Barra horizontal | reviews procesadas (data/processed/reviews.parquet) |
| 3 | ¿Qué atributos aparecen más en reviews negativas? | Frecuencia de atributos en reviews negativas | Barra horizontal | reviews procesadas (data/processed/reviews.parquet) |
| 4 | ¿Cómo evoluciona el volumen de reviews en el tiempo? | Reviews por mes/familia | Línea temporal | reviews procesadas (data/processed/reviews.parquet) |
| 5 | ¿Qué tendencias de búsqueda emergen para productos clave? | Volumen de búsquedas (Google Trends) | Línea temporal, comparación | tendencias procesadas (data/processed/google_trends.parquet) |
| 6 | ¿Cuáles son los principales temas y sentimientos en las reviews? | Temas principales, Sentimiento promedio | Nube de palabras, barra, línea | reviews procesadas (data/processed/reviews.parquet) |

---

## Definición de tipos de visualización

- **Tabla ranking:** Muestra productos ordenados por KPI (ej. rating y volumen), permite priorizar.
- **Barra horizontal:** Compara valores entre categorías (ej. familias, atributos, % recomendación).
- **Línea temporal:** Visualiza evolución de una métrica a lo largo del tiempo (ej. reviews por mes, búsquedas).
- **Nube de palabras:** Resalta los temas más frecuentes en texto libre (ej. tópicos de reviews).
- **Comparación:** Varias líneas o barras para comparar productos/familias en una métrica.

---

> **Nota:** Los datasets principales se generan en `/data/processed/` y pueden estar en formato Parquet para eficiencia. El dashboard consume estos archivos optimizados para visualización rápida.
