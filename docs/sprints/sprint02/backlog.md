## Backlog
En este documento se plantean formalmente las tareas a realizar durante este sprint.

### Definir e implementar fórmula del Health Score
Diseñar formalmente la métrica Health Score, que será uno de los principales subproductos del proyecto.

El Health Score debe combinar:

- Rating promedio
- Volumen de reseñas
- Recencia (Ponderación temporal)
- Sentimiento promedio

Se debe:

1. Proponer la fórmmula matemática.
2. Justificar estadísticamente en un documento.
3. Implementarla como función reutilizable en `/src/modeling/health_score.py`
4. Probarla con datos reales del dataset procesado.

Criterios de aceptación:
- Documento `/docs/health_score.md` con:
    - Formula formal.
    - Explicación de cada componente.
    - Ejemplo numérico.
- Script funcional con función `calculate_health_score()`
- Test básico que valide que la función devuelve valores numéricos válidos.


---

### Construir dataset final consolidado

Crear un script integrador que unifique todas las fuentes procesadas:

- Reviews limpias.
- Sentiment score
- Topic ID
- Tendencias de Google Trends

El resultado debe ser un datset final analítico que contenga (Puede variar según los resultados de las otras tareas):
- product_id
- rating
- sentiment_score
- topic_id
- review_date
- trend_value
- health_score

Criterios de aceptación:
- Dataset generado y guardado correctamente en `/data/processed/final_dataset.csv`.
- Script reproducible en `/src/processing/build_final_dataset.py`.
- Validación básica por tipos de datos.

---

### Documentación completa del Sprint 02

Completar la carpeta `/docs/sprints/sprint02/` incluyendo:
- Sprint goal
- Sprint planning
- Backlog ejecutado
- Sprint review
- Retrospective
- Transition report

Debe reflejar:
- Qué se logró
- Qué no se logró
- Qué aprendió el equipo
- Riesgos detectados

Criterios de aceptación:
- Todos los archivos completos.
- Retrospective con mínimo 3 mejoras concretas.
---

### Mejorar robustez del script de extracción de reseñas
Optimizar el script de scraping implementando: 

- Prevención de duplicados
- Validadción de campos faltantes
- Registro de errores (loggin básico)

Criterios de aceptación:
- Script actualizado y documentado
- CSV en `/data/raw/`
- Documento breve en `/docs/decisions/` describiendo limitaciones.

---

### Implementar pipeline reproducible de limpieza de texto

Crear un módulo de limpieza de texto que incluya:

- Lowercase
- Eliminación de puntuación
- Eliminación de stopwords
- review_date
- Tokenización
- Lematización con spaCy

Criterios de aceptación:
- Script em `/src/processing/clean_text.py`
- Dataset limpio generado
- Nueva versión del datset guardada en `/data/processed/reviews_clean.csv`
- Documentación según el procesamiento y las transformaciones realizadas
---

### Verificación de calidad del dataset procesado

Realizar validación básica de calidad de datos:

- Valores nulos
- Longitud promedio de reseñas
- Distribución temporal de reseñas

Criterios de aceptación:
- Notebook de validación en `/notebooks/`
- Resumen estadístico claro
- Lista de problemas detectados (Si existen)


---

### Implementar análisis de sentimiento

Aplicar un modelo baseline de análisis de sentimiento sobre las reseñas limpias.

Debe generar:
- Columna `sentiment_score`
- Clasidicación opcional (positivo/neutral/negativo)

Criterios de aceptación

- Script funcional
- Notebook con distribución de sentimiento por producto
- Interpretación escrita de resultados
- Datos guardados en `/data/processed/reviews_with_sentiment.csv`

---

### Implementar Topic Modeling

Aplicar un modelo de Topic Modeling (LDA o NMF) para identificar entre 5 y 8 temas recurrentes en las reseñas.

Debe:

- Asignar `/topic_id` por reseña
- Mostrar palabras clave por tópico
- Implementar manualmente cada tema

Criterios de aceptación:

- Notebook con visualización de tópicos
- CSV con clumna `topic_id`
- Documento de interpretación de resultados

---

### Integrar y analizar Google Trends
Procesar datos descargados de Google Trends:

- ormalizar series temporales
- Agrupar por semana
- Calcular correlación con volumen de reseñas

Criterios de aceptación:

- Script limpio 
- Notebook con el correspondiente análisis completo de los datos.

---

### Crear prototipo inicial en Streamlit

Desarrollar un dashboard mínimo que consuma `final_dataset.csv` y muestre:
- Selector de producto
- Rating promedio
- Health Score
- Evolución temporal
- Distribución de sentimiento

Debe ser principalmente funcional, no necesariamente estético.

Criterios de aceptación:
- App ejecutable con `streamlit run app.py`
- Datos cargados correctamente sin errores críticos.