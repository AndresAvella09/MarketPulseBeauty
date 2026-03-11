#### Migrar almacenamiento a formato Parquet con particionamiento

Reemplazar los archivos CSV planos de `/data/processed/` por archivos Parquet particionados, mejorando rendimiento de lectura y compatibilidad con herramientas analíticas futuras.

Se debe:

1. Convertir `reviews_clean.csv`, `reviews_with_sentiment.csv` y `final_dataset.csv` a formato Parquet.
2. Definir estrategia de particionamiento por `product_id` y `review_date` (año/mes).
3. Actualizar todos los scripts de lectura para consumir Parquet en lugar de CSV.
4. Verificar que los tamaños y tiempos de lectura mejoran respecto al formato anterior.

Criterios de aceptación:

* Archivos Parquet generados en `/data/processed/parquet/` con particionamiento correcto.
* Script de conversión en `/src/storage/csv_to_parquet.py`.
* Todos los scripts del proyecto leen desde Parquet sin errores.
* Documento `/docs/decisions/storage-format.md` con justificación técnica.

---

#### Implementar versionado de datos con DVC

Configurar DVC sobre el repositorio para rastrear cambios en los datasets crudos y procesados, integrando con Git para garantizar reproducibilidad completa del pipeline.

Se debe:

1. Inicializar DVC en el repositorio.
2. Agregar los archivos de `/data/raw/` y `/data/processed/` al tracking de DVC.
3. Configurar un remote (Google Drive o carpeta compartida del equipo).
4. Documentar el flujo: cómo un nuevo integrante reproduce el dataset desde cero.

Criterios de aceptación:

* Archivos `.dvc` correctamente generados y commiteados.
* `dvc pull` funcional para el equipo completo.
* Guía de uso en `/docs/decisions/dvc-guide.md`.

---

#### Implementar Data Contracts para validación entre etapas del pipeline

Diseñar un módulo de validación que actúe como contrato entre la capa de ingesta y la de procesamiento, garantizando que los datos cumplen requisitos mínimos antes de avanzar.

Debe verificar en el dataset de reseñas:

* Tipos de datos por columna.
* Ausencia de nulos en campos críticos (`product_id`, `rating`, `review_date`, `review_text`).
* Rango válido de `rating` (entre 1 y 5).
* Longitud mínima de `review_text` (mayor a N caracteres).
* Volumen mínimo de registros por producto.

Debe verificar en el dataset de Google Trends:

* Presencia de todas las palabras clave definidas en Sprint 1.
* Fechas dentro del rango esperado.
* Ausencia de series completamente vacías.

Criterios de aceptación:

* Módulo en `/src/processing/data_contracts.py`.
* Contrato implementado como paso previo obligatorio en el pipeline de limpieza.
* Reporte de validación exportado a `/data/processed/quality_report.json`.
* Notebook demostrativo en `/notebooks/` con ejemplos de datos válidos e inválidos.

---

#### Construir pipeline de ingesta incremental de reseñas

Refactorizar el script de scraping para soportar actualizaciones incrementales: solo extraer reseñas nuevas desde la última fecha registrada, evitando re-descargar datos históricos.

Se debe:

1. Leer la fecha máxima registrada en el archivo Parquet de reseñas.
2. Extraer solo reseñas posteriores a esa fecha.
3. Hacer append al Parquet existente sin sobreescribir.
4. Registrar en log la ejecución: fecha, volumen extraído, errores.

Criterios de aceptación:

* Script actualizado en `/src/ingestion/scraper.py`.
* Append funcional validado con al menos una ejecución real.
* Log de ejecución en `/logs/ingestion.log`.
* Documento en `/docs/decisions/` describiendo la estrategia incremental y sus limitaciones.

---

#### Análisis de correlación entre reseñas y Google Trends

Profundizar el análisis de Google Trends del Sprint 2 cruzándolo con el volumen y sentimiento de reseñas para detectar relaciones entre interés de búsqueda y percepción del consumidor.

Se debe:

1. Alinear temporalmente ambas fuentes (semana como unidad común).
2. Calcular correlación entre volumen de reseñas por semana y valor de tendencia.
3. Calcular correlación entre sentimiento promedio semanal y valor de tendencia.
4. Identificar picos en Trends que precedan o sigan a cambios de sentimiento.

Criterios de aceptación:

* Notebook en `/notebooks/trends_vs_reviews_correlation.ipynb`.
* Visualizaciones de series temporales alineadas por producto.
* Tabla de correlaciones por producto y keyword.
* Interpretación escrita de los hallazgos más relevantes.

---

#### Orquestar pipeline completo con script maestro

Crear un script de orquestación que ejecute secuencialmente todo el pipeline: ingesta → validación (data contracts) → limpieza → sentimiento → tópicos → trends → health score → dataset final, con manejo de errores entre etapas.

Criterios de aceptación:

* Script `/src/pipeline_runner.py` funcional de extremo a extremo.
* Si una etapa falla, el pipeline registra el error y se detiene con mensaje claro en consola y en log.
* Compatible con ejecución desde línea de comandos y desde GitHub Actions.
* Documentación de uso actualizada en el README.

---

#### Configurar CI básico con GitHub Actions

Implementar un workflow de GitHub Actions que se ejecute en cada push a `main` o `develop` y valide automáticamente la integridad del proyecto.

Debe verificar:

* Instalación correcta del entorno desde `requirements.txt`.
* Que los data contracts pasan sobre una muestra sintética de datos de prueba.
* Que todos los scripts principales importan sin errores de sintaxis o dependencias faltantes.

Criterios de aceptación:

* Archivo `.github/workflows/ci.yml` funcional.
* Al menos una ejecución verde registrada en el repositorio.
* Badge de estado del CI visible en el README.
* Documento en `/docs/decisions/` explicando qué valida el workflow y por qué.

---

#### Implementar pruebas unitarias para módulos críticos

Escribir tests unitarios para los módulos más importantes del proyecto usando `pytest`.

Módulos a cubrir:

* `health_score.py`: validar que la función retorna valores en rango esperado con inputs conocidos.
* `data_contracts.py`: validar que detecta correctamente datos inválidos.
* `clean_text.py`: validar que el pipeline de limpieza transforma correctamente casos borde (texto vacío, caracteres especiales, idioma mixto).

Criterios de aceptación:

* Tests en `/tests/` con al menos 3 casos por módulo.
* Todos los tests pasan con `pytest` localmente.
* Tests integrados en el workflow de CI.
* Cobertura mínima del 60% en los módulos testeados.

---

#### Mejorar y estabilizar el dashboard de Streamlit

Iterar sobre el prototipo del Sprint 2 incorporando nuevas fuentes de datos y mayor estabilidad.

Se debe incorporar:

* Carga de datos desde Parquet en lugar de CSV.
* Manejo de errores si los archivos no existen o están vacíos.
* Nueva sección de  **Trends** : gráfica de interés de búsqueda en el tiempo por keyword y producto.
* Nueva sección de  **Alertas** : productos cuyo Health Score bajó más de X puntos respecto a la semana anterior.
* Filtros por rango de fecha aplicables globalmente en el dashboard.

Criterios de aceptación:

* App ejecutable sin errores con `streamlit run app.py`.
* Datos cargados correctamente desde Parquet.
* Sección de Trends con al menos una visualización por producto.
* Sección de Alertas funcional con umbral configurable.
* Sin errores críticos en consola durante navegación normal.

---

#### Implementar visualización comparativa entre productos

Crear una vista comparativa dentro del dashboard que permita seleccionar dos o más productos simultáneamente y contrastar sus métricas principales.

Debe mostrar:

* Health Score comparado en una misma gráfica temporal.
* Distribución de sentimiento lado a lado.
* Volumen de reseñas por semana superpuesto.
* Tendencia de búsqueda (Google Trends) comparada por keyword.

Criterios de aceptación:

* Vista accesible desde el menú lateral del dashboard.
* Selector múltiple de productos funcional.
* Mínimo 3 visualizaciones comparativas.
* Experiencia coherente con el resto del dashboard.

---

#### Documentación completa del Sprint 03

Completar la carpeta `/docs/sprints/sprint03/` incluyendo:

* Sprint goal
* Sprint planning
* Backlog ejecutado
* Sprint review
* Retrospective
* Documento de transición al siguiente Scrum Master

Criterios de aceptación:

* Todos los archivos completos.
* Retrospective con mínimo 3 mejoras concretas.
* Riesgos identificados y documentados para el Sprint 4.
