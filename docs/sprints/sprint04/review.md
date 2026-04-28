# Sprint Review — Sprint 4

**Proyecto:** MarketPulse Beauty
**Sprint Goal:** Finalizar la deuda técnica del Sprint 3 y consolidar el ecosistema DataOps del proyecto.

---

### Resumen del sprint

El Sprint 4 fue diseñado como un sprint de cierre de deuda técnica, sin incorporación de nuevos requerimientos. El equipo logró consolidar la infraestructura DataOps del proyecto en su totalidad: almacenamiento columnar, versionado de datos, validación automática y pruebas unitarias están operativos y funcionales para los tres integrantes. El sprint cumplió su objetivo técnico central.

Sin embargo, dos entregables orientados al análisis y a la capa de presentación del producto no alcanzaron el estado "Done": el dashboard de Streamlit no completó su integración con el nuevo almacenamiento ni sus secciones comparativas, y el análisis de correlación entre reseñas y Google Trends no fue implementado. Ambas tareas se trasladan al Sprint 5 con carácter de máxima prioridad.

---

### Logros obtenidos

**Daniel — Infraestructura de almacenamiento y calidad de datos**

1. **Migración total a Parquet con particionamiento** : los datasets `reviews_clean`, `reviews_with_sentiment` y `final_dataset` fueron migrados definitivamente al formato columnar, particionados por `product_id` y `review_date`. Todos los scripts de lectura del proyecto fueron actualizados para consumir Parquet. Los archivos CSV planos dejaron de ser la fuente de verdad del pipeline.
2. **Data Contracts implementados** : el módulo `/src/processing/data_contracts.py` está operativo como paso obligatorio previo al pipeline de limpieza. Valida tipos de datos por columna, ausencia de nulos en campos críticos (`product_id`, `rating`, `review_date`, `review_text`), rango válido de `rating` (1–5), longitud mínima de `review_text` y presencia de todas las keywords de Google Trends en el rango de fechas esperado.
3. **Ingesta incremental funcional** : el script `/src/ingestion/scraper.py` fue refactorizado para leer la fecha máxima registrada en el Parquet existente y hacer append únicamente de reseñas nuevas, evitando re-descargar datos históricos. La ejecución queda registrada en `/logs/ingestion.log` con fecha, volumen extraído y errores.

**Diego — Automatización, validación y versionado**

4. **CI con GitHub Actions al 100%** : el workflow `.github/workflows/ci.yml` está activo y en verde en el repositorio. Se ejecuta en cada push a `main` y `develop`, validando instalación del entorno desde `requirements.txt`, ejecución de contratos sobre datos sintéticos e importación sin errores de los módulos principales. El badge de estado es visible en el README.
5. **Pruebas unitarias completas con pytest** : los módulos `health_score.py`, `data_contracts.py` y `clean_text.py` cuentan con tests en `/tests/` con al menos 3 casos por módulo, incluyendo casos borde. Todos los tests pasan localmente y están integrados al workflow de CI, alcanzando la cobertura mínima del 60% en los módulos testeados.
6. **DVC sincronizado para el equipo completo** : el remote en Google Drive está configurado y el flujo `dvc pull` / `dvc push` es funcional para los tres integrantes. Los datasets "gold" de `/data/raw/` y `/data/processed/` se encuentran bajo tracking con sus archivos `.dvc` commiteados correctamente.

---

### Aspectos pendientes

Las siguientes tareas no alcanzaron sus criterios de aceptación durante el Sprint 4 y se trasladan al Sprint 5:

**Paula — Dashboard de Streamlit**

El dashboard permanece en estado de prototipo funcional sobre CSV. Si bien existe una versión ejecutable de la app, no se completó la migración de la carga de datos a Parquet, ni se implementaron la sección de Trends, la sección de Alertas con umbral configurable ni la vista comparativa entre productos. Esta tarea acumula deuda desde el Sprint 3 y representa la brecha más visible entre el estado técnico del pipeline y la capacidad de demostrar resultados al negocio.

**Análisis de correlación entre reseñas y Google Trends**

El script `trends_correlation.py` ya genera los archivos de correlación en `/data/processed/google_trends/` (series semanales alineadas, correlaciones Pearson y Spearman por keyword). Sin embargo, el notebook analítico `/notebooks/trends_vs_reviews_correlation.ipynb` con visualizaciones, tabla de correlaciones e interpretación escrita de hallazgos no fue desarrollado. El trabajo de datos está hecho; falta la capa interpretativa.

---

### Trabajo a continuación

El Sprint 5 arranca con la infraestructura DataOps completa y estable, lo que representa un punto de inflexión para el proyecto. A partir de este punto, el foco debe girar hacia la generación de valor analítico y la consolidación del producto visible.

Las prioridades inmediatas para el Sprint 5 son:

1. Completar el dashboard de Streamlit con integración Parquet, sección de Trends, Alertas y vista comparativa — descomponiéndolo en subtareas independientes para garantizar entregas parciales.
2. Desarrollar el notebook de correlación con interpretación de hallazgos, aprovechando que los datos ya están disponibles.
3. Formalizar la fórmula del Health Score en `/docs/health_score.md` con justificación estadística y ejemplo numérico, como prerequisito para cualquier nueva visualización o análisis construido sobre esa métrica.
4. Evaluar la implementación del script maestro `/src/pipeline_runner.py` para garantizar reproducibilidad de extremo a extremo del pipeline completo.

---

*MarketPulse Beauty | Sprint Review Sprint 4 | Uso Interno del Equipo*
