## Documento de transición del mando Scrum Master

---

### Estado general del proyecto

Al cierre del Sprint 4, el ecosistema DataOps del proyecto MarketPulse Beauty quedó mayoritariamente consolidado. La infraestructura técnica de fondo —almacenamiento Parquet, versionado de datos, contratos de calidad, ingesta incremental y validación automática por CI— está operativa y funcional para todo el equipo. Este sprint cumplió su objetivo principal de liquidar la deuda técnica acumulada desde el Sprint 3.

Sin embargo, dos entregables orientados al producto y al análisis no alcanzaron el estado "Done": el dashboard de Streamlit no completó su integración con Parquet ni su sección comparativa, y el análisis de correlación entre reseñas y Google Trends sigue pendiente. Estas dos tareas representan la deuda más crítica que el Sprint 5 debe resolver antes de avanzar hacia nuevos requerimientos.

---

### Trabajo completado en el Sprint 4

* **Migración total a Parquet con particionamiento** : los datasets `reviews_clean`, `reviews_with_sentiment` y `final_dataset` fueron convertidos definitivamente al formato columnar particionado. Todos los scripts de lectura del proyecto consumen Parquet. Los CSV planos ya no son la fuente de verdad.
* **Data Contracts implementados** : el módulo `/src/processing/data_contracts.py` valida tipos de datos, ausencia de nulos en campos críticos, rango de `rating`, longitud mínima de `review_text` y cobertura de keywords de Google Trends. El contrato actúa como paso obligatorio en el pipeline de limpieza.
* **Ingesta incremental funcional** : el scraper fue refactorizado para leer la fecha máxima registrada en el Parquet existente y hacer append únicamente de reseñas nuevas, evitando duplicidad. El log de ejecución queda registrado en `/logs/ingestion.log`.
* **CI con GitHub Actions al 100%** : el workflow `.github/workflows/ci.yml` está activo y en verde, validando instalación del entorno, ejecución de contratos sobre datos sintéticos e importación de módulos críticos en cada push a `main` y `develop`.
* **Pruebas unitarias completas** : los módulos `health_score.py`, `data_contracts.py` y `clean_text.py` tienen cobertura de tests en `/tests/` con al menos 3 casos por módulo, integrados al workflow de CI y funcionando correctamente de forma local para todo el equipo.
* **DVC sincronizado para el equipo** : el remote en Google Drive está configurado y el flujo `dvc pull` / `dvc push` es funcional para los tres integrantes. Los datasets "gold" de `/data/raw/` y `/data/processed/` están bajo tracking.

---

### Tareas pendientes al cierre del Sprint 4

Estas dos tareas no alcanzaron sus criterios de aceptación y deben tratarse con prioridad máxima al inicio del Sprint 5, antes de cualquier requerimiento nuevo:

**1. Dashboard de Streamlit — integración Parquet y secciones faltantes**

El dashboard permanece en estado de prototipo funcional sobre CSV. No se completó la conexión al nuevo almacenamiento Parquet, ni se implementaron la sección de Alertas (productos con caída de Health Score) ni la vista comparativa entre productos. Esta tarea lleva pendiente desde el Sprint 3 y su demora está afectando la capacidad de demostrar valor del proyecto en las revisiones.

Alcance pendiente concreto:

* Migrar la carga de datos de CSV a Parquet en la app.
* Implementar sección de **Trends** con gráfica de interés por keyword y producto.
* Implementar sección de **Alertas** con umbral de caída de Health Score configurable.
* Implementar vista comparativa: Health Score, sentimiento, volumen de reseñas y tendencia de búsqueda entre dos o más productos seleccionados.

**2. Análisis de correlación entre reseñas y Google Trends**

El notebook `/notebooks/trends_vs_reviews_correlation.ipynb` con alineación temporal semanal, cálculo de correlaciones por producto e interpretación escrita de hallazgos no fue generado. El script `trends_correlation.py` ya produce los CSVs de correlación en `/data/processed/google_trends/`, por lo que el trabajo analítico está parcialmente resuelto a nivel de datos; lo que falta es la capa interpretativa y visual.

Alcance pendiente concreto:

* Notebook con visualizaciones de series temporales alineadas por producto.
* Tabla de correlaciones (Pearson y Spearman) por producto y keyword.
* Interpretación escrita de los hallazgos más relevantes.
* Identificación de picos en Trends que precedan o sigan a cambios de sentimiento.

---

### Recomendaciones para el Sprint 5

**Sobre las tareas pendientes:**

Ambas tareas deben estar planificadas como los primeros ítems del backlog del Sprint 5, con estimación propia y responsable asignado desde el primer día. No deben mezclarse con nuevos requerimientos en la misma tarea; si el equipo decide incorporar análisis adicionales sobre los resultados de correlación, deben abrirse como tareas separadas.

**Sobre el dashboard:**

Esta es la tercera vez que el dashboard no completa su sprint. Se recomienda que en el Sprint Planning del Sprint 5 se descomponga en subtareas más pequeñas e independientes (carga Parquet, sección Trends, sección Alertas, vista comparativa) para que cada una pueda marcarse como "Done" de forma autónoma. Esto permite avanzar incrementalmente sin que el bloqueo de una parte paralice las demás.

**Sobre la orientación del Sprint 5:**

Con la infraestructura DataOps estable, el Sprint 5 es el momento natural para que el proyecto gire hacia la generación de valor analítico y de negocio. Se sugiere que las nuevas tareas del sprint se enfoquen en:

* Refinamiento y validación del Health Score con datos reales (la fórmula debe estar documentada y acordada por el equipo).
* Generación de los primeros insights accionables exportables desde el dashboard.
* Evaluación del pipeline de extremo a extremo (`/src/pipeline_runner.py`) si aún no existe como punto de entrada único.

**Sobre la rotación del Scrum Master:**

El nuevo Scrum Master debe verificar en el primer día que DVC esté sincronizado en su máquina (`dvc pull`) y que el CI esté en verde antes de que el equipo empiece a hacer cambios, para garantizar que la base estable del Sprint 4 se mantiene al inicio del Sprint 5.

---

### Riesgos identificados para el Sprint 5

| Riesgo                                                                                   | Severidad | Descripción                                                                                                                                                                         | Mitigación sugerida                                                                                                                         |
| ---------------------------------------------------------------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Dashboard bloqueado por tercera vez consecutiva                                          | Alta      | El dashboard acumula tres sprints sin completarse. Si no se resuelve en el Sprint 5, el proyecto no tiene capa de presentación funcional hacia el cierre del semestre               | Descomponer en subtareas independientes; asignar al menos dos sesiones de trabajo dedicadas exclusivamente al dashboard en la primera semana |
| Health Score sin fórmula formalmente acordada                                           | Alta      | Los cálculos del dashboard y el análisis de correlación dependen de una métrica que no tiene documentación oficial cerrada, lo que puede generar inconsistencias entre módulos | Dedicar los primeros 30 minutos del Sprint Planning a acordar y documentar la fórmula final en `/docs/health_score.md`                    |
| Análisis de correlación sin contexto interpretativo                                    | Media     | Los CSVs de correlación existen, pero sin interpretación escrita los resultados no son accionables para el negocio ni comunicables en una revisión                                | Completar el notebook como entrega prioritaria en la primera semana del sprint                                                               |
| Acumulación de deuda si se agregan requerimientos nuevos antes de cerrar los pendientes | Media     | Incorporar nuevas funcionalidades sin haber cerrado el dashboard y el notebook añade complejidad y puede repetir el patrón de los sprints anteriores                               | Política estricta: ninguna tarea nueva entra al sprint hasta que las dos tareas pendientes tengan criterios de aceptación verificados      |

---

*MarketPulse Beauty | Documento de Transición Sprint 4 → Sprint 5 | Uso Interno del Equipo*
