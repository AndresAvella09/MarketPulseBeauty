## Sprint Review 

El tercer sprint estuvo centrado en la **consolidación de la arquitectura DataOps** y la automatización de la calidad de los datos. A pesar de enfrentar desafíos técnicos significativos con el módulo de scraping que limitaron la generación de "gold data" durante gran parte de la semana, se lograron hitos críticos en la infraestructura y visualización que sientan las bases para la escala del proyecto.

Hacia el final del sprint, se logró resolver la inconsistencia en la extracción, lo que desbloquea el flujo completo para la siguiente iteración.

#### Logros obtenidos

1. **Implementación de Versionado de Datos (DVC):** Se logró una primera versión funcional de DVC, permitiendo el rastreo de cambios en los datasets de `/data/raw/` y el inicio de la reproducibilidad del pipeline.
2. **Evolución del Dashboard en Streamlit:** Se desarrolló una versión temprana del dashboard que integra los resultados previos y prepara el terreno para la visualización de tendencias y alertas.
3. **Resolución de Bloqueo en Scraping:** Se identificó y corrigió el fallo en el script de extracción de reseñas de belleza, asegurando que la ingesta de datos hacia la capa procesada sea estable a partir de ahora.
4. **Cimientos de Integración Continua (CI):** Se iniciaron las configuraciones de GitHub Actions para validar la instalación del entorno y la sintaxis de los módulos críticos.

#### Desafíos y Pendientes

Debido a la demora en la obtención de datos limpios ("gold data"), las siguientes tareas no alcanzaron los criterios de aceptación y se moverán al siguiente ciclo:

* Migración completa a formato Parquet particionado (dependiente de la estabilidad del volumen de datos).
* Validación total mediante Data Contracts en todas las etapas del pipeline.
* Orquestación del pipeline de extremo a extremo con el script maestro.

---

## Trabajo a continuación

Con el problema de ingesta resuelto, el enfoque del **Sprint 4** será la estabilización definitiva y el análisis avanzado:

1. **Migración a Parquet:** Ejecutar la transición de CSV a Parquet para mejorar el rendimiento de lectura en el dashboard.
2. **Análisis de Correlación:** Cruzar finalmente el sentimiento de las reseñas con las tendencias de búsqueda de Google Trends.
3. **Pipeline Robusto:** Finalizar la orquestación secuencial (ingesta **$\rightarrow$** validación **$\rightarrow$** procesamiento) para asegurar la entrega continua de datos.
4. **Pruebas Unitarias:** Completar la cobertura de tests en los módulos de `health_score.py` y `clean_text.py`.
