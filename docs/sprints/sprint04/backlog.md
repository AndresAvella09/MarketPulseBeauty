#### Migrar almacenamiento a formato Parquet con particionamiento

* **Estado:** Pendiente de ejecución final con datos reales.
* **Acción:** Convertir los datasets corregidos y actualizar scripts de lectura.

#### Implementar Data Contracts para validación entre etapas

* **Estado:** En progreso.
* **Acción:** Verificar tipos de datos, rangos de rating y volúmenes mínimos antes de avanzar en el pipeline.

#### Implementar versionado de datos con DVC

* **Estado:** Versión inicial completada; pendiente sincronización final.
* **Acción:** Tracking de `/data/raw/` y `/data/processed/` con el remote configurado para el equipo.

#### Construir pipeline de ingesta incremental de reseñas

* **Estado:** Pendiente.
* **Acción:** Refactorizar el scraper para evitar duplicidad basándose en la fecha máxima del Parquet.

#### Análisis de correlación entre reseñas y Google Trends

* **Estado:** Pendiente (bloqueado anteriormente por falta de datos).
* **Acción:** Generar el notebook y visualizaciones que crucen el sentimiento con el interés de búsqueda.

#### Configurar CI con GitHub Actions y Pruebas Unitarias

* **Estado:** En progreso.
* **Acción:** Lograr que el workflow pase a verde validando contratos y lógica de los módulos críticos.

#### Estabilización del Dashboard de Streamlit

* **Estado:** Prototipo funcional; pendiente integración Parquet y sección comparativa.
* **Acción:** Añadir gráficas de comparación entre productos y sistema de alertas de Health Score.

#### Documentación final del ciclo de desarrollo

* **Estado:** Pendiente.
* **Acción:** Completar reportes de sprint y acta de entrega para el siguiente Scrum Master.
