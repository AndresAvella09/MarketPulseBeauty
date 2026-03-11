### Sprint Review

Durante el segundo sprint se logró consolidar el flujo inicial de adquisición y procesamiento de datos del proyecto MarketPulse Beauty, permitiendo pasar de una fase exploratoria a una etapa más estructurada de preparación y análisis de la información.

Los avances de este sprint se centraron principalmente en fortalecer el scraping, estructurar el almacenamiento de datos crudos y desarrollar los primeros análisis automáticos sobre las reseñas obtenidas.

#### Logros obtenidos

1. Fortalecimiento del proceso de scraping de Sephora, permitiendo una extracción más robusta de información sobre productos y reseñas.

2. Estructuración del almacenamiento de datos crudos dentro del proyecto, incluyendo:
- Archivos de reseñas en formato CSV
- Archivos estructurados en JSON
- Archivo TXT con los enlaces de los productos extraídos

odo organizado dentro del directorio `/data/raw`, estableciendo una base clara para el procesamiento posterior.

3. Implementación inicial del análisis de sentimiento sobre las reseñas obtenidas, permitiendo comenzar a evaluar la percepción de los usuarios sobre los productos.

4. Implementación de un modelo básico de análisis de tópicos (Topic Modeling) para identificar temas recurrentes dentro de las reseñas de los productos.

Estos avances permiten comenzar a transformar las reseñas en información analítica útil para el desarrollo de los indicadores del sistema.

**Aspectos pendientes**

Aunque se lograron avances importantes en la preparación y análisis de datos, el despliegue inicial del dashboard en Streamlit no pudo completarse durante este sprint y quedará como parte del trabajo pendiente para etapas posteriores del proyecto.