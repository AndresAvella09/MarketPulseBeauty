### Sprint Review

Durante el primer sprint se logró establecer la base técnica del proyecto, permitiendo la obtención y procesamiento inicial de datos reales que alimentarán el sistema de análisis de MarketPulse Beauty.

Aunque el producto se encuentra en una fase temprana, se logró validar la viabilidad técnica de las fuentes seleccionadas y establecer el flujo inicial de datos.

#### Logros obtenidos

1. Extracción funcional de reseñas reales desde la página de Sephora, incluyendo:
    - Texto completo
    - Valoración (rating)
    - Fecha de publicación
    Esto valida la disponibilidad de datos públicos relevantes para el proyecto.

2. Extracción de series temporales desde Google Trends, permitiendo medir el interés de búsqueda de productos e ingredientes seleccionados.

3. Procesamiento preliminar de reseñas, preparando los datos para análisis posterior (limpieza inicial y estructuración).

4. Definición clara del dataset mínimo viable, lo que permitirá escalar el procesamiento en el siguiente sprint

### Trabajo a continuación

El siguiente sprint se enfocará en transformar los datos crudos en un dataset analítico consolidado que permita:

1. Implementar un pipeline de limpieza reproducible.
2. Integrar análisis de sentimiento y modelado de tópicos.
3. Diseñar y calcular el primer prototipo del Health Score.
4. Consolidar las distintas fuentes en un dataset final estructurado.
5. Desarrollar un prototipo funcional inicial del dashboard en Streamlit.