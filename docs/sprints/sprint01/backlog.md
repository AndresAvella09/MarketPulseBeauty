## Backlog
En este documento se plantean formalmente las tareas a realizar durante este sprint.

### Crear estructura inicial del repositorio
Crear la estructura base del proyecto siguiendo la arquitectura definida en el README. Con el fin de organizar correctamente el código, los datos y la documentación.

Se deben crear las siguientes carpetas:

- /docs
- /docs/sprints
- /docs/sprints/sprint01
- /docs/sprints/sprint01/sprint-goal.md
- /docs/sprints/sprint01/sprint-planning.md
- /docs/sprints/sprint01/backlog.md
- /docs/sprints/sprint01/review.md
- /docs/sprints/sprint01/restrospective.md
- /data/raw
- /data/processed
- /nootebooks
- /src/ingestion
- /src/processing
- /src/modeling
- /src/dashboard
- /test

Criterios de aceptación:
- Estructura creada y subida al repositorio.
- README actualizado.

---

### Configurar entorno virtual y archivo requirements.txt

Configurar el entorno de desarrollo en Python y generar el archivo `requirements.txt` con las librerías necesarias para la fase inicial del proyecto.

Criterios de aceptación:
- Archivo `requirements.txt` creado.
- Librerías verificadas.

---

### Seleccionar productos de belleza a analizar

Seleccionar 3 productos de belleza y cuidado personal que cumplan:
- Alto volumen de reseñas públicas
- Disponibilidad de ratings y fechas
- Posible relación con tendencias de búsqueda

Criterios de aceptación:
- Productos definidos.
- Documento en `/docs/product-definition/` con justificación

---

### Investigar fuente viable de reseñas públicas
Identificar una fuente pública y accesible para extraer reseñas de productos de belleza.

Evaluar:

- Disponibilidad de texto
- Fecha
- Volumen suficiente

Documentar:

- Plataforma elegida
- Limitaciones
- Riesgos técnicos

Criterios de aceptación:
- Fuente definida.
- Documento en `/docs/decisions/tech-decisions.md`

---

### Desarrollar script inicial de extracción de reseñas

Crear un script en `/src/ingestion/` que permita extraer reseñas de los productos seleccionados.

El dataset mínimo debe incluir:

- product_id
- rating
- review_text
- review_date

Criterios de aceptación:
- Script funcional.
- Archivo CSV guardado en `/data/raw/`.
- Código comentado.

---

### Estructurar y documentar variables extraídas

Definir formalmente las variables del dataset crudo.

Documentar:

- Nombre de variable.
- Tipo de dato.
- Descripción.
- Posibles valores nulos.

Criterios de aceptación:
 
- Diccionario de datos creado en `/docs/data-dictionary.md/` 

---

### Configurar conexión a Google Trends (pytrends)

Configurar pytrends y realizar pruebas de conexión para descargar datos de interés en el tiempo.

Se deben probar al menos 3 palabras clave relacionadas con los productos seleccionados.

Criterios de aceptación:
- Script funcional en `/src/ingestion/`.
- DAtos guardados en `/data/raw/`.
- Documentación del proceso.

---

### Seleccionar palabras clave estratégicas

Definir palabras clave relevantes para Google Trends basadas en:

- Nombre del producto.
- Ingredientes activos.
- Problemas que resuelve (ej: “acne treatment”, “hair loss”)

Criterios de aceptación:

- Lista validada por el equipo.
- Justificación en `docs/product-definition/`
- Entre 6 a 8 palabras clave.

---

### Análisis descriptivo de ratings
Realizar análisis exploratorio de:

- Distribución de ratings.
- Promedio por producto.
- Cantidad de reseñas por fecha.
- Identificación de outliers.

Criterios de aceptación:

- Nootebook en `/notebooks/`
- Visualizaciones claras.
- Comentarios apropiados.
- Identificación de hallazgos preliminares.