# Marketpulse Beauty
![CI](https://github.com/AndresAvella09/MarketPulseBeauty/actions/workflows/ci.yml/badge.svg?branch=main)

Retail Intelligence Dashboard para productos de belleza y cuidado personal

## Descripción del proyecto

MarketPulse Beauty es un sistema de análisis de datos enfocados en productos de belleza y cuidado personal.

El objetivo es transformar datos públicos (reseñas y tendencias de búsqueda) en insights accionables que permitan:

- Medir la percepción real de los consumidores
- Detectar problemas recurrentes en productos
- Identificar tendencias emergentes
- Analizar la evolución temporal del desempeño de un producto

Este proyecto se desarrolla bajo metodología Scrum y principios de DataOps.

## Problemas que resolvemos

Las marcas y retailers de belleza:

- No pueden revisar miles de reseñas manualmente
- Detectan tarde cambios en la percepción del consumidor
- No identifican a tiempo tendencias emergentes en ingredientes o rutinas

MarketPulse Beauty busca automatizar este análisis usando datos públicos y técnicas estadísticas interpretables.

---

## Subproductos Planeados y alcance

1. **Health Score del Producto**
    - Calculado a partir del rating promedio.
    - Ajustado por volumen de búsquedas.
    - Evolución temporal.

2. **Review Insights**
    - Identificación de temas recurrentes.
    - Principales quejas o problemas de productos seleccionados.
    - Análisis de sentimiento.

3. **Trend Analysis**
    - Interés de búsqueda en el tiempo.
    - Comparación entre marcas o productos.
    - Detección de picos de búsqueda.

---

## Arquitectura planeada

Datos Públicos | APIs  
  ├── Reseñas de productos
  └── Google Trends
        ↓
Pipeline de ingesta (Python)
        ↓
Almacenamiento estructurado
        ↓
Procesamiento y Modelado
        ↓
Dashboard + Alertas


---

## Stack Tecnológico

- Python
    - pandas
    - numpy
    - scikit-learn
    - nlth | spaCy
    - Streamlit
- GitHub
    - GitHub Projects
    - Github Actions
- Metodología SCRUM

---

## Metodología de Trabajo

El proyecto se desarrolla bajo metodología Scrum, organizado en 6 sprints de 2 semanas, para una duración total de 12 semanas.

Cada sprint tendrá los siguientes eventos formales:

- Sprint Planning: definición del objetivo del sprint, selección de tareas y asignación de responsabilidades.
- Daily Stand-up: seguimiento breve del progreso y detección de bloqueos.
- Sprint Review: demostración de avances funcionales.
- Retrospectiva: análisis interno del desempeño del equipo y oportunidades de mejora.

### Rotación del Scrum Master

El rol de Scrum Master rota entre los integrantes del equipo cada dos sprints, con el fin de:

- Garantizar aprendizaje en liderazgo y gestión ágil.
- Distribuir la responsabilidad organizacional.
- Fomentar la mejora continua desde diferentes perspectivas.

### Documentación por Sprint

Cada Sprint tendrá su propia carpeta de documentación dentro del repositorio, donde se incluirá:

- Objetivo del sprint.
- Backlog comprometido.
- Acta de Sprint Planning.
- Evidencias de avance.
- Acta de Sprint Review.
- Retrospectiva.
- Documento de transición al siguiente Scrum Master.

### Transición entre Scrum Masters
Al finalizar su periodo, el Scrum Master saliente realizará una reunión formal de traspaso con el siguiente Scrum Master, en la cual se documentará:

- Estado actual del proyecto.
- Tareas completadas y pendientes.
- Riesgos identificados.
- Recomendaciones estratégicas para el siguiente sprint.

Toda la documentación será almacenada en el repositorio para garantizar trazabilidad, transparencia y continuidad del proyecto.