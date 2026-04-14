# Backlog Detallado - Sprint 03

Este documento contiene el desglose de tareas individuales para la evolución del proyecto MarketPulse Beauty, alineado con la planificación actualizada.

---

## 1. Definición Estratégica y Arquitectura Base (Responsable: Andrés)

### [Tarea 1.1] Redacción de preguntas de negocio prioritarias
Definir el marco analítico que guiará el desarrollo técnico del dashboard.

**Descripción:**
Establecer un mínimo de 5 preguntas estratégicas que el negocio necesita responder (ej. "¿Cuál es la tendencia de sentimiento en la línea de cuidado facial frente a lanzamientos de la competencia?").

**Criterios de aceptación:**
- Documento en `/docs/product/business_questions.md` con las preguntas redactadas.
- Validación de las preguntas por parte del equipo.

### [Tarea 1.2] Mapeo de métricas y visualizaciones obligatorias
Vincular las necesidades de negocio con componentes técnicos específicos.

**Descripción:**
Crear una matriz que relacione cada pregunta de negocio de la Tarea 1.1 con una métrica (KPI), un tipo de gráfico y el dataset necesario.

**Criterios de aceptación:**
- Tabla de mapeo integrada en `/docs/product/business_logic.md`.
- Definición clara de los tipos de visualización para cada métrica.

### [Tarea 1.3] Auditoría y depuración de componentes del Dashboard
Evaluar qué elementos actuales del Streamlit aportan valor y cuáles deben eliminarse.

**Descripción:**
Identificar visualizaciones, filtros o textos que no estén alineados con la nueva narrativa analítica para proceder a su remoción.

**Criterios de aceptación:**
- Lista de elementos a eliminar o simplificar documentada en un Issue de GitHub.

### [Tarea 1.4] Refactorización de rutas y limpieza de dependencias
Asegurar que el sistema de archivos sea coherente y portable antes del despliegue.

**Descripción:**
Limpiar rutas absolutas en todos los scripts del repositorio (usando librerías como `pathlib`) y organizar las dependencias en el sistema de carpetas, depurando el `requirements.txt`.

**Criterios de aceptación:**
- Todos los scripts del repositorio validados funcionando únicamente con rutas relativas.
- Archivo `requirements.txt` actualizado y sin librerías huérfanas.

---

## 2. Ingeniería de Datos y Optimización (Responsable: Daniel)

### [Tarea 2.1] Lógica de filtrado y agregación para Datasets de Visualización
Diseñar la estructura de datos reducida para mejorar el rendimiento.

**Descripción:**
Definir qué columnas y qué nivel de agregación (diaria, semanal, por producto) se requiere para que el dashboard funcione sin cargar el dataset completo.

**Criterios de aceptación:**
- Documentación del esquema (schema) de los nuevos datasets en `/docs/data_dictionary_viz.md`.

### [Tarea 2.2] Implementación del script de transformación Parquet
Desarrollar el proceso de generación de datasets ligeros.

**Descripción:**
Crear el script funcional que tome el dataset procesado y genere los archivos optimizados.

**Criterios de aceptación:**
- Script funcional en `/src/processing/create_viz_datasets.py`.
- Generación de archivos `.parquet` en la ruta `/data/processed/viz/`.
- Script o notebook de validación que confirme la integridad de los datos en `/data/processed/viz/`.

---

## 3. Infraestructura y Despliegue (Responsable: Daniel)


### [Tarea 3.1] Configuración de Dockerfile y Docker-compose
Escribir los archivos de configuración para la contenerización.

**Descripción:**
Configurar la imagen base de Python, la instalación de requerimientos mínimos y la exposición del puerto para Streamlit.

**Criterios de aceptación:**
- Archivos `Dockerfile` y `docker-compose.yml` integrados en la raíz del proyecto.

### [Tarea 3.2] Test de portabilidad y ejecución en entorno limpio
Validar que el contenedor funciona independientemente del sistema operativo local.

**Descripción:**
Realizar una ejecución de prueba (`docker-compose up`) y verificar que el dashboard carga los datos optimizados sin requerir configuración extra.

**Criterios de aceptación:**
- Aplicación corriendo en contenedor sin errores de carga de datos.
- Documento `README_DOCKER.md` con instrucciones precisas de despliegue.

---

## 4. Visualización y Dashboard (Responsable: Paula)

### [Tarea 4.1] Limpieza de UI y eliminación de visualizaciones redundantes
Refactorizar la interfaz de Streamlit según la auditoría de negocio.

**Descripción:**
Remover el código sobrante y simplificar la navegación para enfocarse estrictamente en los KPIs prioritarios.

**Criterios de aceptación:**
- Interfaz limpia y alineada con la lógica de negocio definida en la Tarea 1.1.

### [Tarea 4.2] Integración de datasets optimizados en Streamlit
Cambiar la fuente de datos principal del dashboard.

**Descripción:**
Actualizar las funciones de carga de datos en el Streamlit para que consuman los archivos Parquet de `/data/processed/viz/` en lugar de los archivos pesados originales.

**Criterios de aceptación:**
- App funcional con tiempos de carga notablemente reducidos.

### [Tarea 4.3] Modularización de componentes de visualización
Mejorar la arquitectura interna del código del dashboard.

**Descripción:**
Separar los componentes de la interfaz (layouts, gráficos, selectores) de la lógica de carga de datos y procesamiento.

**Criterios de aceptación:**
- Código de Streamlit modularizado adecuadamente en la carpeta `/src/visualization/`.

---

## 5. Cierre y Calidad (Equipo / Liderado por Andrés)

### [Tarea 5.1] Integración en rama Develop y Pull Requests
Asegurar que todo el trabajo del sprint esté consolidado.

**Criterios de aceptación:**
- Todas las tareas cerradas con sus respectivas Pull Requests aprobadas y mergeadas en `Develop`.

### [Tarea 5.2] Documentación del Sprint 03
Generar los artefactos de cierre del ciclo Scrum.

**Criterios de aceptación:**
- Carpeta `/docs/sprints/sprint03/` completa incluyendo: Sprint Goal, Planning, Review, Retrospective y el documento de transición para el siguiente Scrum Master.