# Documento de transición del mando Scrum Master

## Scrum Master saliente
Paula (`paulagacha0`)

## Scrum Master entrante
Andres (`AndresAvella09`)

* * *

## Estado general del proyecto

Al cierre de este periodo, el proyecto MarketPulse Beauty cuenta con una base técnica funcional sobre datos en formato Parquet y una app de Streamlit que ya puede ejecutarse de forma local con los insumos actualmente disponibles. La rama `Develop` concentra los cambios integrados del periodo y refleja el estado actualizado del trabajo del equipo.

Durante este ciclo se avanzó especialmente en la capa de visualización y en la estabilización del dashboard, pero también se identificó que el siguiente paso del proyecto no debe ser únicamente “agregar más pantallas”, sino precisar mejor qué preguntas de negocio queremos responder y qué estructura mínima de datos necesitamos para ello.

* * *

## Cierre del periodo de Scrum Master

Como propuesta de gestión para este cierre, se impulsó que las tareas trabajadas durante el periodo quedaran efectivamente terminadas a nivel de flujo Scrum:  
- pull requests revisadas e integradas en `Develop`
- issues correspondientes cerradas
- documentación de continuidad preparada para el siguiente responsable

En particular, el rango de trabajo desarrollado durante este periodo quedó consolidado a través de merges en `Develop`, dejando trazabilidad del avance y permitiendo una transición ordenada.

* * *

## Avances relevantes del periodo

### 1. Streamlit estabilizado sobre Parquet
Se avanzó en la app de Streamlit para que funcionara con datos locales en formato Parquet, reemplazando dependencias anteriores sobre CSV. También se incorporaron validaciones para escenarios donde los archivos no existen, están vacíos o no contienen columnas esperadas.

### 2. Ajustes de alcance en visualización
Se comenzó a depurar el dashboard para mostrar únicamente información alineada con el alcance estratégico del proyecto, especialmente sobre las tres líneas de producto priorizadas. Esto ayuda a evitar ruido y a mantener una narrativa más coherente.

### 3. Inicio de modularización del dashboard
Se trabajó en separar responsabilidades dentro del Streamlit, con el fin de que la app sea más mantenible y escalable en próximos sprints.

### 4. Transición a Docker iniciada pero no cerrada
El compañero `Ch0cmilo` sí trabajó sobre la transición del proyecto a Docker. Sin embargo, durante la **daily del 13/04** se identificó que esta línea requería cambios adicionales en organización de datos, definición de insumos mínimos y ajuste de rutas para que el empaquetado fuera realmente útil.  
Por decisión del equipo, esta issue se deja **pendiente para el siguiente Scrum Master**, en lugar de cerrarla parcialmente.

* * *

## Pendientes clave para el siguiente Scrum Master

### Pendiente 1: Definir el enfoque analítico del Streamlit
Se recomienda abrir o priorizar tareas específicas para analizar el dashboard actual y aterrizar con precisión:

- qué preguntas de negocio debe responder la app
- qué gráficos son obligatorios
- qué métricas son indispensables
- qué visualizaciones sobran o pueden simplificarse
- si conviene incluir propuestas como clusters, comparativas o vistas ejecutivas

La necesidad no es solo técnica, sino de producto: el equipo ya tiene base de datos y visualización, pero falta consolidar una narrativa analítica clara.

### Pendiente 2: Retomar la transición a Docker
La línea de Docker debe continuar, pero sobre una base más concreta. La recomendación es que primero se defina qué datos mínimos necesita realmente el Streamlit para correr correctamente, y a partir de eso:

- organizar inputs mínimos
- limpiar rutas y dependencias
- reducir el volumen de datos a empaquetar
- preparar una ejecución más portable y controlada

Esto permitirá que Docker no sea solo un contenedor del proyecto completo, sino un entorno realmente útil para despliegue o demo.

### Pendiente 3: Preparar datasets orientados a visualización
Se recomienda evaluar si conviene construir datasets resumidos o derivados específicamente para consumo del dashboard. Esto ayudaría a:
- mejorar tiempos de carga
- simplificar lógica en Streamlit
- facilitar despliegue local o con Docker
- separar mejor procesamiento analítico y visualización

* * *

## Riesgos y observaciones

- El principal riesgo actual no está en la recolección de datos, sino en la falta de definición fina del producto analítico que se quiere mostrar.
- Si se sigue ampliando la visualización sin definir primero preguntas de negocio, el dashboard puede crecer en complejidad sin mejorar realmente el valor entregado.
- La transición a Docker puede bloquearse nuevamente si no se reduce antes el conjunto de datos y dependencias necesarios para ejecución.
- La continuidad sobre `Develop` debe mantenerse como rama de integración principal para evitar dispersión del trabajo del equipo.

* * *

## Recomendaciones concretas para AndresAvella09

1. Priorizar una revisión funcional del Streamlit actual antes de pedir nuevas visualizaciones.
2. Liderar una discusión corta de equipo para definir las preguntas de negocio centrales del proyecto.
3. Convertir esa discusión en issues concretas de mejora del dashboard.
4. Retomar la transición a Docker solo después de definir insumos mínimos y estructura de datos necesaria.
5. Mantener la práctica de cerrar el ciclo completo: implementación, PR, merge en `Develop`, cierre de issue y documentación.

* * *

## Mensaje final de transición

El proyecto queda en una etapa donde la base técnica ya permite avanzar hacia una versión más madura del producto analítico. La prioridad del siguiente periodo no debería ser solo agregar más componentes, sino transformar lo ya construido en una herramienta más clara, portable y enfocada en responder preguntas útiles. Se entrega el proyecto con contexto, decisiones documentadas y líneas de continuidad explícitas para facilitar la siguiente gestión.