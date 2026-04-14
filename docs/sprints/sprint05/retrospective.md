# Sprint Retrospective — Sprint 5

Proyecto: MarketPulse Beauty

* * *

## ¿Qué salió bien?

- Se logró dar continuidad al proyecto sobre la rama `Develop`, manteniendo el trabajo alineado con el flujo real del equipo.
- Se fortaleció la app de Streamlit para trabajar con datos locales en Parquet, lo que acerca más la capa de visualización al estado real del proyecto.
- Se avanzó en la depuración del alcance del dashboard, evitando mantener componentes que dependían de datos no disponibles o de estructuras sintéticas.
- Se promovió el cierre ordenado de las tareas priorizadas del periodo de Scrum Master, logrando que las pull requests asociadas quedaran integradas en `Develop` y sus issues cerradas.
- Se detectó a tiempo que la transición a Docker necesitaba mayor preparación técnica, lo que evitó forzar una entrega inestable.

## ¿Qué no salió tan bien?

- Persistió cierta ambigüedad sobre el objetivo exacto del Streamlit: en varios momentos la visualización avanzó más rápido que la definición de las preguntas de negocio que debía responder.
- La transición a Docker no alcanzó a completarse porque dependía de decisiones previas sobre organización de datos, rutas y artefactos mínimos para ejecución.
- El proyecto aún requiere una mejor separación entre datos de procesamiento y datos listos para consumo en dashboard.
- Algunas tareas de visualización dependieron de validaciones manuales en local, lo que hace más lenta la iteración del equipo.

## ¿Qué aprendimos?

- Antes de seguir agregando visualizaciones, es indispensable definir claramente qué decisiones de negocio o hallazgos analíticos se quieren mostrar.
- No toda mejora técnica debe cerrarse en el mismo sprint: en el caso de Docker, fue mejor documentar el avance y dejar una base clara para continuidad que cerrar con una solución incompleta.
- La modularización del Streamlit es importante no solo por orden del código, sino porque facilita pruebas, mantenimiento y evolución futura.
- Para que el dashboard sea estable, conviene reducir dependencias sobre archivos pesados y trabajar con insumos mínimos, resumidos y bien definidos.

## ¿Qué se propone mejorar en el siguiente sprint?

1. Definir un conjunto corto de preguntas de negocio prioritarias que el Streamlit debe responder sí o sí.
2. A partir de esas preguntas, decidir qué métricas, agregados, comparativas y visualizaciones realmente deben permanecer en la app.
3. Retomar la transición a Docker con una lista concreta de insumos necesarios para correr el dashboard sin cargar más datos de los requeridos.
4. Evaluar si conviene generar datasets derivados específicos para visualización y despliegue.
5. Mantener la disciplina de integrar cambios revisados en `Develop` y documentar su impacto funcional.

## Cierre retrospectivo

El sprint fue valioso porque permitió ordenar el estado real del proyecto: se consolidó lo que sí está funcionando, se dejó explícito lo que aún no está suficientemente maduro y se documentó un camino claro para el siguiente Scrum Master. El principal mensaje de cierre es que el proyecto ya no necesita solo más visualizaciones, sino una mejor alineación entre datos, preguntas de negocio y despliegue.