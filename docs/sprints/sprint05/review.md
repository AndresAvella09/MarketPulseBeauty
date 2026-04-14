# Sprint Review — Sprint 5

Proyecto: MarketPulse Beauty  
Sprint Goal: Consolidar la capa de visualización en Streamlit, estabilizar la app con datos Parquet locales y cerrar el periodo de Scrum Master con las tareas priorizadas integradas en `Develop`.

* * *

### Resumen del sprint

Durante este sprint se priorizó la estabilización de la capa de presentación del proyecto y el cierre operativo de varias tareas acumuladas del frente de analítica visual. El enfoque principal estuvo en fortalecer la app de Streamlit para que funcionara con datos locales en formato Parquet, incorporar mejoras modulares en la estructura del dashboard y alinear la visualización con el estado real de los datos disponibles.

Además, durante este periodo se impulsó como lineamiento de Scrum Master el cierre ordenado de las tareas abiertas del rango trabajado, promoviendo que las pull requests asociadas fueran revisadas, integradas en `Develop` y sus issues correspondientes quedaran cerradas al finalizar el periodo.

* * *

### Logros obtenidos

#### paulagacha0 — Streamlit y estabilización de visualización
1. Se consolidó la ejecución local del dashboard en Streamlit utilizando datasets en formato Parquet, en lugar de CSV, respetando la arquitectura actual del proyecto.
2. Se implementaron mejoras funcionales en la app para manejo de errores cuando los archivos no existen, están vacíos o no contienen ciertas columnas esperadas.
3. Se incorporaron mejoras para filtrar y enfocar la visualización sobre las líneas de producto estratégicas definidas por el proyecto, evitando mostrar productos fuera del alcance analítico.
4. Se inició la modularización de la aplicación para reducir acoplamiento en `app.py` y facilitar futuras extensiones del dashboard.
5. Se propusieron mejoras orientadas a negocio para que la visualización no sea solo exploratoria, sino que responda preguntas concretas del proyecto.

#### AndresAvella09 — continuidad técnica y soporte a la integración
6. Se dio continuidad a la línea técnica del proyecto sobre la rama `Develop`, apoyando la integración de cambios y la estabilidad general del repositorio.
7. Se mantuvo el flujo de revisión e integración de pull requests del equipo, permitiendo consolidar avances previos y actuales en una rama de trabajo común.
8. También se dejó encaminada la continuidad del proyecto desde la coordinación del siguiente periodo, al quedar como Scrum Master entrante con contexto suficiente sobre el estado técnico y organizacional del repositorio.

#### Ch0cmilo — transición técnica y preparación de despliegue
9. Se avanzó en la transición del proyecto hacia una ejecución más portable basada en Docker.
10. Durante la daily del **13/04** se identificó que esta transición requería ajustes adicionales sobre organización de datos, rutas, y selección de insumos mínimos para el funcionamiento del dashboard.
11. Como resultado, se decidió dejar esta línea **pendiente para el siguiente Scrum Master**, con el fin de retomarla con mejor definición técnica y evitar un cierre incompleto.

#### Equipo
12. Se mantuvo la integración de cambios en `Develop`, procurando que los avances funcionales de este periodo quedaran consolidados en la rama principal de trabajo del equipo.
13. El periodo cerró con las tareas trabajadas durante este periodo de Scrum Master revisadas, con sus pull requests asociadas integradas en `Develop` y sus issues correspondientes cerradas.

* * *

### Decisiones relevantes del sprint

- Se definió que el dashboard debe construirse con base en los datos efectivamente disponibles en Parquet, evitando depender de datasets intermedios o estructuras sintéticas.
- Se acordó que la evolución del Streamlit debe orientarse menos a “mostrar todo” y más a responder preguntas de negocio concretas.
- Se dejó explícito que la transición a Docker no se considera descartada, sino **pospuesta estratégicamente** hasta contar con una definición más precisa sobre qué datos y artefactos deben empaquetarse.

* * *

### Aspectos pendientes

Las siguientes líneas quedan abiertas para el siguiente sprint:

1. **Definir con precisión las preguntas de negocio que debe responder el Streamlit**  
   Hace falta aterrizar qué métricas, gráficos y comparaciones son indispensables para demostrar valor del proyecto, y cuáles visualizaciones son accesorias.

2. **Continuar la transición a Docker**  
   La línea ya fue iniciada, pero requiere depurar dependencias, organizar mejor los insumos de datos y asegurar que el dashboard corra con un subconjunto controlado y suficiente de archivos.

3. **Refinar la capa de datos para visualización**  
   Será necesario definir qué datasets resumidos o derivados conviene exponer al dashboard para evitar sobrecarga y mejorar estabilidad.

* * *

### Conclusión

El Sprint 5 permitió cerrar el periodo de Scrum Master con avances reales en la capa de visualización, mayor claridad sobre el uso de Parquet en local y una base más sólida para la continuidad del proyecto. Aunque no todas las líneas quedaron completamente cerradas, sí se logró dejar integrado en `Develop` el trabajo priorizado del periodo y documentadas las decisiones clave para que el siguiente Scrum Master continúe con contexto suficiente.