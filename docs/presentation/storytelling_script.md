# Libreto de presentacion (3 personas)

Formato: cada slide indica quien habla, objetivo y guion sugerido.
Distribucion propuesta:
- Speaker 1: negocio (Fase 1 completa + apertura)
- Speaker 2: tecnica base (datos, arquitectura, pipeline)
- Speaker 3: tecnica avanzada (insights, calidad, Scrum, cierre)

---

## Slide 1 - Portada
**Speaker 1**
**Objetivo:** abrir con contexto y promesa.
**Guion:**
"Somos MarketPulse Beauty, un dashboard de inteligencia retail para belleza. Hoy les contamos dos fases: primero por que esto es valioso para una pyme, y luego como lo construimos para que sea confiable y escalable."

---

## Slide 2 - Agenda
**Speaker 1**
**Objetivo:** enmarcar el recorrido.
**Guion:**
"Vamos en dos partes. Fase 1: problema, propuesta de valor y como se usa el dashboard. Fase 2: datos, arquitectura, calidad y metodologia Scrum. Cerramos con siguientes pasos."

---

# Fase 1: Vender el valor

## Slide 3 - Por que esto importa a una pyme
**Speaker 1**
**Objetivo:** mostrar dolor y urgencia.
**Guion:**
"Las pymes tienen mucha data publica pero poco tiempo para analizarla. Reviews y tendencias cambian rapido, y se detectan tarde. Al final, se decide por intuicion. MarketPulse convierte ese ruido en decisiones claras."

---

## Slide 4 - Propuesta de valor
**Speaker 1**
**Objetivo:** explicar el beneficio directo.
**Guion:**
"Automatizamos el analisis de reviews y tendencias, resumimos el estado del producto en indicadores simples, y detectamos alertas tempranas. Esto reduce tiempo de analisis y acelera decisiones comerciales."

---

## Slide 5 - Tres pilares del producto
**Speaker 1**
**Objetivo:** presentar el producto en 3 bloques memorables.
**Guion:**
"El producto se sostiene en tres pilares: Health Score para entender performance, Review Insights para detectar temas y sentimiento, y Trend Analysis para ver interes de busqueda y picos de demanda."

---

## Slide 6 - Flujo de decision en el dashboard
**Speaker 1**
**Objetivo:** explicar el journey del usuario.
**Guion:**
"El dashboard esta pensado como una lectura ejecutiva. Empezamos con un resumen, vemos cambios recientes, entendemos el motivo, validamos con tendencias, revisamos alertas y, si es necesario, bajamos al detalle. En minutos se entiende estado, causa y riesgo."

---

## Slide 7 - KPIs y visuales que guian acciones
**Speaker 1**
**Objetivo:** conectar valor con metricas.
**Guion:**
"Los KPIs clave son rating promedio, Health Score y volumen de reviews. Sumamos porcentaje de recomendacion y frecuencia de atributos. Esto se presenta con rankings, barras y lineas temporales para tomar decisiones claras."

---

# Fase 2: Como se construyo

## Slide 8 - Como se construyo
**Speaker 2**
**Objetivo:** cambiar el enfoque a la parte tecnica.
**Guion:**
"Ahora vamos a la segunda fase: como lo construimos. La base es una arquitectura orientada a datos con enfoque DataOps, un pipeline de ingesta a delivery, y un diseno que permite crecer con scraping, tendencias y alertas."

---

## Slide 9 - Estrategia de datos
**Speaker 2**
**Objetivo:** justificar la fuente de datos y el arranque.
**Guion:**
"La fuente principal es Sephora, porque tiene reviews ricas y con fecha. Para arrancar usamos un dataset Kaggle con mas de un millon de reviews y 8 mil productos. Luego planificamos scraping incremental. Documentamos limitaciones como sesgo de categorias e idioma."

---

## Slide 10 - Pipeline y arquitectura
**Speaker 2**
**Objetivo:** explicar el flujo tecnico.
**Guion:**
"El pipeline sigue medallion: raw, bronze, silver, gold. Orquestamos con Airflow y cinco DAGs: ingestion, silver, gold, trends y data quality. Guardamos objetos en MinIO y resultados en PostgreSQL, y el dashboard corre en Streamlit con Docker."

---

## Slide 11 - Inteligencia y calidad
**Speaker 3**
**Objetivo:** mostrar el valor tecnico diferencial.
**Guion:**
"Sobre el gold layer construimos un motor de insights con rating, volumen, health score y tendencias. Definimos reglas con umbrales para alertas y un monitoreo entre ejecuciones para detectar regresiones. Asi garantizamos consistencia y confiabilidad."

---

## Slide 12 - Metodologia Scrum aplicada
**Speaker 3**
**Objetivo:** demostrar metodologia y gestion.
**Guion:**
"Trabajamos en 6 sprints de dos semanas con eventos Scrum formales. En el Sprint 1 definimos repo, librerias y fuentes. En el Sprint 6 consolidamos el producto analitico, datasets derivados y Docker funcional. Todo queda documentado por sprint para trazabilidad."

---

# Cierre

## Slide 13 - Cierre: valor + ejecucion
**Speaker 3**
**Objetivo:** cerrar con vision y siguiente paso.
**Guion:**
"MarketPulse traduce datos publicos en decisiones comerciales. Tenemos un MVP con arquitectura escalable y metodologia solida. El siguiente paso es la demo con casos reales y validacion con pymes. Gracias."

---

## Opciones de redistribucion (si se necesita)
- Si quieren balancear mas: Speaker 2 presenta Slide 11 y Speaker 3 solo Scrum + cierre.
- Si quieren mas negocio: Speaker 1 agrega el cierre y Speaker 3 se queda solo con Scrum.
