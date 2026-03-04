# MarketPulse Beauty

## Panel de Inteligencia Retail

### Documento de Investigación de Fuente de Datos

---

| Campo             | Detalle                                         |
| ----------------- | ----------------------------------------------- |
| Proyecto          | MarketPulse Beauty                              |
| Tipo de Documento | Investigación y Evaluación de Fuente de Datos |
| Sprint            | Sprint 1                                        |
| Estado            | Completado                                      |
| Fecha             | Marzo 2026                                      |

---

## 1. Objetivo

Este documento identifica, evalúa y selecciona una fuente de datos pública y accesible para extraer reseñas de productos de belleza y cuidado personal como parte del proyecto MarketPulse Beauty.

La evaluación considera tres ejes críticos:

- **Disponibilidad de texto:** riqueza y completitud del contenido de las reseñas
- **Disponibilidad de fecha:** presencia de marcas temporales para permitir análisis temporal
- **Volumen:** cantidad suficiente de reseñas para soportar modelamiento estadístico y NLP

---

## 2. Fuentes Candidatas Evaluadas

Se evaluaron cuatro plataformas como posibles fuentes de datos de reseñas de productos de belleza. Cada una fue analizada según los criterios definidos en el alcance del proyecto.

| Plataforma                | Texto de Reseña  | Fecha         | Volumen     | Acceso API            | Riesgo ToS |
| ------------------------- | ----------------- | ------------- | ----------- | --------------------- | ---------- |
| **Sephora.com**     | ✓ Texto completo | ✓ Disponible | ✓ Muy alto | No oficial / scraping | ⚠ Medio   |
| **Ulta Beauty**     | ✓ Texto completo | ✓ Disponible | ⚠ Medio    | Sin API oficial       | ✗ Alto    |
| **Amazon Beauty**   | ✓ Texto completo | ✓ Disponible | ✓ Muy alto | PA-API (limitada)     | ✗ Alto    |
| **Kaggle Datasets** | ✓ Texto completo | ⚠ Parcial    | ✓ Alto     | Descarga directa      | ✓ Bajo    |

---

## 3. Fuente Seleccionada: Sephora.com

### 3.1 Justificación de la Selección

Sephora.com fue seleccionada como la fuente principal de datos para el proyecto MarketPulse Beauty con base en los siguientes factores:

| Criterio                           | Evaluación                                                                                                                                                                              |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Riqueza del texto**        | Las reseñas incluyen título, cuerpo, tipo de piel, rango de edad y votos de utilidad, proporcionando características ricas para análisis NLP.                                        |
| **Datos temporales**         | Cada reseña incluye fecha de envío, permitiendo análisis de series de tiempo y seguimiento de cambios en la percepción a lo largo de meses y años.                                  |
| **Volumen**                  | Los productos acumulan regularmente cientos o miles de reseñas. Los productos más populares superan las 10.000 reseñas. Datasets públicos en Kaggle contienen millones de registros. |
| **Cobertura de categorías** | Incluye cuidado de la piel, maquillaje, fragancias, cuidado capilar y productos corporales — segmentos centrales para MarketPulse Beauty.                                               |
| **Metadatos estructurados**  | Los productos incluyen categoría, marca, rango de precio e ingredientes junto con reseñas, permitiendo análisis multidimensional.                                                     |

---

### 3.2 Dataset Recomendado: *Sephora Products and Skincare Reviews (Kaggle)*

Para el Sprint 1, el equipo utilizará el dataset público disponible en Kaggle: **"Sephora Products and Skincare Reviews"** (Nikiforova, 2023). Este dataset proporciona:

- Más de 1 millón de reseñas en 8.000+ productos
- Campos de reseña: título, cuerpo, calificación, tipo de piel, edad, votos de utilidad y fecha de envío
- Metadatos del producto: marca, categoría, precio, ingredientes y conteo de “love”
- Licencia: Community Data License Agreement (CDLA-Sharing), adecuada para uso académico y de investigación

Este dataset permite comenzar el modelamiento y el desarrollo del pipeline inmediatamente sin requerir infraestructura de scraping durante el Sprint 1.

---

### 3.3 Estrategia Futura de Scraping (Sprint 2+)

En sprints posteriores, se implementará la extracción de datos en vivo desde Sephora.com para mantener reseñas actualizadas. El enfoque técnico planificado incluye:

- Solicitudes HTTP usando la librería `requests` de Python con rotación de headers
- Parseo HTML mediante `BeautifulSoup4` para extracción estructurada
- Control de tasa (mínimo 2–5 segundos entre solicitudes)
- Almacenamiento de respuestas JSON crudas antes de la transformación
- Scraping incremental para evitar reprocesar registros existentes

---

## 4. Limitaciones

Las siguientes limitaciones se documentan para garantizar transparencia y apoyar la planeación de sprints:

| Limitación                        | Descripción                                                                                                                                | Mitigación                                                                            |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| **Dataset estático**        | El dataset de Kaggle tiene una fecha de corte fija y no refleja reseñas posteriores a su recolección.                                     | Implementar scraping en vivo a partir del Sprint 2.                                    |
| **Sesgo por categoría**     | El dataset se inclina hacia skincare; otras categorías pueden tener menos registros.                                                       | Complementar con scraping dirigido por categoría.                                     |
| **Sin compras verificadas**  | A diferencia de Amazon, Sephora no distingue compradores verificados.                                                                       | Usar votos de utilidad como proxy de confiabilidad.                                    |
| **Idioma**                   | El dataset es principalmente en inglés. El análisis multilingüe está fuera del alcance del MVP.                                         | Documentar como restricción conocida del alcance.                                     |
| **Distribución de ratings** | Las reseñas de belleza tienden a concentrarse en 4–5 estrellas (distribución en J), lo que puede afectar el modelamiento de sentimiento. | Aplicar muestreo estratificado por rating para conjuntos de entrenamiento balanceados. |

---

## 5. Riesgos Técnicos

| Riesgo                                 | Severidad | Descripción                                                                                           | Contingencia                                                                                       |
| -------------------------------------- | --------- | ------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| **Violación de ToS (scraping)** | ⚠ Medio  | Los términos de servicio de Sephora restringen el scraping automatizado. Puede generar bloqueo de IP. | Usar dataset de Kaggle en Sprint 1; aplicar scraping ético y con límites en sprints posteriores. |
| **Bloqueo de IP**                | ⚠ Medio  | Solicitudes de alta frecuencia pueden activar detección de bots.                                      | Implementar delays, rotación de user-agent e infraestructura de proxy si es necesario.            |
| **Cambios en el esquema**        | ✓ Bajo   | Cambios en la estructura HTML pueden romper scripts de parseo.                                         | Construir parsers modulares con validaciones y alertas automáticas.                               |
| **Volumen / memoria**            | ✓ Bajo   | 1M+ registros pueden generar problemas de memoria en desarrollo local.                                 | Procesamiento por chunks con pandas; usar muestra del 10% en etapas tempranas.                     |
| **Ruido y spam**                 | ✓ Bajo   | Reseñas falsas o promocionales pueden distorsionar métricas.                                         | Aplicar filtros de longitud mínima y umbrales de votos de utilidad.                               |

---

## 6. Plan de Acción Sprint 1

| # | Tarea                                                                                           | Responsable   | Estado    |
| - | ----------------------------------------------------------------------------------------------- | ------------- | --------- |
| 1 | Descargar dataset de Sephora desde Kaggle y almacenarlo en `/data/raw/`                       | Data Engineer | Pendiente |
| 2 | Validar esquema: columnas, tipos de datos, nulos y rangos de fecha                              | Data Engineer | Pendiente |
| 3 | Generar reporte EDA: distribución de ratings, longitud de reseñas, cobertura temporal         | Data Analyst  | Pendiente |
| 4 | Definir pipeline de preprocesamiento: tokenización, stopwords, normalización de codificación | NLP Lead      | Pendiente |
| 5 | Documentar diccionario de datos y definición de columnas en `/docs/data/`                    | Scrum Master  | Pendiente |

---

## 7. Conclusión

Sephora.com, accedida a través del dataset público de Kaggle para el Sprint 1, representa la fuente de datos óptima para MarketPulse Beauty. Cumple con todos los criterios de evaluación: texto rico en contenido, metadatos temporales y volumen suficiente para modelamiento NLP y análisis de tendencias.

El principal riesgo técnico — cumplimiento de los Términos de Servicio para scraping en vivo — se mitiga en el corto plazo mediante el uso del dataset de Kaggle, con una estrategia estructurada de scraping planificada para el Sprint 2 en adelante.

Esta decisión es trazable, documentada y alineada con los principios DataOps del proyecto: transparencia y reproducibilidad.

---

*MarketPulse Beauty | Investigación de Fuente de Datos Sprint 1 | Confidencial – Uso Interno*
