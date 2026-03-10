# Diccionario de Datos — `pd_info.csv`

**Fuente:** `src/ingestion/scraper/scrape_product_info.py`

**Ruta de salida:** `data/raw/csv/pd_info.csv`

**Granularidad:** Una fila por ID de producto único (`pd_id`)

---

## Identificadores Principales

| Columna     | Tipo       | Fuente                     | Descripción                                                                                                                                                                                                            |
| ----------- | ---------- | -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pd_id`   | `string` | URL (regex)                | ID de producto de Sephora extraído de la URL. Siempre en mayúsculas (ej.`P12345`). Se usa como llave primaria.                                                                                                      |
| `keyword` | `string` | URL (coincidencia de slug) | Categoría de búsqueda que llevó a este producto. Mapeada desde fragmentos del slug de la URL via `KEYWORD_MAP`. Valores posibles:`Niacinamide`,`Hyaluronic Acid`,`Sunscreen`.`null`si no hay coincidencia. |

---

## Atributos del Producto (Playwright — página de Sephora)

| Columna           | Tipo             | Fuente                                                           | Descripción                                                                                                     |
| ----------------- | ---------------- | ---------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `brand`         | `string`       | `linkStore`JSON →`brandName`/`brand`/`BrandName`        | Nombre de la marca del producto. Se complementa con `Brand.Name`de Bazaarvoice si Playwright retorna `null`. |
| `size_and_item` | `string`       | `linkStore`JSON →`size`/`itemNumber`/`netWeight`        | Tamaño, peso neto o número de ítem del producto tal como aparece en la página (ej.`1 oz / 30 mL`).         |
| `category`      | `string`       | `linkStore`JSON → breadcrumbs `displayName`                 | Ruta de categorías separada por comas (hasta 4 niveles), ej.`Skincare, Moisturizers, Face Moisturizers`.      |
| `price`         | `string`       | `linkStore`JSON →`listPrice`/`salePrice`/`currentPrice` | Precio de venta al público como texto (ej.`$32.00`). Refleja el precio del SKU en el momento del scraping.    |
| `love_count`    | `string / int` | `linkStore`JSON →`lovesCount`/`loves`/`loveCount`       | Número de usuarios que marcaron el producto como favorito en Sephora.                                           |
| `reviews_count` | `string / int` | `linkStore`JSON →`reviews`/`reviewCount`/`numReviews`   | Total de reseñas del producto en Sephora al momento del scraping.                                               |

---

## Metadatos del Producto (Bazaarvoice — `scraper_result.json`)

| Columna         | Tipo       | Fuente                            | Descripción                                     |
| --------------- | ---------- | --------------------------------- | ------------------------------------------------ |
| `Name`        | `string` | BV `product_result.Name`        | Nombre completo del producto según Bazaarvoice. |
| `Description` | `string` | BV `product_result.Description` | Descripción del producto según Bazaarvoice.    |

---

## Estadísticas de Reseñas (Bazaarvoice — `ReviewStatistics`)

| Columna                  | Tipo                 | Fuente                                       | Descripción                                                |
| ------------------------ | -------------------- | -------------------------------------------- | ----------------------------------------------------------- |
| `AverageOverallRating` | `float`            | BV `ReviewStatistics.AverageOverallRating` | Calificación promedio en todas las reseñas (escala 0–5). |
| `FirstSubmissionTime`  | `string`(ISO 8601) | BV `ReviewStatistics.FirstSubmissionTime`  | Fecha y hora de la primera reseña enviada.                 |
| `LastSubmissionTime`   | `string`(ISO 8601) | BV `ReviewStatistics.LastSubmissionTime`   | Fecha y hora de la reseña más reciente.                   |

## Notas

* **Llave primaria:** `pd_id` (deduplicado en la ingesta; se conserva la primera ocurrencia).
* **Momento del scraping:** Todos los campos de Playwright reflejan el estado de la página al momento de la extracción. Precios, conteos de favoritos y reseñas pueden cambiar con el tiempo.
* **Jerarquía de fallback:** La marca se llena primero desde Playwright; Bazaarvoice completa los valores `null` restantes.
* **Inconsistencia de tipos:** `love_count` y `reviews_count` pueden quedar como texto o entero según la ruta de extracción. Convertir a `int` antes del análisis.
* **Valores faltantes:** `null` / `NaN` indica que el campo no fue encontrado en la página ni en el payload de BV para ese producto.
