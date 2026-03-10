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

---

# Diccionario de Datos — `review_data.csv`

**Fuente:** `src/ingestion/scraper/parse_reviews.py`

**Ruta de salida:** `data/raw/csv/review_data.csv`

**Granularidad:** Una fila por reseña individual

---

## Identificador

| Columna   | Tipo       | Fuente                                  | Descripción                                                                           |
| --------- | ---------- | --------------------------------------- | -------------------------------------------------------------------------------------- |
| `pd_id` | `string` | `scraper_result.json`(llave del dict) | ID de producto de Sephora. Llave foránea que conecta con `pd_id`en `pd_info.csv`. |

---

## Atributos de la Reseña (Bazaarvoice)

| Columna            | Tipo                 | Fuente                    | Descripción                                                                                  |
| ------------------ | -------------------- | ------------------------- | --------------------------------------------------------------------------------------------- |
| `AuthorId`       | `string`           | `review.AuthorId`       | Identificador anónimo del autor de la reseña en Bazaarvoice.                                |
| `Rating`         | `int`              | `review.Rating`         | Calificación otorgada por el reseñador (escala 1–5).                                       |
| `Title`          | `string`           | `review.Title`          | Título de la reseña escrito por el usuario.                                                 |
| `ReviewText`     | `string`           | `review.ReviewText`     | Cuerpo completo de la reseña.                                                                |
| `Helpfulness`    | `string / float`   | `review.Helpfulness`    | Puntaje de utilidad de la reseña según votos de otros usuarios.                             |
| `SubmissionTime` | `string`(ISO 8601) | `review.SubmissionTime` | Fecha y hora en que se envió la reseña.                                                     |
| `IsRecommended`  | `bool`             | `review.IsRecommended`  | Indica si el reseñador recomienda el producto (`True`/`False`).`null`si no respondió. |

---

## Perfil del Reseñador (Bazaarvoice — `ContextDataValues`)

Datos de caracterización personal declarados voluntariamente por el reseñador.

| Columna       | Tipo       | Fuente                                | Descripción                                                                    |
| ------------- | ---------- | ------------------------------------- | ------------------------------------------------------------------------------- |
| `eyeColor`  | `string` | `ContextDataValues.eyeColor.Value`  | Color de ojos declarado por el reseñador (ej.`Brown`,`Blue`).              |
| `hairColor` | `string` | `ContextDataValues.hairColor.Value` | Color de cabello declarado por el reseñador (ej.`Black`,`Blonde`).         |
| `skinTone`  | `string` | `ContextDataValues.skinTone.Value`  | Tono de piel declarado por el reseñador (ej.`Fair`,`Medium`,`Deep`).     |
| `skinType`  | `string` | `ContextDataValues.skinType.Value`  | Tipo de piel declarado por el reseñador (ej.`Oily`,`Dry`,`Combination`). |

---

## Tabla de Referencia de Links

| Columna           | Tipo       | Fuente                               | Descripción                                                                                              |
| ----------------- | ---------- | ------------------------------------ | --------------------------------------------------------------------------------------------------------- |
| `pd_id`         | `string` | URL (regex)                          | ID de producto extraído de la URL. Llave primaria.                                                       |
| `product_links` | `string` | `data/raw/links/product_links.txt` | URL de la página del producto en Sephora. Una URL por `pd_id`(primera ocurrencia tras deduplicación). |

> Esta tabla intermedia (`pd_links_df`) se construye en memoria y no se persiste en disco. Se usa para enriquecer o cruzar datos si es necesario.

---

## Notas Generales

### `pd_info.csv`

* **Llave primaria:** `pd_id` (deduplicado en la ingesta; se conserva la primera ocurrencia).
* **Momento del scraping:** Todos los campos de Playwright reflejan el estado de la página al momento de la extracción. Precios, conteos de favoritos y reseñas pueden cambiar con el tiempo.
* **Jerarquía de fallback:** La marca se llena primero desde Playwright; Bazaarvoice completa los valores `null` restantes.
* **Inconsistencia de tipos:** `love_count` y `reviews_count` pueden quedar como texto o entero según la ruta de extracción. Convertir a `int` antes del análisis.
* **Valores faltantes:** `null` / `NaN` indica que el campo no fue encontrado en la página ni en el payload de BV para ese producto.

### `review_data.csv`

* **Granularidad:** Una fila por reseña — un mismo `pd_id` puede tener múltiples filas.
* **Relación con `pd_info.csv`:** Unir por `pd_id` para cruzar reseñas con atributos del producto.
* **Perfil del reseñador:** Los campos `eyeColor`, `hairColor`, `skinTone` y `skinType` son autodeclarados y opcionales; se espera una alta tasa de `null`.
* **`Helpfulness`:** Puede ser `null` si la reseña no ha recibido votos de utilidad. Verificar tipo antes de operar numéricamente.
