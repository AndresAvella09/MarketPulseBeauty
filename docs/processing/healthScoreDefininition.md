## Health Score

En este documento definimos el health score para medir qué tan bien le está llendo a cada una de las categorías de productos, esta fórmula puede ser actualizada según los resultados de los análisis realizados con los datos extraidos, se van a suponer ciertas fuentes de información tanto acá como en el archivo `heath_score.py`, cualquier cambio debe quedar documentado.

### Definición de variables

- $s_{i,j}$: Sentimiento promedio del producto j en la categoría i (En un rango de [-1,1])

- $v_{i,j}$: Volumen de menciones/reviews del producto j en la categoria i

### Fórmula básica
$$
HS_i = \left( \frac{\sum_{j=1}^{n} (s_{i,j} \cdot v_{i,j})
}{\sum_{j=1}^{n} v_{i,j}} + 1 \right) \cdot \frac{1}{2}
$$

### Justificación de la fórmula

1. Ponderación por volumen: El término $s \cdot v$ asegura que los productos con más "ruido" definan la salud de la categoría.
2. Escalabilidad: Al sumar 1 y dividir entre 2, trasnformamos el rango de $[-1,1]$ a $[0,1]$

---

## Actualización de implementación

Se actualizó el script de cálculo en [src/processing/health_score.py](src/processing/health_score.py) para hacerlo modular, tipado y robusto frente a fuentes de datos no unificadas. **La fórmula base se mantiene igual**. Los cambios principales son:

- **Soporte de fuentes separadas**: ahora se puede calcular el health score desde una tabla unificada (sentimiento + volumen) o desde dos archivos parquet separados (sentimiento y volumen).
- **Normalización de columnas**: el script detecta nombres alternativos comunes (por ejemplo `pd_id`, `ProductID`, `CategoryId`, `ProductCategory`) y los mapea a las columnas canónicas.
- **Robustez ante volumen cero o nulo**: si el volumen total de una categoría es 0, el score se reporta como `NaN` para evitar divisiones inválidas o sesgos.
- **Procesamiento eficiente**: se leen solo las columnas necesarias de los parquet y se agregan datos a nivel producto antes de calcular el promedio ponderado.

Si necesitas un "master table" (sentimiento + volumen por producto), se registró el backlog técnico en [docs/technical_backlog.md](docs/technical_backlog.md).

---

## Entradas esperadas

El cálculo opera a nivel **producto** dentro de una **categoría** y requiere:

- `category`: categoría del producto.
- `product_id`: identificador único del producto.
- `sentiment_score`: sentimiento promedio del producto (rango [-1, 1]).
- `mention_count` (o equivalente): volumen de menciones/reviews del producto.

Si los datos vienen separados, el script une sentimiento y volumen por `category` + `product_id` antes de calcular el score.

---

## Resumen del flujo de cálculo

1. **Carga** de parquet (tabla unificada o fuentes separadas).
2. **Estandarización** de columnas a nombres canónicos.
3. **Agregación a nivel producto**:
	 - `sentiment_score`: promedio por producto.
	 - `mention_count`: suma de volumen por producto (o conteo de filas si no existe un volumen explícito).
4. **Cálculo por categoría** usando la fórmula definida arriba.
5. **Salida** en parquet con `category`, `health_score`, `total_volume`.

---

## Uso del script

### 1) Con tabla unificada (sentimiento + volumen en un solo parquet)

```bash
python src/processing/health_score.py \
	--input data/processed/sentiment_results.parquet \
	--output data/processed/category_health_scores.parquet
```

### 2) Con fuentes separadas (sentimiento y volumen)

```bash
python src/processing/health_score.py \
	--sentiment data/processed/sentiment_results.parquet \
	--volume data/processed/volume_results.parquet \
	--output data/processed/category_health_scores.parquet
```

### 3) Si los nombres de columna son distintos

```bash
python src/processing/health_score.py \
	--input data/processed/sentiment_results.parquet \
	--category-col ProductCategory \
	--product-col pd_id \
	--sentiment-col sentiment_score \
	--volume-col review_count
```

---

## Salida

El archivo de salida contiene:

- `category`
- `health_score` en rango $[0, 1]$
- `total_volume` (suma de volumen por categoría)

Si una categoría no tiene volumen, el `health_score` queda como `NaN` para indicar que el dato no es confiable.

