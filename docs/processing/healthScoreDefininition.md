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

