## Documento de transición del mando Scrum Master

### Trabajo completado
Durante los primeros dos sprints se completaron las primeras fuentes de información, como lo fueron el scrapping de reseñas de sephora y los datos extraidos de google trends, así como un proceso inicial de transofrmación y disposición de los datos (Parcialmente).

puntualmente los avances que se tienen son los siguintes:
- Extracción de datos de google trends a partir de las palabras clave definidas
- Extracción de reseñas de productos de sephora robusta y de comprobación
- Procesamiento inicial de los datos obtenidos durante la extracción y iniciación de la fase de carga de los datos
- Análisis de resultados iniciales de google trends

Las tareas que quedaron faltando (Al momento de redactar esto) son las siguientes:
- Definición del Health Score
- Análisis de sentimiento inicial
- Topic modeling de las reseñas
- Dashboard inicial

### Trabajos faltantes

- **Documentación debida de los conjuntos de datos extraidos**: Al momento de acceder a los datos de google trends, no se entiende directamente qué significa cada columna, o los datos que se tienen que cada uno, es necesario documentar lo que significa da registro y las columnas correspondientes.
- **Definir correctamente las informacón extraida**: Existe una posible confusión entre categoría de producto (Niacinamida, shapoo, ácido) y cada producto en si, en el que cada producto, pertenece a una de las tres categorías, es necesario definir esta separación de manera correcta, para poder procesar correctamente los datos y hacer los cálculos de score pertinentes.
- **Definición del pipeline**: tras definir una ruta clara para la ingestión, procesamiento e implementación de los datos en el dashboard, es necesario un orquestador que automatice este proceso por ahora con una única llamada, los requisistos quedan a disposición de la persona que se le pasa el cargo.
- **Tareas faltantes**: Toda tarea que no haya sido terminada durante el sprint 1 o 2, debe ser completada en el siguiente sprint con caracter urgente previo a las taeras de los siguientes sprints.

**Nota:** Para la agilidad del desarrollo se recomienda no trasladar las tareas faltantes del sprint 1 o 2 a los siguientes spints, sino que las tareas de estos últmos deben estar planeadas como si el sprint 1 y 2 hubisen sido terminadas, por eso estas tareas atrasadas deben tomarse con la mayor prioridad posible. Si se decide trasladar estas tareas al backlog de los siguientes sprints, se sugiere argumentar esta decisión por tiempos o alcance individual de las tareas. Esta decisión debe estar dentro de un pull request para que el resto del equipo esté de acuerdo con la decisión. A su vez, se propone hacer una sesión de pair programing donde los tres integrantres se reunan a completar estas tareas y poder continuar con backlog definido en el sprint03.