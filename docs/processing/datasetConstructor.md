## Descripción del proceso de transformación inicial de los datos

### Para los datos de google trends

Para estos, la idea es separar inicialmente el conjunto de datos extraido de google trends en varias tablas que puedan ser utilizadas para su análisis independiente (Fata agregar procesos de enriquesimiento), las herramientas utilizadas son las siguientes:

- `separe_google_dataset(self, output_dir)`: Esta función, perteneciente a la clase `DatasetConstructor` que se encarga de separar el conjunto de datos que se encuentra en el directorio especificado y crea los csv de cada uno de los `keyword` por separado en el directorio `/data/processed/google_trends`.
- `process_google_trends.py`: Oquesta la llamada e implementación de la anterior función sobre todos los posibles conjuntos de datos crudos en la carpta especificada.