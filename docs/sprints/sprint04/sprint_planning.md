### Planificación del Sprint 4 (Cierre de Deuda Técnica)

En este sprint no se agregan nuevos requerimientos para garantizar que todos los módulos críticos alcancen el estado "Done" con los datos finales corregidos.

#### Daniel

1. **Migración total a Parquet** : Reemplazar definitivamente los CSV por el formato columnar particionado.
2. **Implementación de Data Contracts** : Validar la calidad de los datos de reseñas y Google Trends bajo el esquema final.
3. **Ingesta incremental funcional** : Asegurar que el scraper solo traiga datos nuevos hacia el almacenamiento Parquet.

#### Diego

1. **GitHub Actions (CI) al 100%** : Automatizar la validación de dependencias y ejecución de contratos en el repositorio.
2. **Pruebas Unitarias** : Cobertura completa de tests para `health_score.py`, `data_contracts.py` y `clean_text.py`.
3. **DVC Final** : Garantizar que todo el equipo pueda sincronizar los datasets "gold" mediante `dvc pull`.

#### Paula

1. **Dashboard estable y productivo** : Conectar la interfaz de Streamlit al nuevo almacenamiento Parquet y activar alertas.
2. **Análisis de Correlación y Comparativas** : Implementar las visualizaciones cruzadas entre sentimientos y Google Trends en el dashboard.
