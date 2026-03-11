### Planificación del Sprint 3

En este documento se definen los objetivos de las tareas planificadas para el tercer sprint. El despliegue detallado de cada una se encuentra en `backlog.md`.

#### Daniel

1. **Migrar almacenamiento a formato Parquet con particionamiento** : objetivo — reemplazar los CSV planos por un formato columnar eficiente, particionado y compatible con herramientas analíticas futuras.
2. **Implementar Data Contracts** : objetivo — establecer un mecanismo formal de validación que garantice integridad de los datos entre etapas del pipeline, cubriendo tanto reseñas como Google Trends.
3. **Construir pipeline de ingesta incremental** : objetivo — optimizar el scraping para que sea eficiente, no redundante y compatible con el nuevo almacenamiento.

#### Diego

1. **Configurar CI básico con GitHub Actions** : objetivo — garantizar que cada cambio al repositorio sea validado automáticamente sin intervención manual.
3. **Implementar pruebas unitarias para módulos críticos** : objetivo — asegurar el correcto funcionamiento de los módulos más sensibles del proyecto ante cambios futuros.
4. **Implementar versionado de datos con DVC** : objetivo — habilitar reproducibilidad completa del dataset desde cualquier punto histórico del proyecto.

#### Paula

1. **Mejorar y estabilizar el dashboard de Streamlit** : objetivo — conectar el dashboard al nuevo almacenamiento Parquet e incorporar las secciones de Trends y Alertas.
2. **Implementar visualización comparativa entre productos** : objetivo — habilitar al usuario del dashboard para contrastar directamente el desempeño de múltiples productos en una sola vista.
