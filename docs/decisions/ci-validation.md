# Flujo de validación de CI

## Qué valida el flujo de trabajo
- Configuración del entorno a partir de requirements.txt usando una versión fija de Python.
- Validación de contratos de datos utilizando muestras sintéticas en tests/sample_data.
- Verificación de imports para módulos críticos para detectar problemas de sintaxis y dependencias de forma temprana.
- Pruebas unitarias con umbrales de cobertura para módulos críticos.

## Por qué se eligieron estas verificaciones
- La instalación de dependencias confirma que requirements.txt está completo y es reproducible.
- La validación de contratos de datos asegura que los supuestos del pipeline se cumplen antes del procesamiento.
- Las verificaciones de imports detectan módulos rotos sin ejecutar scripts completos.
- Los umbrales de cobertura protegen la lógica central de regresiones silenciosas.

## Problemas que esto previene
- Dependencias faltantes o desactualizadas en CI o en nuevos entornos.
- Datos inválidos de reseñas o tendencias que pasan a etapas posteriores.
- Errores de sintaxis o imports faltantes en módulos críticos.
- Cambios sin cobertura en health score, contratos de datos y lógica de limpieza de texto.

## Notas
- Los datos de muestra se encuentran en tests/sample_data y están diseñados para ser pequeños y rápidos.
- Si los tiempos de CI aumentan, considera dividir las dependencias pesadas en extras.