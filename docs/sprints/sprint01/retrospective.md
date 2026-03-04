### Retrospectiva

Como equipo logramos cumplir los objetivos planeados inicialmente, aunque los tiempos fueron ajustados debido a que el proyecto comenzó durante la semana de exámenes del primer tercio del semestre 2026-1.

Esto generó una distribución desigual del trabajo a lo largo de la semana, concentrándose principalmente en el fin de semana y el lunes previo al siguiente Sprint Planning.

#### Problemas identificados

- **Dificultad técnica en la extracción de datos**
 El web scraping se realizó utilizando un repositorio antiguo de GitHub que no era directamente funcional, lo que obligó a realizar ajustes manuales y resolver problemas de acceso a la página de Sephora.

**Acción de mejora:**
- Evaluar previamente la vigencia de repositorios externos antes de adoptarlos.
- Documentar decisiones técnicas en el momento en que se tomen.

- **Incompatibilidad de librerías:** Se presentaron problemas con versiones de pandas y `Python`, lo que generó retrasos iniciales.

**Acción de mejora:**

1. Establecer versión fija de Python para el proyecto.
2. Utilizar un entorno virtual documentado.
3. Considerar automatización básica con GitHub Actions para validar compatibilidad. 


- **Falta de comunicación directa:** Durante el desarrollo del notebook de análisis inicial, aún no estaban disponibles los datos finales del scraping, lo que obligó a trabajar con un dataset sintético temporal.

**Acción de mejora:**

- Establecer una mini reunión de sincronización a mitad del sprint.
- Definir claramente dependencias entre tareas en el Sprint Planning.
- Marcar tareas bloqueadas en el tablero cuando dependan de otra.

#### Conclusión del sprint

El sprint permitió validar la viabilidad técnica del proyecto y detectar áreas de mejora en la coordinación interna. Las lecciones aprendidas serán aplicadas en el siguiente sprint para mejorar eficiencia y comunicación.