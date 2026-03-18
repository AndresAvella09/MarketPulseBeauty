# Configuración y Uso de DVC con Google Drive

Este documento explica cómo configurar y utilizar Data Version Control (DVC) en este proyecto para gestionar los datos. Utilizamos Google Drive como almacenamiento.

Para evitar bloqueos por desincronización de datos, se configuró una aplicación de autenticación propia (OAuth) en Google Cloud. Esto permite que cada miembro del equipo sincronice datos utilizando su propia cuenta de Google de forma segura.

## Requisitos Previos

1. **Acceso a Google Drive**: Debes tener tu cuenta de Google con acceso a la carpeta de Google Drive donde se almacenan los datos del proyecto.
2. **Autorización en Google Cloud**: Tu correo de Google debe haber sido añadido a la lista de usuarios de prueba en la API de Google Cloud del proyecto (pide a un administrador que te añada si aún no lo ha hecho).
3. **Credenciales Privadas**: Debes tener el **ID de Cliente (Client ID)** y el **Secreto del Cliente (Client Secret)**.

---

## Configuración Inicial (Solo la primera vez)

Una vez que tengas clonado el repositorio y DVC instalado, abre la terminal en la raíz del proyecto y configura las credenciales de la API en tu entorno.

**Es muy importante no omitir el `--local`**, esto asegura que las credenciales se guarden en un archivo local (`.dvc/config.local`) y no se suban por accidente a GitHub.

Ejecuta los siguientes comandos reemplazando los valores entre comillas con las credenciales que te proporcionaron:

```bash
# 1. Configurar el Client ID
dvc remote modify --local gdrive gdrive_client_id "EL_ID_DE_CLIENTE_AQUI"

# 2. Configurar el Client Secret
dvc remote modify --local gdrive gdrive_client_secret "EL_SECRETO_DEL_CLIENTE_AQUI"
```

> **INFO:** El archivo `.dvc/config.local` ya se encuentra en el `.gitignore` del proyecto, por lo que las llaves están protegidas.

---

## Uso en el día a día

Una vez configuradas las credenciales, cuentas con acceso directo para gestionar los datos.

### Descargar datos (Pull)

Para descargar la última versión de los datos (o los datos de la rama actual) desde Google Drive a tu máquina local, utiliza:

```bash
dvc pull
```

### Añadir nuevos datos o actualizar datos existentes

Cuando trabajas en el proyecto, es común que agregues nuevos archivos de datos o modifiques los que ya existen. El flujo para registrar estos cambios (ya sean nuevos archivos o actualizaciones) es el mismo:

1. **Rastrear los datos con DVC**:
   Dile a DVC que empiece a hacer seguimiento del nuevo archivo/carpeta o que registre los cambios de un archivo existente:

   ```bash
   dvc add data/tu_archivo_o_carpeta
   ```

   *(Nota: Esto creará o actualizará un archivo `.dvc` con el mismo nombre, por ejemplo `data/tu_archivo_o_carpeta.dvc`)*

2. **Subir los datos a Google Drive (Push)**:
   Envía los datos físicos a la nube para que estén disponibles para tu equipo:

   ```bash
   dvc push
   ```

3. **Guardar el registro en Git**:
   DVC almacena los datos pesados en Drive, pero **Git necesita guardar el apuntador** (el archivo `.dvc`) para que tus compañeros sepan qué versión descargar.

   ```bash
   git add data/tu_archivo_o_carpeta.dvc
   git commit -m "Añadidos/Actualizados datos de X"
   git push origin tu-nombre-de-rama
   ```

---

### Autenticación en el Navegador (Inicio de Sesión)

La **primera vez** que intentes hacer un `dvc pull` o `dvc push` después de configurarlo:

1. DVC generará un enlace y **se abrirá automáticamente una pestaña en tu navegador web**.
2. Te pedirá iniciar sesión con tu cuenta de Google. **Asegúrate de elegir la cuenta de correo a la que se le dio acceso a la carpeta de Drive y a la API.**
3. Como es una aplicación interna, Google mostrará una pantalla diciendo "Google hasn’t verified this app" (Google no ha verificado esta aplicación). Haz clic en **Configuración Avanzada (Advanced)** y luego en **Ir a [Nombre de la App] (unsafe/inseguro)**.
4. Concede los permisos necesarios (marcando las casillas) para que DVC pueda ver, editar y crear archivos en tu Google Drive, y haz clic en **Continuar**.
5. Cierra la pestaña y vuelve a tu terminal; la descarga o subida de tus datos habrá comenzado.

A partir de este momento, tus accesos quedarán guardados localmente y no volverá a pedirte iniciar sesión en el navegador (a menos que revoques el permiso o expiren).
