
---

## Instrucciones Detalladas Paso a Paso

### Parte 1: Preparar S3 (Almacenamiento)

1.  **Ir a S3:** Busca `S3` en la consola de AWS y entra al servicio.
2.  **Crear Bucket:**
    *   Clic en `+ Crear bucket`.
    *   **Nombre del bucket:** Elige un nombre **único global** (ej. `tu-alias-glue-workshop-fecha`. 
    *   **Región de AWS:** Selecciona tu región de trabajo (ej. `us-east-1`). 
    *   Deja todas las demás opciones por defecto (importante: `Bloquear todo el acceso público` debe estar **marcado**).
    *   Clic en `Crear bucket`.
3.  **Crear Carpetas `input` y `output`:**
4.  **Subir Archivos a `input`:**
    *   `users.csv` y `listening_history.csv`.

### Parte 2: Crear Rol IAM (Permisos)

Glue necesita permiso para acceder a S3 y a sí mismo.

1.  **Ir a IAM:** Busca `IAM` en la consola de AWS y entra al servicio.
2.  **Crear Rol:**
    *   Menú izquierdo: `Roles` -> Clic `Crear rol`.
    *   **Entidad de confianza:** `Servicio de AWS`.
    *   **Caso de uso:** Busca y selecciona `Glue`. Haz clic en `Siguiente`.
    *   **Agregar permisos:**
        *   Busca y marca la política `AWSGlueServiceRole`.
        *   Ahora, clic en `Crear política` (abre nueva pestaña).
            *   **En la nueva pestaña:** Selecciona `JSON`. Borra todo y pega esto ( **REEMPLAZA `[TU_BUCKET_AQUI]` con el nombre de bucket** ):
                ```json
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "s3:GetObject",
                                "s3:PutObject",
                                "s3:DeleteObject"
                            ],
                            "Resource": [
                                "arn:aws:s3:::[TU_BUCKET_AQUI]/input/*",
                                "arn:aws:s3:::[TU_BUCKET_AQUI]/output/*"
                            ]
                        },
                        {
                            "Effect": "Allow",
                            "Action": "s3:ListBucket",
                            "Resource": "arn:aws:s3:::[TU_BUCKET_AQUI]",
                             "Condition": {
                                 "StringLike": {
                                     "s3:prefix": [
                                         "input/*",
                                         "output/*"
                                     ]
                                 }
                             }
                        }
                    ]
                }
                ```
            *   Clic `Siguiente: Etiquetas` -> `Siguiente: Revisar`.
            *   **Nombre:** `Policy-GlueS3-InputOutput-Workshop`.
            *   Clic `Crear política`. Cierra esta pestaña.
        *   **De vuelta en la pestaña de creación de rol:** Clic en el botón de refrescar políticas. Busca y marca la política `Policy-GlueS3-InputOutput-Workshop` que acabas de crear.
        *   Asegúrate que `AWSGlueServiceRole` y `Policy-GlueS3-InputOutput-Workshop` estén marcadas. Clic `Siguiente`.
    *   **Nombre del rol:** `Role-Glue-Workshop`.
    *   Clic `Crear rol`.

### Parte 3: Catalogar Datos (Glue Crawler)

El Crawler detecta la estructura (esquema) de los datos en S3.

1.  **Ir a AWS Glue:** Busca `Glue` en la consola.
2.  **Crear Base de Datos:**
    *   Menú izquierdo: `Databases` -> `Add database`.
    *   **Database name:** `spotify_db`. 
    *   Clic `Create database`.
3.  **Crear Crawler:**
    *   Menú izquierdo: `Crawlers` -> `Create crawler`.
    *   **Nombre:** `spotify_crawler`. Clic `Next`.
    *   **Data sources:** `Add a data source`.
        *   Data source: `S3`.
        *   S3 path: Clic `Browse S3` -> Navega y selecciona la carpeta `input` de tu bucket -> Clic `Choose`.
        *   Clic `Add an S3 data source`. Clic `Next`.
    *   **IAM Role:** `Choose an existing IAM role` -> Selecciona `Role-Glue-Workshop` (el rol que creaste). Clic `Next`.
    *   **Output configuration:**
        *   Target database: Selecciona `spotify_db`.
        *   Clic `Next`.
    *   **Review y Create:** Revisa y clic `Create crawler`.
4.  **Ejecutar Crawler:**
    *   Busca `spotify_crawler` en la lista, márcalo.
    *   Clic `Run crawler`. Espera 1-2 minutos a que termine (Estado `Ready`, Tables added `2`).
5.  **Verificar Tablas:**
    *   Menú izquierdo: `Tables`. Deberías ver `input_users_csv` y `input_listening_history_csv` (o similar) en la database `spotify_db`.

### Parte 4: Crear y Ejecutar el Job ETL (Glue Job)

Este es el trabajo que procesará los datos usando el script `spotify_wrapped_job.py`.

1.  **Ir a ETL Jobs:** Menú izquierdo: `ETL jobs`.
2.  **Crear Job:**
    *   Asegúrate que `Spark script editor` esté seleccionado.
    *   Opción: `Upload and edit an existing script`.
    *   Cargar script: `Local file` -> `Choose file` -> Selecciona `spotify_wrapped_job.py`.
    *   Clic `Create script` o `Create`.
3.  **Configurar Job Details:**
    *   Pestaña `Job details`.
    *   **Name:** `spotify_aggregation_job`.
    *   **IAM Role:** Selecciona `Role-Glue-Workshop`.
    *   **Glue version:** `Glue 4.0...`.
    *   **Worker type:** `G.1X`.
    *   **Requested number of workers:** `2` (Workers mínimos).
4.  **Configurar Job Parameters:**
    *   Desplázate abajo a `Job parameters`.
    *   Añade estos parámetros (Key y Value):
        *   Key: `--GLUE_DATABASE_NAME` | Value: `spotify_db` (o tu nombre de DB)
        *   Key: `--GLUE_TABLE_NAME_USERS` | Value: `input_users_csv` (o tu nombre exacto de tabla users)
        *   Key: `--GLUE_TABLE_NAME_HISTORY` | Value: `input_listening_history_csv` (o tu nombre exacto de tabla history)
        *   Key: `--S3_OUTPUT_PATH` | Value: `s3://[TU_BUCKET_AQUI]/output/` ( **REEMPLAZA `[TU_BUCKET_AQUI]`** )
5.  **Guardar:** Clic `Save`.
6.  **Ejecutar:** Clic `Run`.
7.  **Monitorear:** Ve a la pestaña `Runs`. Espera a que el estado sea `Succeeded` (puede tardar varios minutos). Si falla, revisa los logs (`Output logs` / `Error logs`).

### Parte 5: Verificar Salida en S3

1.  **Ir a S3:** Vuelve a tu bucket S3.
2.  **Navegar a `output`:** Entra en la carpeta `output/`.
3.  **Verificar Archivos:** Deberías ver uno o más archivos `.parquet`. 

### Parte 6: (OPTIONAL EXTRA STEP) Consultar con Athena

1.  **Ir a Athena:** Busca `Athena` en la consola.
2.  **Configurar Query Result Location (si es necesario):** Sigue las instrucciones si aparece el mensaje, usando una ruta como `s3://[TU_BUCKET_AQUI]/athena_results/`.
3.  **Ejecutar DDL:** En el editor de consultas (asegúrate que la Database sea `spotify_db`), pega y ejecuta esto ( **AJUSTA LA RUTA `LOCATION`** ):
    ```sql
    CREATE EXTERNAL TABLE IF NOT EXISTS user_summary (
      user_id STRING, username STRING, country STRING,
      total_songs BIGINT, total_listening_time_sec BIGINT
    )
    STORED AS PARQUET
    LOCATION 's3://[TU_BUCKET_AQUI]/output/'; -- TU RUTA S3
    ```
4.  **Ejecutar Consulta:** Pega y ejecuta esto para ver datos:
    ```sql
    SELECT * FROM user_summary LIMIT 10;
    ```

---
