---

## Instrucciones 
![Diagrama ETL](diagram.png)

### Parte 1: Preparar S3 (Almacenamiento)

1.  **Ir a S3:** Busca `S3` en la consola de AWS y entra al servicio.
2.  **Crear Bucket:**
    *   Clic en `+ Crear bucket`.
    *   **Nombre del bucket:** Elige un nombre **único global** (ej. `tu-alias-glue-workshop-fecha`). Anota este nombre, lo necesitarás varias veces.
    *   **Región de AWS:** Selecciona tu región de trabajo (ej. `us-east-1`). Asegúrate de usar la misma región para todos los servicios (Glue, S3, IAM si es relevante).
    *   Deja todas las demás opciones por defecto (importante: `Bloquear todo el acceso público` debe estar **marcado**).
    *   Clic en `Crear bucket`.
3.  **Crear Carpetas `input` y `output`:**
    *   Dentro de tu bucket recién creado, haz clic en `Crear carpeta`.
    *   Nombra la primera carpeta `input`. Haz clic en `Crear carpeta`.
    *   Vuelve al nivel raíz del bucket y crea otra carpeta llamada `output`.
4.  **Subir Archivos a `input`:**
    *   Entra en la carpeta `input`.
    *   Haz clic en `Cargar`.
    *   Añade los archivos `users.csv` y `listening_history.csv` desde tu máquina local.
    *   Clic en `Cargar`.

### Parte 2: Crear Rol IAM (Permisos)

Glue necesita permiso para acceder a S3, a sí mismo y a CloudWatch Logs.

1.  **Ir a IAM:** Busca `IAM` en la consola de AWS y entra al servicio.
2.  **Crear Rol:**
    *   Menú izquierdo: `Roles` -> Clic `Crear rol`.
    *   **Tipo de entidad de confianza:** `Servicio de AWS`.
    *   **Caso de uso:** Busca y selecciona `Glue`. Haz clic en `Siguiente`.
    *   **Políticas de permisos:**
        *   La política `AWSGlueServiceRole` ya debería estar seleccionada por defecto (¡verifica!). Esta política base es necesaria.
        *   Ahora, necesitamos añadir permisos específicos para S3 y Logs. Clic en `Crear política` (esto debería abrir una nueva pestaña/ventana).
            *   **En la nueva pestaña (Editor de Políticas):**
                *   Selecciona la pestaña `JSON`.
                *   Borra el contenido existente y pega la siguiente política. **¡IMPORTANTE! Reemplaza `[TU_BUCKET_AQUI]` con el nombre EXACTO de tu bucket S3 creado en la Parte 1.**
                    ```json
                    {
                      "Version": "2012-10-17",
                      "Statement": [
                        {
                          "Effect": "Allow",
                          "Action": [
                            "s3:GetObject",
                            "s3:PutObject", // Necesario para que el script escriba la salida CSV
                            "s3:DeleteObject" // Opcional, pero útil si el job necesita sobrescribir
                          ],
                          "Resource": [
                            "arn:aws:s3:::[TU_BUCKET_AQUI]/input/*",
                            "arn:aws:s3:::[TU_BUCKET_AQUI]/output/*" // Permiso para escribir en la carpeta de salida
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
                                "output/*",
                                "" // Permite listar la raíz del bucket también si es necesario
                              ]
                            }
                          }
                        },
                        {
                          "Effect": "Allow",
                          "Action": [
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents",
                            "logs:AssociateKmsKey" // Añadido por si se usa KMS
                          ],
                          "Resource": [
                            "arn:aws:logs:*:*:log-group:/aws-glue/jobs/*", // Recurso más preciso para logs de jobs
                            "arn:aws:logs:*:*:log-group:/aws-glue/jobs/*:*" // Permite acceso a streams dentro del grupo
                          ]
                        }
                      ]
                    }
                    ```
                *   Clic `Siguiente: Etiquetas` (puedes omitir las etiquetas).
                *   Clic `Siguiente: Revisar`.
                *   **Nombre:** Dale un nombre descriptivo, por ejemplo `Policy-GlueWorkshop-S3-Logs`.
                *   **Descripción (Opcional):** "Permite al rol de Glue leer de input/, escribir en output/ del bucket específico y escribir logs."
                *   Clic `Crear política`. Puedes cerrar esta pestaña.
        *   **De vuelta en la pestaña de creación de rol:**
            *   Haz clic en el botón de **refrescar** (icono circular con flecha) al lado de la barra de búsqueda de políticas.
            *   Busca y **marca la casilla** de la política que acabas de crear (`Policy-GlueWorkshop-S3-Logs`).
            *   Asegúrate de que tanto `AWSGlueServiceRole` como tu política personalizada (`Policy-GlueWorkshop-S3-Logs`) estén marcadas.
            *   Clic `Siguiente`.
    *   **Nombre del rol:** Dale un nombre descriptivo, por ejemplo `Role-Glue-Workshop`.
    *   **Descripción (Opcional):** "Rol para el workshop de Glue ETL con acceso a S3 y Logs."
    *   Revisa los detalles y haz clic en `Crear rol`.

### Parte 3: Catalogar Datos (Glue Crawler)

El Crawler detecta la estructura (esquema) de los datos en S3 y los registra en el Catálogo de Datos de Glue.

1.  **Ir a AWS Glue:** Busca `Glue` en la consola. Asegúrate de estar en la misma región que tu bucket S3.
2.  **Crear Base de Datos:**
    *   Menú izquierdo: `Data Catalog` -> `Databases` -> `Add database`.
    *   **Database name:** `spotify_db` (Este nombre debe coincidir con el valor de la variable `glue_database` en tu script Python).
    *   Clic `Create database`.
3.  **Crear Crawler:**
    *   Menú izquierdo: `Data Catalog` -> `Crawlers` -> `Create crawler`.
    *   **Nombre del Crawler:** `spotify_crawler`. Clic `Next`.
    *   **Data sources:** Clic `Add a data source`.
        *   Data source: `S3`.
        *   Location of S3 data: `In this account`.
        *   S3 path: Clic `Browse S3` -> Navega y selecciona la carpeta `input` dentro de tu bucket -> Clic `Choose`.
        *   Subsequent crawler runs: Deja `Crawl all sub-folders`.
        *   Clic `Add an S3 data source`. Clic `Next`.
    *   **IAM Role:** Selecciona `Choose an existing IAM role`. Busca y selecciona `Role-Glue-Workshop` (el rol que creaste en la Parte 2). Clic `Next`.
    *   **Output configuration:**
        *   Target database: Selecciona `spotify_db` del desplegable.
        *   *Opcional: Añadir prefijo a tablas*: Puedes dejarlo vacío.
        *   *Opcional: Configuration options*: Aquí es donde podrías configurar cómo se manejan los esquemas, pero para este caso, los defaults suelen funcionar.
        *   Clic `Next`.
    *   **Review y Create:** Revisa la configuración y clic `Create crawler`.
4.  **Ejecutar Crawler:**
    *   Busca `spotify_crawler` en la lista, selecciónalo marcando la casilla.
    *   Clic en el botón `Run crawler`.
    *   Espera unos minutos (1-3 min típicamente). El estado cambiará de `Starting` -> `Running` -> `Stopping` -> `Ready`. Busca en la columna `Tables added`. Debería indicar `2`.
5.  **Verificar Tablas:**
    *   Menú izquierdo: `Data Catalog` -> `Tables`.
    *   Asegúrate que la base de datos `spotify_db` esté seleccionada.
    *   Deberías ver dos tablas. Los nombres probablemente serán `users_csv` y `listening_history_csv` (basados en los nombres de archivo dentro de la carpeta `input`). **¡IMPORTANTE! Anota los nombres EXACTOS de estas tablas.**
    *   **CRÍTICO:** Abre tu script Python (`spotify_wrapped_job.py`) y asegúrate de que los valores de las variables `users_table` y `history_table` coincidan **exactamente** con los nombres de tabla creados por el crawler. Si no coinciden, edita el script Python y guarda los cambios antes de continuar.
    *   *Nota sobre Delimitadores:* Este crawler asume archivos CSV delimitados por comas. Si usaste un delimitador diferente (ej. `|`), necesitarías editar el crawler (después de crearlo), ir a su configuración de Data Source, y especificar el delimitador correcto *antes* de ejecutarlo.

### Parte 4: Crear y Ejecutar el Job ETL (Glue Job)

Este es el script que transforma los datos y escribe el resultado.

1.  **Ir a ETL Jobs:** Menú izquierdo: `Data integration and ETL` -> `ETL jobs`.
2.  **Crear Job:**
    *   Asegúrate que la opción `Spark script editor` esté seleccionada.
    *   Bajo `Create job`, selecciona la opción `Upload and edit an existing script`.
    *   **Upload script file:** Clic en `Choose file` -> Selecciona tu script Python modificado (`spotify_wrapped_job.py`, el que escribe un CSV único con timestamp y tiene los parámetros hardcodeados).
    *   Clic en `Create script`.
3.  **Configurar Job:**
    *   Una vez en el editor, ve a la pestaña `Job details`.
    *   **Name:** `spotify_aggregation_job`.
    *   **IAM Role:** Selecciona `Role-Glue-Workshop` del desplegable.
    *   **Glue version:** Selecciona una versión reciente, por ejemplo `Glue 4.0 - Python 3, Spark 3.3, Scala 2.12`.
    *   **Worker type:** `G.1X` es un buen punto de partida para datos pequeños/medianos.
    *   **Requested number of workers:** `2` o `3` suele ser suficiente para este workshop. Un número mayor procesa más rápido pero cuesta más.
    *   **(Importante) Verifica el Script:** Ve a la pestaña `Script`. Asegúrate de que los valores de las variables `s3_output_base_path`, `glue_database`, `users_table`, y `history_table` estén correctamente configurados con **tus** valores (tu bucket S3, tu base de datos, y los nombres exactos de las tablas del crawler).
    *   **Job parameters:** **NO** necesitas configurar parámetros de Job aquí, ya que los valores están definidos *dentro* del script.
4.  **Guardar:** Clic en el botón `Save` en la esquina superior derecha.
5.  **Ejecutar:** Clic en el botón `Run`.
6.  **Monitorear:**
    *   Una vez iniciado, aparecerá una barra en la parte inferior o puedes ir a la pestaña `Runs`.
    *   El estado pasará por `Running` -> `Stopping` -> `Succeeded`. Esto puede tardar varios minutos (5-15 min dependiendo de los workers y el arranque inicial).
    *   Si el estado es `Failed`, haz clic en la ejecución fallida y luego en `Output logs` o `Error logs` (pueden estar en CloudWatch) para diagnosticar el problema. Revisa los mensajes de error en tu script y los permisos del rol IAM.

### Parte 5: Verificar Salida en S3

1.  **Ir a S3:** Vuelve a tu bucket S3.
2.  **Navegar a `output`:** Entra en la carpeta `output/`.
3.  **Verificar Archivo:** Deberías ver un **único archivo CSV** con un nombre similar a `glue_output_YYYYMMDD_HHMMSS.csv` (donde YYYYMMDD_HHMMSS es la fecha y hora de ejecución). Puedes descargarlo y abrirlo con un editor de texto o una hoja de cálculo para verificar el contenido.

### Parte 6: (OPTIONAL EXTRA STEP) Consultar con Athena

Athena permite ejecutar consultas SQL directamente sobre los archivos en S3.

1.  **Ir a Athena:** Busca `Athena` en la consola AWS.
2.  **Configurar Ubicación de Resultados (si es la primera vez):**
    *   Si ves un mensaje solicitando configurar una ubicación para resultados, haz clic en `Settings` (Configuración) o sigue el enlace.
    *   Ingresa una ruta S3 dentro de tu bucket donde Athena pueda guardar los resultados de las consultas, por ejemplo: `s3://[TU_BUCKET_AQUI]/athena_results/` (reemplaza `[TU_BUCKET_AQUI]`). Athena creará esta carpeta si no existe.
    *   Guarda la configuración.
3.  **Seleccionar Base de Datos:** En el panel izquierdo, bajo `Database`, asegúrate de seleccionar `spotify_db`.
4.  **Crear Tabla Externa:** En el editor de consultas, pega y ejecuta el siguiente comando DDL (Data Definition Language). **¡IMPORTANTE! Reemplaza `[TU_BUCKET_AQUI]` con el nombre de tu bucket.**
    ```sql
    CREATE EXTERNAL TABLE IF NOT EXISTS user_summary (
      user_id STRING,
      username STRING,
      country STRING,
      total_songs BIGINT,
      total_listening_time_sec BIGINT
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' -- Especifica que es CSV
    WITH SERDEPROPERTIES (
      'separatorChar' = ',', -- Indica el separador
      'skip.header.line.count' = '1' -- Le dice a Athena que ignore la fila de encabezado
    )
    STORED AS TEXTFILE -- Indica que el almacenamiento subyacente es texto (CSV es texto)
    LOCATION 's3://[TU_BUCKET_AQUI]/output/'; -- Apunta a la CARPETA donde está tu archivo CSV
    ```
5.  **Ejecutar Consulta de Prueba:** Una vez creada la tabla (deberías ver `user_summary` en la lista de tablas a la izquierda), ejecuta una consulta para ver los datos:
    ```sql
    SELECT * FROM user_summary LIMIT 10;
    ```
    O una consulta más interesante:
    ```sql
    SELECT username, country, total_songs, total_listening_time_sec
    FROM user_summary
    ORDER BY total_listening_time_sec DESC
    LIMIT 10;
    ```
