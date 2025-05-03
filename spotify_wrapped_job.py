import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                     'S3_INPUT_PATH_USERS',
                                     'S3_INPUT_PATH_HISTORY',
                                     'S3_OUTPUT_PATH',
                                     'GLUE_DATABASE_NAME',
                                     'GLUE_TABLE_NAME_USERS',
                                     'GLUE_TABLE_NAME_HISTORY'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ---- Parámetros ----
# Estos valores se pasarán como argumentos del Job,
# pero puedes definirlos aquí para pruebas locales si es necesario.
s3_input_path_users = args['S3_INPUT_PATH_USERS'] # Ejemplo: "s3://<TU_BUCKET_AQUI>/input/users.csv"
s3_input_path_history = args['S3_INPUT_PATH_HISTORY'] # Ejemplo: "s3://<TU_BUCKET_AQUI>/input/listening_history.csv"
s3_output_path = args['S3_OUTPUT_PATH'] # Ejemplo: "s3://<TU_BUCKET_AQUI>/output/"
glue_database = args['GLUE_DATABASE_NAME'] # Ejemplo: "spotify_workshop_db"
users_table = args['GLUE_TABLE_NAME_USERS'] # Ejemplo: "users_csv"
history_table = args['GLUE_TABLE_NAME_HISTORY'] # Ejemplo: "listening_history_csv"

# ---- Extracción (Extract) ----
# Lee los datos desde el Glue Data Catalog (asumiendo que el Crawler ya se ejecutó)

# Leer tabla de usuarios
users_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=glue_database,
    table_name=users_table,
    transformation_ctx="users_dyf"
)

# Leer tabla de historial de escucha
history_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=glue_database,
    table_name=history_table,
    transformation_ctx="history_dyf"
)

print(f"Registros leídos de {users_table}: {users_dyf.count()}")
print(f"Registros leídos de {history_table}: {history_dyf.count()}")

# ---- Transformación (Transform) ----

# 1. Join de los DynamicFrames por 'user_id'
joined_dyf = Join.apply(
    frame1=users_dyf,
    frame2=history_dyf,
    keys1=['user_id'],
    keys2=['user_id'],
    transformation_ctx="joined_dyf"
)

print(f"Registros después del Join: {joined_dyf.count()}")
# joined_dyf.printSchema() # Descomenta para ver el esquema después del join

# 2. Convertir a DataFrame de Spark para usar funciones de agregación SQL
joined_df = joined_dyf.toDF()

# 3. Realizar agregaciones
# Agrupar por usuario, contar canciones y sumar duración
aggregated_df = joined_df.groupBy("user_id", "username", "country").agg(
    F.count("song_title").alias("total_songs"),
    F.sum("duration_sec").alias("total_listening_time_sec")
)

print(f"Registros después de la agregación: {aggregated_df.count()}")
# aggregated_df.printSchema() # Descomenta para ver el esquema final
# aggregated_df.show(5)      # Descomenta para ver una muestra del resultado

# 4. Convertir de nuevo a DynamicFrame para escribir con Glue
output_dyf = DynamicFrame.fromDF(aggregated_df, glueContext, "output_dyf")

# ---- Carga (Load) ----
# Escribe el resultado en formato Parquet en S3
glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    connection_options={"path": s3_output_path},
    format="parquet",
    transformation_ctx="datasink"
)

print(f"Datos agregados escritos exitosamente en: {s3_output_path}")

job.commit()