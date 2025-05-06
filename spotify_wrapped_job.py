import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F
import boto3
from datetime import datetime
import io
from urllib.parse import urlparse 

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], {}) # Pasamos un diccionario vacío para los argumentos restantes

# ---- Parámetros Definidos Directamente en el Script ----
s3_output_base_path = "s3://<TU_BUCKET_AQUI>/<PREFIJO_SALIDA>/" # Ejemplo: "s3://mi-bucket-datos/salida_agregada/"
glue_database = "nombre_de_tu_basedatos_glue" # Ejemplo: "spotify_db"
users_table = "nombre_tabla_glue_usuarios" # Ejemplo: "users_csv"
history_table = "nombre_tabla_glue_historial" # Ejemplo: "listening_history_csv"

# --- Verificación Rápida  ---
print(f"Usando Base de Datos Glue: {glue_database}")
print(f"Usando Tabla de Usuarios: {users_table}")
print(f"Usando Tabla de Historial: {history_table}")
print(f"Ruta Base de Salida S3: {s3_output_base_path}") # Cambiado nombre de variable para claridad

# ---- Extracción (Extract) ----
try:
    users_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=glue_database,
        table_name=users_table,
        transformation_ctx="users_dyf"
    )
    history_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=glue_database,
        table_name=history_table,
        transformation_ctx="history_dyf"
    )
except Exception as e:
    print(f"Error al leer del Glue Data Catalog. Asegúrate que la base de datos '{glue_database}' y las tablas '{users_table}', '{history_table}' existen y el rol del Job tiene permisos.")
    print(f"Error detallado: {e}")
    sys.exit(1)

print(f"Registros leídos de {users_table}: {users_dyf.count()}")
print(f"Registros leídos de {history_table}: {history_dyf.count()}")

# ---- Transformación (Transform) ----
if users_dyf.count() == 0 or history_dyf.count() == 0:
    print("Una o ambas tablas de entrada están vacías. No se puede realizar el Join.")
    sys.exit(1)

user_cols = [field.name for field in users_dyf.schema()]
history_cols = [field.name for field in history_dyf.schema()]

if 'user_id' not in user_cols:
    print(f"Error: La columna 'user_id' no se encontró en la tabla '{users_table}'. Columnas encontradas: {user_cols}")
    sys.exit(1)
if 'user_id' not in history_cols:
    print(f"Error: La columna 'user_id' no se encontró en la tabla '{history_table}'. Columnas encontradas: {history_cols}")
    sys.exit(1)

joined_dyf = Join.apply(
    frame1=users_dyf,
    frame2=history_dyf,
    keys1=['user_id'],
    keys2=['user_id'],
    transformation_ctx="joined_dyf"
)

print(f"Registros después del Join: {joined_dyf.count()}")

joined_cols = [field.name for field in joined_dyf.schema()]
required_agg_cols = ['user_id', 'username', 'country', 'song_title', 'duration_sec']
missing_cols = [col for col in required_agg_cols if col not in joined_cols]

if missing_cols:
    print(f"Error: Faltan columnas necesarias para la agregación después del Join: {missing_cols}")
    print(f"Columnas disponibles después del Join: {joined_cols}")
    sys.exit(1)

joined_df = joined_dyf.toDF()

aggregated_df = joined_df.groupBy("user_id", "username", "country").agg(
    F.count("song_title").alias("total_songs"),
    F.sum("duration_sec").alias("total_listening_time_sec")
)

print(f"Registros después de la agregación: {aggregated_df.count()}")

# ---- INICIO: Preparación para escritura de archivo único ----

# 1. Asegurar una sola partición (necesario antes de toPandas si los datos son > default parallelism)
print("Aplicando coalesce(1)...")
aggregated_df_single_partition = aggregated_df.coalesce(1)

# 2. Generar nombre de archivo con timestamp
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
output_filename = f"glue_output_{timestamp_str}.csv"

# 3. Parsear el bucket y el prefijo (key) de la ruta S3 base
s3_parsed = urlparse(s3_output_base_path, allow_fragments=False)
s3_bucket = s3_parsed.netloc
# Asegurar que el prefijo termine en / si no lo hace, y luego añadir nombre de archivo
s3_prefix = s3_parsed.path.lstrip('/')
if s3_prefix and not s3_prefix.endswith('/'):
    s3_prefix += '/'
s3_key = s3_prefix + output_filename

print(f"Se escribirá en Bucket: {s3_bucket}, Key: {s3_key}")

# ---- Carga (Load) - Usando Pandas y Boto3 ----
try:
    # 4. Convertir a Pandas DataFrame 
    print("Convirtiendo resultado a DataFrame de Pandas (puede consumir mucha memoria)...")
    pandas_df = aggregated_df_single_partition.toPandas()
    print(f"Conversión a Pandas completada. {len(pandas_df)} filas.")

    if not pandas_df.empty:
        # 5. Crear buffer de CSV en memoria
        csv_buffer = io.StringIO()
        pandas_df.to_csv(csv_buffer, index=False, header=True, sep=',') # Escribir CSV al buffer

        # 6. Subir a S3 usando Boto3
        print(f"Subiendo archivo a s3://{s3_bucket}/{s3_key}...")
        s3_client = boto3.client('s3')
        s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer.getvalue())
        print("Subida completada exitosamente.")
    else:
        print("El DataFrame agregado estaba vacío. No se escribió ningún archivo.")

except MemoryError:
    print("¡ERROR DE MEMORIA! El DataFrame resultante es demasiado grande para caber en la memoria del driver.")
    print("Considera no usar .toPandas() y escribir en formato Parquet (directorio) o usar otras técnicas para archivos grandes.")
    sys.exit(1)
except Exception as e:
    print(f"Error durante la conversión a Pandas o la escritura a S3.")
    print(f"Error detallado: {e}")
    sys.exit(1)

# ---- FIN: Carga ----

job.commit()
