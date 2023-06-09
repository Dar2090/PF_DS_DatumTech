# Databricks notebook source
# MAGIC %md ### Importamos las librerias necesarias para realizar nuestro ETL:

# COMMAND ----------

from pyspark.sql.functions import concat_ws, col, sum

# COMMAND ----------

# MAGIC %md ### Realizamos la conexion a ADLS para poder acceder a los archivos reviews-estados:

# COMMAND ----------

spark.conf.set("fs.azure.account.key.datumtechstorage.dfs.core.windows.net","2IEO7wL5cOzrt/r4jBQ8WnSRCq5LHWA3ezQ33eZYVsp1W9PI+53LPQ6bz56KFnEJKwsJEPZFtDPS+AStqxFgeA==")

# COMMAND ----------

# MAGIC %md ### Creacion de tabla `processed_files` para mantener el registro de los archivos ya procesados.

# COMMAND ----------

#spark.sql("CREATE TABLE IF NOT EXISTS processed_files (file_name STRING) USING DELTA")

# COMMAND ----------

# MAGIC %md ### Obtenemos la lista de archivos en ADLS.

# COMMAND ----------

adls_files = dbutils.fs.ls("abfss://datumcontainer@datumtechstorage.dfs.core.windows.net/GoogleMaps/reviews-estados")
#display(adls_files)

# COMMAND ----------

# MAGIC %md ### Creamos un variable `new_files` que contiene los archivos que no estan en ADLS.

# COMMAND ----------

processed_files = spark.sql("SELECT file_name FROM processed_files").toPandas()["file_name"].tolist()

new_files = [file for file in adls_files if file.name not in processed_files]

# COMMAND ----------

# MAGIC %md ### Podemos ver que archivos ya estan procesados en la tabla `processed_files`

# COMMAND ----------

#spark.sql("SELECT * FROM processed_files").show()

# COMMAND ----------

# MAGIC %md ### Podemos ver que archivos no estan procesados aun.

# COMMAND ----------

print(new_files)

# COMMAND ----------

display(new_files)

# COMMAND ----------

#new_files[1][1].rstrip("/")

# COMMAND ----------

# MAGIC %md ### Creamos la funcion `etl` que realiza todo el proceso y devuelve el dataframe en formato parquet a la tabla silver corrrespondiente a los datos ya procesados.

# COMMAND ----------


def etl(file):
    
    # Definimos las rutas:
    path_raw=f"abfss://datumcontainer@datumtechstorage.dfs.core.windows.net/GoogleMaps/reviews-estados/{file}"
    path_bronze = f"abfss://datumcontainer@datumtechstorage.dfs.core.windows.net/Bronze/GoogleMapsBronze/reviews-estados-Bronze/{file}-bronze.parquet"
    path_silver = f"abfss://datumcontainer@datumtechstorage.dfs.core.windows.net/Silver/GoogleMapsSilver/reviews-estados-Silver/{file}-silver.parquet"

    # Cargamos el archivo desde ADLS. Nos quedamos solo con las columnas consideradas para el proyecto:
    df_raw = spark.read.format("json").option("multiline", True).load(path_raw).select('gmap_id', 'rating', 'text', 'user_id')
     
    # Guardamos el DataFrame df_raw en la tabla bronze correspondiente a los datos en crudo o poco procesados en Azure Data Lake con un formato parquet ideal para manejar altos volumenes de datos.
    df_raw.write.format("parquet").save(path_bronze)
        
    # Cargamos el archivo desde la tabla bronze en Azure Data Lake en un DataFrame.
    df_review = spark.read.format("parquet").load(path_bronze)

    # Eliminar los duplicados
    df_review = df_review.dropDuplicates()

    # Rellenamos valores vacíos o nulos en las columnas 'text', 'user_id' y 'rating'. A pesar de no tener nulos en algunas, dejamos planteado el codigo para usar el notebook en jobs posteriores.
    # Eliminamos los registros donde 'gmap_id' son nulos o vacios. Representan menos del 10% de los datos totales y es una columna que sera usada de PK mas adelante.
    df_review = df_review.fillna('Unknown', subset=['text'])
    df_review = df_review.fillna(0, subset=['user_id', 'rating'])
    df_review = df_review.na.drop(subset=['gmap_id'])

    # Guardamos el DataFrame df_review en la tabla silver correspondiente a los datos procesados en Azure Data Lake.
    return df_review.write.format("parquet").save(path_silver)
    

# COMMAND ----------

# MAGIC %md ### Iteramos sobre cada archivo sin procesar.

# COMMAND ----------

for file in new_files:
    etl(file.name.rstrip("/"))

# COMMAND ----------

# MAGIC %md ### Agregamos a la tabla `processed_files` los archivos ya procesados.

# COMMAND ----------

new_files_df = spark.createDataFrame([(file.name,) for file in new_files], ["file_name"])
new_files_df.write.format("delta").mode("append").saveAsTable("processed_files")

# COMMAND ----------

# MAGIC %md ### Podemos verificar que, efectivamente esten registrados los archivos ya procesados.

# COMMAND ----------

spark.sql("SELECT * FROM processed_files").show()

# COMMAND ----------

# MAGIC %md #### Para comprobar que todo se haya ejecutado de manera correcta, podemos traer cualquier archivo de la tabla silver y hacer algunas verificaciones.

# COMMAND ----------

# Funcion para cargar el archivo desde la tabla correspondiente al estado de los datos en Azure Data Lake en un DataFrame.
'''def load_from(file, level):
    path= f"abfss://datumcontainer@datumtechstorage.dfs.core.windows.net/{level}/GoogleMaps{level}/reviews-estados-{level}/{file}-{level.lower()}.parquet"
    df = spark.read.format("parquet").load(path)
    return df'''

# COMMAND ----------

# Cambiando ...new_files[<valor de la fila en la tabla>][1]... podras cargar algun archivo de los ya procesados de la tabla silver.
#df = load_from(new_files[0][1].rstrip("/"), "Silver")

# COMMAND ----------

#display(df)

# COMMAND ----------

# MAGIC %md ### Podemos verificar si hay nulos en algun archivo en la tabla silver.

# COMMAND ----------

# Funcion para el conteo de nulos del dataframe.
'''def null_counts (df):
    counts = df.select([sum(col(c).isNull().cast("integer")).alias(c) for c in df.columns])
    return counts.show()'''

# COMMAND ----------

# Vemos que columnas poseen nulos y en que cantidad.
#nulls = null_counts(df)