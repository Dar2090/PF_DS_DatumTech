# Databricks notebook source
# MAGIC %md ### Importamos las librerias necesarias para realizar nuestro ETL:

# COMMAND ----------

import re
from pyspark.sql.functions import expr, lower, col

# COMMAND ----------

# MAGIC %md ### Realizamos la conexion a ADLS para poder acceder a los archivos reviews-estados:

# COMMAND ----------

spark.conf.set("fs.azure.account.key.datumtechstorage.dfs.core.windows.net","2IEO7wL5cOzrt/r4jBQ8WnSRCq5LHWA3ezQ33eZYVsp1W9PI+53LPQ6bz56KFnEJKwsJEPZFtDPS+AStqxFgeA==")

# COMMAND ----------

# MAGIC %md ### Creacion de tabla `processed_files_business` para mantener el registro de los archivos ya procesados.

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS processed_files_business (file_name STRING) USING DELTA")

# COMMAND ----------

# MAGIC %md ### Obtenemos la lista de archivos en ADLS.

# COMMAND ----------

adls_files = dbutils.fs.ls("abfss://datumcontainer@datumtechstorage.dfs.core.windows.net/Yelp/business")
display(adls_files)

# COMMAND ----------

# MAGIC %md ### Creamos un variable `new_files` que contiene los archivos que no estan en ADLS.

# COMMAND ----------

processed_files = spark.sql("SELECT file_name FROM processed_files_business").toPandas()["file_name"].tolist()

new_files = [file for file in adls_files if file.name not in processed_files]

# COMMAND ----------

# MAGIC %md ### Podemos ver que archivos ya estan procesados en la tabla `processed_files_business`

# COMMAND ----------

spark.sql("SELECT * FROM processed_files_business").show()

# COMMAND ----------

# MAGIC %md ### Podemos ver que archivos no estan procesados aun.

# COMMAND ----------

print(new_files)

# COMMAND ----------

display(new_files)

# COMMAND ----------

new_files[0][1].rstrip("/")

# COMMAND ----------

# MAGIC %md ### Creamos la funcion `etl` que realiza todo el proceso y devuelve el dataframe en formato parquet a la tabla silver corrrespondiente a los datos ya procesados. Tambien definimos alguna variable necesaria.

# COMMAND ----------

# Variable con categorias a filtrar
category = [
    'burger', 'burgers', 'hamburger', 'hamburgers' 'hot dog', 'steakhouse', 'lunch', 'motel', 'patisserie', 'pizza', 'deli', 'diner', 'dinner', 'icecream', 'ice cream', 'hotel', 'hotels', 'seafood','cookie', 'crab house', 'cupcake', 'chocolate', 'churreria', 'cocktail', 'cocktails', 'coffee', 'coffees' 'tea', 'restaurant', 'restaurats', 'chesse', 'charcuterie', 'cafe', 'cafes', 'BBQ', 'bagle', 'bakery' 'bakerys', 'bar', 'bars', 'bar & grill', 'barbacue', 'beer' 'bistro', 'pasteleria', 'pastelerias', 'breakfast', 'brunch', 'buffet', 'burrito', 'cafeteria', 'cafeterias', 'cake', 'cakes', 'food']

# COMMAND ----------


def etl(file):
    
    # Definimos las rutas:
    path_raw=f"abfss://datumcontainer@datumtechstorage.dfs.core.windows.net/Yelp/business/{file}"
    path_bronze = f"abfss://datumcontainer@datumtechstorage.dfs.core.windows.net/Bronze/YelpBronze/business-Bronze/{file}-bronze.parquet"
    path_silver = f"abfss://datumcontainer@datumtechstorage.dfs.core.windows.net/Silver/YelpSilver/business-Silver/{file}-silver.parquet"

    # Cargamos el archivo desde ADLS. Nos quedamos solo con las columnas consideradas para el proyecto:
    df_raw = spark.read.format("parquet").load(path_raw).select('business_id', 'name', 'address', 'city', 'state', 'postal_code', 'latitude', 'longitude', 'stars', 'review_count','attributes', 'categories')
     
    # Guardamos el DataFrame df_raw en la tabla bronze correspondiente a los datos en crudo o poco procesados en Azure Data Lake con un formato parquet ideal para manejar altos volumenes de datos.
    df_raw.write.format("parquet").save(path_bronze)
        
    # Cargamos el archivo desde la tabla bronze en Azure Data Lake en un DataFrame.
    df_business = spark.read.format("parquet").load(path_bronze)

    # Borramos duplicados.
    df_business = df_business.dropDuplicates()

    # Desanidamos la columna 'attributes', esta informacion nos servira mas adelante en la devolucion de recomandacion de nuestro modelo de ML.



    # Rellenamos valores vacíos o nulos en las columnas 'name', 'address', 'city', 'attributes', 'latitude', 'longitude', 'postal_code', 'stars', 'review_count'. A pesar de no tener nulos en algunas, dejamos planteado el codigo para usar el notebook en jobs posteriores.
    # Eliminamos los registros donde 'business_id', 'categories' y 'state' son nulos o vacios. Representan menos del 10% de los datos totales. Ademas 'business_id' sera una PK mas adlenate.
    df_business = df_business.fillna('Unknown', subset=['name', 'address', 'city', 'attributes'])
    df_business = df_business.fillna(0, subset=['latitude', 'longitude', 'postal_code', 'stars', 'review_count'])
    df_business = df_business.na.drop(subset=['business_id', 'categories', 'state'])

    # Definimos un filtro para la columna 'categories' asi solo nos quedamos con las categorias necesarias para el proyecto.
    filtro = expr("lower(categories)").rlike("|".join(category))
    df_business = df_business.filter(filtro)

    # Guardamos el DataFrame df_metadata en la tabla silver correspondiente a los datos procesados en Azure Data Lake.
    return df_business.write.format("parquet").save(path_silver)
    

# COMMAND ----------

# MAGIC %md ### Iteramos sobre cada archivo sin procesar.

# COMMAND ----------

for file in new_files:
    etl(file.name.rstrip("/"))

# COMMAND ----------

# MAGIC %md ### Agregamos a la tabla `processed_files_business` los archivos ya procesados.

# COMMAND ----------

new_files_df = spark.createDataFrame([(file.name,) for file in new_files], ["file_name"])
new_files_df.write.format("delta").mode("append").saveAsTable("processed_files_business")

# COMMAND ----------

# MAGIC %md ### Podemos verificar que, efectivamente esten registrados los archivos ya procesados.

# COMMAND ----------

spark.sql("SELECT * FROM processed_files_business").show()

# COMMAND ----------

# MAGIC %md #### Para comprobar que todo se haya ejecutado de manera correcta, podemos traer cualquier archivo de la tabla silver y hacer algunas verificaciones.

# COMMAND ----------

# Funcion para cargar el archivo desde la tabla correspondiente al estado de los datos en Azure Data Lake en un DataFrame.
def load_from(file, level):
    path= f"abfss://datumcontainer@datumtechstorage.dfs.core.windows.net/{level}/Yelp{level}/business-{level}/{file}-{level.lower()}.parquet"
    df = spark.read.format("parquet").load(path)
    return df

# COMMAND ----------

# Cambiando ...new_files[<valor de la fila en la tabla>][1]... podras cargar algun archivo de los ya procesados de la tabla silver.
df = load_from(new_files[0][1].rstrip("/"), "Silver")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md ### Podemos verificar si hay nulos en algun archivo en la tabla silver.

# COMMAND ----------

# Funcion para el conteo de nulos del dataframe.
def null_counts (df):
    counts = df.select(*[sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
    return counts.show()

# COMMAND ----------

# Vemos que columnas poseen nulos y en que cantidad.
nulls = null_counts(df)