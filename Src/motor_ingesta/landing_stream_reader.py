from loguru import logger
import os
import io
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, replace, lit, col, pandas_udf, regexp_extract
from PIL import Image
from databricks.sdk.runtime import dbutils, spark

class LandingStreamReader:
    """
    Clase que utilizará una sesión de Spark para leer ficheros en la capa de Landing de un Data Lakehouse, utilizando
    Autolader y preparando los datos que posteriormente sean movidos a la capa bronce en formato delta table.
    """
    def __init__(self, builder):
        """
        Constructor de la clase.

        :param builder: Clase builder, dentro de esta clase.
        """
        # self.spark = builder.spark
        self.datasource = builder.datasource
        self.dataset = builder.dataset
        self.landing_path = builder.landing_path
        self.raw_path = builder.raw_path
        self.bronze_path = builder.bronze_path
        self.format = builder.format
        self.is_image = builder.is_image
        self.config = builder.config
        self.dataset_landing_path = f'{self.landing_path}/{self.datasource}/{self.dataset}'
        self.dataset_bronze_schema_location = f'{self.bronze_path}/{self.datasource}/{self.dataset}_schema'
        if self.config["EXECUTION_ENVIRONMENT"] == "databricks":
            dbutils.fs.mkdirs(self.dataset_bronze_schema_location)
        elif self.config["EXECUTION_ENVIRONMENT"] == "local":
            os.makedirs(self.dataset_bronze_schema_location, exist_ok=True)
        else:
            logger.error("Por favor elija un entorno de ejecución válido.")

    def __str__(self):
        return (f"LandingStreamReader(datasource='{self.datasource}',dataset='{self.dataset}')")

    def add_metadata_columns(self, df):
        """
        Método de la clase, que agregará columnas de metadatos a un DataFrame de spark. Las columnas a agregar
        variarán según si el fichero que se intentará luego leer, es o no una imagen.

        :param df: dataframe de Spark
        :return: Devuelve un DataFrame de spark, con las columnas de metadatos agregadas por delante.
        """

        def extract_label(path_col):
            """Función que extrae una etiqueta de un nombre de un fichero, utilizando regex"""
            return regexp_extract(path_col, "flower_photos/([^/]+)", 1)

        def extract_size(content):
            """Función que extrae las medidas de una imagen, desde su contenido"""
            image = Image.open(io.BytesIO(content))
            return image.size

        @pandas_udf("width: int, height: int")
        def extract_size_udf(content_series):
            """Función UDF que aplica como mapeo la función extract_size, para poder tener el alto y largo en enteros"""
            sizes = content_series.apply(extract_size)
            return pd.DataFrame(list(sizes))

        logger.info("Agregando columnas de metadatos.")
        data_cols = df.columns

        try:
            if self.is_image == True:
                metadata_cols = ['_ingested_at', '_ingested_filename', '_label', '_size']
                df = (df
                      .withColumn("_size", extract_size_udf(col("content")))
                      .withColumn("_label", extract_label(col("path")))
                      .withColumn("_ingested_at", current_timestamp())
                      .withColumn("_ingested_filename",
                                  replace(input_file_name(), lit(self.landing_path), lit(self.raw_path)))
                      )
            else:
                metadata_cols = ['_ingested_at', '_ingested_filename']
                df = (df.withColumn("_ingested_at", current_timestamp())
                      .withColumn("_ingested_filename",
                                  replace(input_file_name(), lit(self.landing_path), lit(self.raw_path)))
                      )
        except Exception as e:
            logger.error(f"Algo ha ido mal intentando agregar las columnas de metadatos: {e}")
        else:
            logger.success("Columnas de metadatos agregadas con éxito.")

        # reordernamos columnas
        return df.select(metadata_cols + data_cols)

    def read_json(self):
        """
        Método de la clase, que creará un stream de lectura de Spark, utilizando autoloader para leer ficheros json, con esquema evolutivo.

        :return: El objeto de readStream de Spark.
        """
        # spark = self.spark
        print(self.dataset_landing_path)
        return (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.allowOverwrites", "true")
                .option("cloudFiles.schemaLocation", self.dataset_bronze_schema_location)
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .load(self.dataset_landing_path)
                )

    def read_csv(self):
        """
        Método de la clase, que creará un stream de lectura de Spark, utilizando autoloader para leer ficheros csv, con esquema evolutivo.
        El delimitador utilizado será ";"

        :return: El objeto de readStream de Spark.
        """
        # spark = self.spark
        print(self.dataset_landing_path)
        return (spark.readStream
                # .option("header","true")
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("delimiter", ";")
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.allowOverwrites", "true")
                .option("cloudFiles.schemaLocation", self.dataset_bronze_schema_location)
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .load(self.dataset_landing_path)
                )

    def read_avro(self):
        """
        Método de la clase, que creará un stream de lectura de Spark, utilizando autoloader para leer ficheros avro, con esquema evolutivo.

        :return: El objeto de readStream de Spark.
        """
        # spark = self.spark
        print(self.dataset_landing_path)
        return (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "avro")
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.allowOverwrites", "true")
                .option("cloudFiles.schemaLocation", self.dataset_bronze_schema_location)
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .load(self.dataset_landing_path)
                )

    def read_parquet(self):
        """
        Método de la clase, que creará un stream de lectura de Spark, utilizando autoloader para leer ficheros parquet, con esquema evolutivo.

        :return: El objeto de readStream de Spark.
        """
        print(self.dataset_landing_path)
        return (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.allowOverwrites", "true")
                .option("cloudFiles.schemaLocation", self.dataset_bronze_schema_location)
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .load(self.dataset_landing_path)
                )

    def read_jpg(self):
        """
        Método de la clase, que creará un stream de lectura de Spark, utilizando autoloader para leer ficheros jpg, con esquema evolutivo.

        :return: El objeto de readStream de Spark.
        """
        print(self.dataset_landing_path)
        return (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "binaryFile")
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.allowOverwrites", "true")
                .option("pathGlobFilter", "*.jpg")
                .option("cloudFiles.schemaLocation", self.dataset_bronze_schema_location)
                .load(self.dataset_landing_path)
                )

    def read(self):
        """
        Método de la clase, que según el atributo de formato definido por el builder al instanciar la clase, define
        el formato del fichero que luego se intentará leer. Luego llama al método add_metadata_columns para agregar
        las columnas de metadatos, que variarán según si el fichero es o no imágen.

        :return: El objeto de readStream de Spark.
        """
        df = None

        try:
            if (self.format == "json"):
                logger.info("Intentando leer ficheros .json en Landing.")
                df = self.read_json()
            elif (self.format == "csv"):
                logger.info("Intentando leer ficheros .csv en Landing.")
                df = self.read_csv()
            elif (self.format == "avro"):
                logger.info("Intentando leer ficheros .avro en Landing.")
                df = self.read_avro()
            elif (self.format == "parquet"):
                logger.info("Intentando leer ficheros .parquet en Landing.")
                df = self.read_parquet()
            elif (self.format == "jpg"):
                logger.info("Intentando leer ficheros .jpg en Landing.")
                df = self.read_jpg()
            else:
                raise Exception(f"Format {self.format} not supported")

            if df:
                # Aplico el método add_metadata_columns a cada fila del df
                df = df.transform(self.add_metadata_columns)
        except Exception as e:
            logger.error(f"Algo ha ido mal intentando crear el readStream para {format}: {e}")
        else:
            logger.success(f"Lectura fichero {format} exitosa.")
        return df

    class Builder:
        """
        Clase que por temas de diseño, permite tener separada la lógica de sets de atributos de la clase original,
        dentro de esta clase Builder.

        :return: devuelve la instancia de la clase LandingStreamReader, pero con valores para sus atributos.
        """
        def __init__(self):
            # self.spark = SparkSession.builder.getOrCreate()
            self.config = None
            self.datasource = None
            self.dataset = None
            self.landing_path = None
            self.raw_path = None
            self.bronze_path = None
            self.format = None
            self.is_image = False

        # def set_spark(self, spark):
        #    self.spark = spark
        #    return self

        def set_config(self, config):
            self.config = config
            return self

        def set_datasource(self, datasource):
            self.datasource = datasource
            return self

        def set_dataset(self, dataset):
            self.dataset = dataset
            return self

        def set_landing_path(self, landing_path):
            self.landing_path = landing_path
            return self

        def set_raw_path(self, raw_path):
            self.raw_path = raw_path
            return self

        def set_bronze_path(self, bronze_path):
            self.bronze_path = bronze_path
            return self

        def set_format(self, format):
            self.format = format
            return self

        def set_is_image(self, is_image):
            self.is_image = is_image
            return self

        def build(self):
            return LandingStreamReader(self)