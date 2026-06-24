from loguru import logger
import os
from pyspark.sql import SparkSession
from databricks.sdk.runtime import dbutils, spark

class BronzeStreamWriter:
    """
    Clase que utilizará una sesión de Spark para escribir ficheros en la capa de Bronze de un Lakehouse, utilizando
    Autolader y teniendo como referencia la ruta en Landing. Permite también opcionalmente definir una columna para particionar
    al crear la Delta Table.

    Finalmente, guardará una copia en la capa Raw.
    """
    def __init__(self, builder):
        """
        Constructor de la clase.

        :param builder: Clase builder, dentro de esta clase.
        """
        # self.spark = builder.spark
        self.merge_schema = builder.merge_schema
        self.partition_column = builder.partition_column
        self.datasource = builder.datasource
        self.dataset = builder.dataset
        self.landing_path = builder.landing_path
        self.raw_path = builder.raw_path
        self.bronze_path = builder.bronze_path
        self.is_source_kafka = builder.is_source_kafka
        self.dataset_landing_path = f"{self.landing_path}/{self.datasource}/{self.dataset}"
        self.dataset_raw_path = f"{self.raw_path}/{self.datasource}/{self.dataset}"
        self.dataset_bronze_path = f"{self.bronze_path}/{self.datasource}/{self.dataset}"
        self.dataset_checkpoint_location = f'{self.dataset_bronze_path}_checkpoint'
        self.query_name = f"bronze-{self.datasource}-{self.dataset}"
        self.config = builder.config
        if self.config["EXECUTION_ENVIRONMENT"] == "databricks":
            #from databricks.sdk.runtime import dbutils, spark
            self.table = f'hive_metastore.bronze.{self.datasource}_{self.dataset}'
            dbutils.fs.mkdirs(self.dataset_raw_path)
            dbutils.fs.mkdirs(self.dataset_bronze_path)
            dbutils.fs.mkdirs(self.dataset_checkpoint_location)
        elif self.config["EXECUTION_ENVIRONMENT"] == "local":
            self.table = f'hive_metastore.bronze.{self.datasource}_{self.dataset}'
            os.makedirs(self.dataset_raw_path, exist_ok=True)
            os.makedirs(self.dataset_bronze_path, exist_ok=True)
            os.makedirs(self.dataset_checkpoint_location, exist_ok=True)
        else:
            logger.error("Por favor elija un entorno de ejecución válido.")

    def __str__(self):
        return (f"BronzeStreamWriter(datasource='{self.datasource}',dataset='{self.dataset}')")

    def archive_raw_files(self, df):
        """
        Método de la clase, que recibe un df de Spark y utilizando la columna de metadatos _ingested_filename,
        intentará mover los ficheros a la capa Raw.

        :param df: dataframe de Spark.
        """

        try:
            if "_ingested_filename" in df.columns:
                files = [row["_ingested_filename"] for row in df.select("_ingested_filename").distinct().collect()]
                for file in files:
                    if file:
                        file_landing_path = file.replace(self.dataset_raw_path, self.dataset_landing_path)
                        dbutils.fs.mkdirs(file[0:file.rfind('/') + 1])
                        dbutils.fs.mv(file_landing_path, file)
        except:
            logger.error("Error intentando guardar ficheros en capa Raw.")
        else:
            logger.success("Ficheros movidos a capa Raw exitosamente.")

    def write_data(self, df):
        """
        Método de la clase, que creará una BBDD en el hive_metastore para la capa de bronce, si no existe, luego
        creará una tabla delta en la locación de la capa de bronce, si no existe. Finalmente, escibirá en
        la capa de bronce cada fichero que se le haya dado como input en un df de Spark.
        Permite particionar la información en el destino final en columnas, de haberse elegido una al settear
        el atributo opcional desde el builder.

        :param df: dataframe de Spark.
        """
        logger.info("Intentando escribir fichero en Bronce.")
        # spark = self.spark
        spark.sql('CREATE DATABASE IF NOT EXISTS hive_metastore.bronze')
        spark.sql(f"CREATE TABLE IF NOT EXISTS {self.table} USING DELTA LOCATION '{self.dataset_bronze_path}' ")
        logger.info("Intentando escribir en tabla Delta.")
        try:
            if self.partition_column:
                logger.info("Escribiendo fichero de Landing en tabla Delta con particiones.")
                (df.write
                 .partitionBy(self.partition_column)
                 .format("delta")
                 .mode("append")
                 .option("mergeSchema", str(self.merge_schema))
                 .option("path", self.dataset_bronze_path)
                 .saveAsTable(self.table)
                 )
            else:
                logger.info("Escribiendo fichero de Landing en tabla Delta sin particiones.")
                (df.write
                 .format("delta")
                 .mode("append")
                 .option("mergeSchema", str(self.merge_schema))
                 .option("path", self.dataset_bronze_path)
                 .saveAsTable(self.table)
                 )
        except Exception as e:
            logger.error("Error intentando guardar ficheros en tabla Delta: {e}")
        else:
            logger.success("Tabla escrita con éxito.")

    def append_to_bronze(self, batch_df, batch_id):
        """
        Método de la clase, que recibirá un dataframe de Spark y un ID de batch, para luego persistirlo
        en memoria, intentar escribir los ficheros en la capa bronce llamando al método write_data de la
        clase, luego lo mismo en la capa Raw con el método archive_raw_files y finalmente despersistir el df.

        :param batch_df: dataframe de Spark.
        :param batch_id: ID que el writeStream luego utilizará para iterar por fichero.
        """
        batch_df.persist()
        self.write_data(batch_df)
        if self.is_source_kafka == False:
            self.archive_raw_files(batch_df)
        batch_df.unpersist()
        logger.success("Ficheros movidos a capa Bronce exitosamente.")

    class Builder:
        """
        Clase que por temas de diseño, permite tener separada la lógica de sets de atributos de la clase original,
        dentro de esta clase Builder.

        :return: devuelve la instancia de la clase BronzeStreamWriter, pero con valores para sus atributos.
        """
        def __init__(self):
            # self.spark = SparkSession.builder.getOrCreate()
            self.config = None
            self.datasource = None
            self.dataset = None
            self.landing_path = None
            self.raw_path = None
            self.bronze_path = None
            self.partition_column = None
            self.is_source_kafka = False
            self.merge_schema = "false"

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

        def set_partition_column(self, partition_column):
            self.partition_column = partition_column
            return self

        def set_is_source_kafka(self, is_source_kafka):
            self.is_source_kafka = is_source_kafka
            return self

        def set_merge_schema(self, merge_schema):
            self.merge_schema = merge_schema
            return self

        def build(self):
            return BronzeStreamWriter(self)