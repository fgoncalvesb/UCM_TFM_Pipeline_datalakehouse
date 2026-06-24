from loguru import logger
import pyspark.sql.functions as F
from databricks.sdk.runtime import dbutils, spark
from pyspark.sql.avro.functions import from_avro

class KafkaStreamReader:
    """
    Clase que utilizará una sesión de Spark para leer mensajes de uno o varios topics de Kafka, sea con método
    singleplex con esquemas json o avro, o multiplex con esquema json.
    """

    def __init__(self, builder):
        """
        Constructor de la clase.

        :param builder: Clase builder, dentro de esta clase.
        """
        # self.spark = builder.spark
        self.config = builder.config
        self.ingestion_method = builder.ingestion_method
        self.kafka_options = builder.kafka_options
        self.schema_format = builder.schema_format
        self.schema = builder.schema
        self.schema_registry_conf = builder.schema_registry_conf
        if self.config["EXECUTION_ENVIRONMENT"] == "databricks":
            from databricks.sdk.runtime import dbutils, spark

    def __str__(self):
        return (f"KafkaStreamReader(kafka_options='{self.kafka_options}')")

    def add_metadata_columns(self, df):
        """
        Método de la clase, que agregará columnas de metadatos a un DataFrame de spark. Las columnas a agregar
        variarán según si el método de ingestión y el formato del fichero que define el esquema.

        :param df: dataframe de Spark
        :return: Devuelve un DataFrame de spark, con las columnas de metadatos agregadas por delante.
        """

        # renombramos columnas
        columns = [F.col(column).alias(f'_{column}') for column in df.columns]
        df = df.select(*columns)

        try:
            if self.schema_format == "json" and self.ingestion_method == "singleplex":
                df = (df
                      .withColumn("_ingested_at", F.current_timestamp())  # metadata
                      .withColumn("value", F.from_json(F.col("_value").cast("string"), self.schema))
                      .select("*", "value.*")
                      .drop("value")
                      )
            elif self.schema_format == "avro" and self.ingestion_method == "singleplex":
                df = (df
                      .withColumn("value", from_avro(F.expr("substring(_value,6,length(_value)-5)"), self.schema))
                      .withColumn("_ingested_at", F.current_timestamp())  # metadata
                      .select("*", "value.*")
                      .drop("value")
                      )
        except Exception as e:
            logger.error(f"Algo ha ido mal intentando agregar las columnas de metadatos: {e}")

        return df

    def read_singleplex(self):
        """
        Método de la clase, que creará un stream de lectura de Spark, con formato Kafka ya que leerá de topics de Kafka.
        Cargará las opciones de configuración de kafka que vengan con la clase como atributo.
        Este método es específico para cuando se carga con método singleplex.

        :return: El objeto de readStream de Spark.
        """
        # spark = self.spark
        try:
            return (spark
                    .readStream
                    .format("kafka")
                    .options(**self.kafka_options)
                    .load()
                    )
        except Exception as e:
            logger.error(f"Algo ha ido mal intentando crear el readStream con format singleplex: {e}")

    def read_multiplex(self):
        """
        M        Método de la clase, que creará un stream de lectura de Spark, con formato Kafka ya que leerá de topics de Kafka.
        Cargará las opciones de configuración de kafka que vengan con la clase como atributo.
        Este método es específico para cuando se carga con método multiplex.
        No es muy distinto al de singleplex, sólo he agregado el agregar la columna de metadatos aquí.
        Lo he separado más que nada porque es buena práctica para que sea más "modular" el código.

        :return: El objeto de readStream de Spark.
        """
        # spark = self.spark
        try:
            return (spark
                    .readStream
                    .format("kafka")
                    .options(**self.kafka_options)
                    .load()
                    .withColumn("ingested_at", F.current_timestamp())
                    )
        except Exception as e:
            logger.error(f"Algo ha ido mal intentando crear el readStream con format multiplex: {e}")

    def read(self):
        """
        Método de la clase, que según el atributo de formato definido por el builder al instanciar la clase, define
        el formato del método de ingestión de Kafka y formato del fichero de esquema que se definirá al leer el topic, afectando luego
        la escritura en la capa de Bronce.

        :return: El objeto de readStream de Spark.
        """
        df = None

        try:
            if (self.ingestion_method == "singleplex" and (
                    self.schema_format == "json" or self.schema_format == "avro")):
                logger.info("Intentando leer ficheros desde Kafka con método singleplex y esquema json.")
                df = self.read_singleplex()
            elif (self.ingestion_method == "multiplex"):
                logger.info("Intentando leer ficheros desde Kafka con método multiplex.")
                df = self.read_multiplex()
            else:
                raise Exception(f"Método de ingesta {self.format} no contemplado en schema_format.")

            if df:
                df = df.transform(self.add_metadata_columns)
        except Exception as e:
            logger.error(f"Algo ha ido mal intentando crear el readStream desde Kafka: {e}")
        else:
            logger.success(f"Lectura topic desde Kafka exitosa.")
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
            self.bronze_path = None
            self.ingestion_method = None
            self.kafka_options = None
            self.schema = None
            self.schema_registry_conf = None
            self.schema_format = None

        # def set_spark(self, spark):
        #    self.spark = spark
        #    return self

        def set_config(self, config):
            self.config = config
            return self

        def set_bronze_path(self, bronze_path):
            self.bronze_path = bronze_path
            return self

        def set_ingestion_method(self, ingestion_method):
            self.ingestion_method = ingestion_method
            return self

        def set_kafka_options(self, kafka_options):
            self.kafka_options = kafka_options
            return self

        def set_schema(self, schema):
            self.schema = schema
            return self

        def set_schema_registry_conf(self, schema_registry_conf):
            self.schema_registry_conf = schema_registry_conf
            return self

        def set_schema_format(self, schema_format):
            self.schema_format = schema_format
            return self

        def build(self):
            return KafkaStreamReader(self)