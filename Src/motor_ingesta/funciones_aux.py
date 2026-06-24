import datetime
from databricks.sdk.runtime import dbutils
from loguru import logger

def mover_a_landing(df, landing_path, datasource, dataset, format, transformar_en_parquet = False):
    """
      Guarda un DataFrame en un sistema de archivos distribuido con una estructura de directorios basada en la fecha actual,
      utilizando un formato específico. La función escribe el DataFrame en una ubicación temporal,
      lo mueve a una ruta final organizada por fuente de datos, conjunto de datos y marca de tiempo, y luego elimina el
      directorio temporal.

      Parámetros:
          df (pyspark.sql.DataFrame): El DataFrame de Spark que se desea guardar.
          datasource (str): Nombre o identificador de la fuente de datos, usado para organizar la ruta final.
          dataset (str): Nombre o identificador del conjunto de datos, usado para organizar la ruta final.
          format (str, opcional): Formato en el que se guardará el archivo. Por defecto es 'json'.
                                  Otros formatos soportados dependen de Spark (e.g., 'parquet', 'csv').

      Comportamiento:
          1. Escribe el DataFrame en una carpeta temporal (`tmp_path`) usando el formato especificado, coalesciendo los datos en un solo archivo.
          2. Genera una ruta final basada en la fecha actual (`YYYY/MM/DD`), el nombre de la fuente de datos, el conjunto de datos y una marca de tiempo.
          3. Mueve el archivo generado desde la carpeta temporal a la ruta final.
          4. Imprime la ruta final del archivo guardado.
          5. Elimina la carpeta temporal.

      Variables externas utilizadas:
          - landing_path (str): Ruta base del sistema de archivos donde se almacenan los datos. Debe estar definida globalmente.
          - dbutils.fs: Utilidad de Databricks para manipular el sistema de archivos (ls, mv, rm).
          - datetime: Módulo de Python para manejar fechas y marcas de tiempo.

      Ejemplo:
          save_file(mi_dataframe, "ventas", "diarias", format="parquet")
          # Salida esperada: "dbfs:/landing/ventas/diarias/2025/03/14/ventas_diarias_20250314123045.parquet"

      Notas:
          - La función asume que está ejecutándose en un entorno compatible con Databricks (por el uso de `dbutils.fs`).
          - Si el formato especificado no es compatible con Spark, se generará un error.
      """
    tmp_path = f'{landing_path}/tmp/'
    df.coalesce(1).write.format(format).mode("overwrite").save(tmp_path)
    now = datetime.datetime.utcnow()
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%Y%m%d%H%M%S")

    try:
        for file in dbutils.fs.ls(tmp_path):
            if file.name.endswith(f'.{format}'):
                final_path = file.path.replace('tmp', f'{datasource}/{dataset}')
                if transformar_en_parquet == True:
                    final_path = final_path.replace(file.name, f'{date_path}')
                    df.write.mode("overwrite").parquet(final_path)
                    dbutils.fs.rm(file.path)
                else:
                    final_path = final_path.replace(file.name, f'{date_path}/{datasource}-{dataset}-{timestamp}.{format}')
                    dbutils.fs.mv(file.path, final_path)
        dbutils.fs.rm(tmp_path, True)
    except:
        logger.error("Algo ha ido mal intentando mover el fichero a Landing.")
    else:
        logger.success(f"Fichero movido exitosamente a Landing, en {final_path}")

def mover_imagenes_landing(image_source, landing_path, datasource, dataset, format):
    """
    Versión muy simplificada de la función mover_a_landing, pero para ficheros de imágen, ya que para estos
    casos las imagenes no vendrán en un dataframe.

    :param image_source (str): ruta origen de la imagen.
    :param landing_path (str): ruta en Landing, que será nuestro destino.
    :param datasource (str): identificador de la fuente de datos, que luego se usa para construir la ruta final.
    :param dataset (str): identificador del set de datos, que luego se utiliza para construir la ruta final.
    :param format (str): formato o extensión del fichero a mover.
    """
    now = datetime.datetime.utcnow()
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%Y%m%d%H%M%S")
    final_path = f'{landing_path}/{datasource}/{dataset}/{date_path}/{datasource}-{dataset}-{timestamp}.{format}'
    try:
        dbutils.fs.cp(image_source, final_path)
    except:
        logger.error("Algo ha ido mal intentando mover la imagen a Landing.")
    else:
        logger.success(f"Imagen movida exitosamente a Landing, en {final_path}")

def read_config_client_properties(file_path):
    config = {}
    with open(file_path) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config