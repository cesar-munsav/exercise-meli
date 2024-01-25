# Proyecto exercise-meli
Este repositorio contiene los pipelines necesarios para poder generar una tabla en Bigquery con data necesaria 
para un modelo de Machine Learning

# Descripción de la arquitectura
Para este caso de uso se utilizará un Bucket en Google Cloud Storage que contenga 3 fuentes de datos, estos archivos se ingestarán en tablas de Google Bigquery para posteriormente realizar una query sobre ellas y así poder generar una tabla final con los datos necesarios para un modelo de Machine Learning. Los pipelines serán construidos en código Python usando Apache Airflow y se utilizará GitHub como sistema de control de versiones git.

# Diagrama de arquitectura
Dentro del repositorio se incluye un diagrama con la arquitectura utilizada en este proyecto.
url: https://github.com/cesar-munsav/exercise-meli/tree/main/pipeline%20diagram

# Descripción a detalle
Dentro del repositorio se incluye un documento con las decisiones tomadas en cada paso del flujo.
url: https://github.com/cesar-munsav/exercise-meli/tree/main/exercise_decisions

# Es necesario tener configuradas las siguientes variables como conectores en Airflow:
(Airflow Admin -> Connections -> Add new record)
path_dag_config_airflow_default : ruta del repositorio donde se encuentra la carpeta dags/dag_config con los archivos de configuracion
path_sql_airflow_default : ruta del repositorio donde se encuentra la carpeta sql que contiene el sql file
google_cloud_bigquery_airflow_default : conector de Bigquery para poder insertar data en tablas de Bigquery
google_cloud_gcs_airflow_default : conector de Cloud Storage para poder acceder a archivos dentro de un bucket


# Version de Python usada para el desarrollo:
Python 3.12