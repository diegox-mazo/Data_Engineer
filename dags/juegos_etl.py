from apiConsumer_module import extraer_datos_api
from apiConsumer_module import extraer_datos_file
from apiConsumer_module import concatenar_dataFrames
from apiConsumer_module import transformar_datos
from bdConnect_module import crear_tabla_bd
from bdConnect_module import insertar_datos_BD
import os

# Ruta del Archivo
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
ruta_archivo_json = AIRFLOW_HOME  + '/dags/raw_data/deals_Store_2.json'

# URL de la API
api_url = "https://www.cheapshark.com/api/1.0/deals?storeID=1&maxAge=24&sortBy=Recent"

#-------------------------------------------------------------------------------

def main():
    data_api = extraer_datos_api(api_url)
    data_file = extraer_datos_file(ruta_archivo_json)
    consolidated_data = concatenar_dataFrames(data_api , data_file)
    juegos_df = transformar_datos(consolidated_data)
    crear_tabla_bd()
    insertar_datos_BD(juegos_df)

#-----------------------------------------------------------------------------