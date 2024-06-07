from datetime import timedelta,datetime
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from airflow.models import Variable
import os

# Ruta del Archivo
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
ruta_archivo_json = AIRFLOW_HOME  + '/dags/raw_data/deals_Store_2.json'

# URL de la API
api_url = "https://www.cheapshark.com/api/1.0/deals?storeID=1&maxAge=24&sortBy=Recent"

# Conexion redshift
file_name = "redshift.txt"
host = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
dbname = "data-engineer-database"
user = "diegomazofl_coderhouse"
password = Variable.get("secret_pass_redshift")
port = '5439'

#--------------------------------------------------------------------------------------------------

# Función para extraer datos de la API
def extraer_datos_api(url):
    print('Extrayendo datos de API')
    response = requests.get(url)
    data = response.json()
    data_df = pd.DataFrame(data)
    print(data_df.head())
    print(data_df.shape)
    return data_df

# Función para extraer datos de un Archivo
def extraer_datos_file(path):
    print('Leyendo datos de Archivo')
    try:
        print(path)
        data_df = pd.read_json(path)
        print(data_df.head())
        print(data_df.shape)
        return data_df
    except Exception as ex:
        print("No es posible leer el archivo")
        print(ex)

# Función para combinar fuentes de datos dataFrames
def concatenar_dataFrames(df1 , df2):
    print('Combinando Data Frames')
    data_merged_df = pd.concat([df1, df2])
    print(data_merged_df.sample(5))
    print('Nuevo tamaño: ', data_merged_df.shape)
    return data_merged_df

# Procesamiento de datos con DataFrame Pandas
def transformar_datos(juegos_df):
    print('Transformando y Limpiando Datos DF')
    #juegos_df = pd.DataFrame(data)
    juegos_df.columns = ['NombreInterno', 'Titulo', 'MetacriticLink', 'OfertaID', 'TiendaID', 'JuegoID', 'PrecioOferta', 'PrecioNormal', 'isOnSale', 'Ahorro', 'MetacriticScore', 'SteamScoreTexto', 'SteamScorePorcentaje', 'NumeroCalificaciones', 'SteamAppID', 'FechaLanzamiento', 'UltimaModificacion', 'PuntajeOferta', 'ImagenJuego']
    # Organizar tipo de datos
    juegos_df['TiendaID'] = juegos_df['TiendaID'].astype('int64')
    juegos_df['PrecioOferta'] = juegos_df['PrecioOferta'].astype('float64')
    juegos_df['PrecioNormal'] = juegos_df['PrecioNormal'].astype('float64')
    juegos_df['isOnSale'] = juegos_df['isOnSale'].astype('int64')
    juegos_df['Ahorro'] = juegos_df['Ahorro'].astype('float64')
    juegos_df['MetacriticScore'] = juegos_df['MetacriticScore'].astype('int64')
    juegos_df['SteamScorePorcentaje'] = juegos_df['SteamScorePorcentaje'].astype('float64')
    juegos_df['NumeroCalificaciones'] = juegos_df['NumeroCalificaciones'].astype('int64')
    juegos_df['FechaLanzamiento'] = juegos_df['FechaLanzamiento'].astype("datetime64[s]")
    juegos_df['UltimaModificacion'] = juegos_df['UltimaModificacion'].astype("datetime64[s]")
    juegos_df['PuntajeOferta'] = juegos_df['PuntajeOferta'].astype('float64')
    ## Reordenar columnas
    juegos_df = juegos_df.reindex(['JuegoID', 'NombreInterno', 'Titulo', 'TiendaID', 'SteamAppID', 'OfertaID', 'isOnSale', 'PrecioOferta', 'PrecioNormal', 'Ahorro', 'PuntajeOferta', 'MetacriticLink', 'MetacriticScore', 'SteamScoreTexto', 'SteamScorePorcentaje', 'NumeroCalificaciones', 'FechaLanzamiento', 'UltimaModificacion', 'ImagenJuego'], axis=1)
    juegos_df.dtypes
    #Evitar registros con valores nulos o vacios en los campos principales
    juegos_df.dropna(subset=['JuegoID', 'OfertaID','PrecioOferta', 'Titulo'], inplace=True)
    juegos_df.fillna(0, inplace=True)
    #Evitar que haya registros duplicados
    juegos_df.drop_duplicates(subset=['JuegoID', 'OfertaID','PrecioOferta'], keep='first', inplace=True)
    print(juegos_df.sample(10))
    print('tamaño: ',juegos_df.shape)
    return juegos_df


# Creando la conexión a Redsshift 
def conectar_con_DB():
    try:
        conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=port
        )
        print("Conectado a Redshift con éxito!")
        return conn
    except Exception as e:
        print("No es posible conectar a Redshift")
        print(e)


#Crear Tabla en BD
def crear_tabla_bd():
    query = """CREATE TABLE IF NOT EXISTS diegomazofl_coderhouse.juegos_en_oferta(
                id INTEGER IDENTITY(1,1) PRIMARY KEY,
                JuegoID VARCHAR(10),
                NombreInterno VARCHAR(100),
                Titulo VARCHAR(100),
                TiendaID INTEGER,
                SteamAppID VARCHAR(10),
                OfertaID VARCHAR(MAX),
                isOnSale BOOLEAN,
                PrecioOferta FLOAT,
                PrecioNormal FLOAT,
                Ahorro FLOAT,
                PuntajeOferta FLOAT,
                MetacriticLink VARCHAR(100),
                MetacriticScore FLOAT,
                SteamScoreTexto VARCHAR(50),
                SteamScorePorcentaje DECIMAL,
                NumeroCalificaciones INTEGER,
                FechaLanzamiento DATETIME,
                UltimaModificacion DATETIME,
                ImagenJuego VARCHAR(MAX),
                FechaInsercion DATETIME DEFAULT GETDATE()
            )"""
    conn = conectar_con_DB()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
        print("Tabla creada en Redshift")
    except Exception as e:
        print("Error creando tabla en Redshift")
        print(e)
    #Vaciar la tabla para evitar duplicados o inconsistencias
    try:
        with conn.cursor() as cur:
            cur.execute("Truncate table juegos_en_oferta")
            count = cur.rowcount
            conn.close() 
    except Exception as e:
        print("Error vaciando tabla en Redshift")
        print(e)


#Insertando los datos en Redsfhift
def insertar_datos_BD(data_df):
    query = ''' INSERT INTO juegos_en_oferta (JuegoID, NombreInterno, Titulo, TiendaID, SteamAppID, OfertaID, isOnSale, PrecioOferta, PrecioNormal, Ahorro, PuntajeOferta, MetacriticLink, MetacriticScore, SteamScoreTexto, SteamScorePorcentaje, NumeroCalificaciones, FechaLanzamiento, UltimaModificacion, ImagenJuego)
                VALUES %s '''
    conn = conectar_con_DB()
    try:
        with conn.cursor() as cur:
            execute_values(cur, query, [tuple(row) for row in data_df.values], page_size=len(data_df))
            conn.commit()
        cur.close()
        conn.close()
        print("Datos insertados exitosamente en Redshift")
    except Exception as e:
        print("Error creando tabla en Redshift")
        print(e)

#-------------------------------------------------------------------------------

def main():
    data_api = extraer_datos_api(api_url)
    data_file = extraer_datos_file(ruta_archivo_json)
    consolidated_data = concatenar_dataFrames(data_api , data_file)
    juegos_df = transformar_datos(consolidated_data)
    crear_tabla_bd()
    insertar_datos_BD(juegos_df)

#-----------------------------------------------------------------------------