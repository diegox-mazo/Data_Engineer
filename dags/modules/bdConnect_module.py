import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from airflow.models import Variable


# Conexion redshift
file_name = "redshift.txt"
host = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
dbname = "data-engineer-database"
user = "diegomazofl_coderhouse"
password = Variable.get("secret_pass_redshift")
port = '5439'

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