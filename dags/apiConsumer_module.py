from datetime import timedelta,datetime
import requests
import pandas as pd


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
