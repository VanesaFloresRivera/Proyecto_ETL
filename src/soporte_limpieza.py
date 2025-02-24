import pandas as pd
import numpy as np
import requests
from datetime import datetime
import pyarrow
import os
import sys #permite navegar por el sistema
sys.path.append("../") #solo aplica al soporte
import src.soporte_limpieza as sl
import src.soporte_escrapeo as se

def tran_data(lista_col, dataframe):
    """
    Convierte las columnas especificadas de un DataFrame a tipo datetime.

    Args:
        lista_col (list): Lista de nombres de columnas a convertir.
        dataframe (pd.DataFrame): DataFrame que contiene las columnas a transformar.

    Returns:
        pd.DataFrame: DataFrame con las columnas convertidas a datetime.
    """
    for col in lista_col:
        dataframe[col] = pd.to_datetime(dataframe[col], errors="coerce")


def detectar_oulieres_iqr(dataframe):
    """
    Detecta outliers en la variable 'precio_noche' de un DataFrame utilizando el método IQR (rango intercuartil).
    
    La función se enfoca en hoteles que no pertenecen a la competencia y calcula los outliers 
    para cada hotel basado en la columna 'precio_noche'.

    Args:
        dataframe (pd.DataFrame): DataFrame con las columnas 'competencia', 'nombre_hotel' y 'precio_noche'.

    Returns:
        dict: Un diccionario donde las claves son los nombres de los hoteles y los valores son diccionarios 
              con la cantidad y el porcentaje de outliers detectados en 'precio_noche'.
    """
    df_no_competencia = dataframe[dataframe.competencia == False]
    lista_hoteles_propios = df_no_competencia.nombre_hotel.values
    resultados = {}
    for nombrehotel in lista_hoteles_propios:
        df_filtrado = dataframe[dataframe['nombre_hotel']== nombrehotel]
        Q1=df_filtrado['precio_noche'].quantile(0.25) #Primer cuartil
        Q3 = df_filtrado['precio_noche'].quantile(0.75) #Tercer cuartil
        IQR = Q3-Q1 #rango intercuartil

        #Defino los limites de los outliers
        lower_bound = Q1-1.5*IQR
        upper_bound = Q3+1.5*IQR

        #Filtrar los valores outliers
        outliers = df_filtrado[(df_filtrado['precio_noche'] < lower_bound) | (df_filtrado['precio_noche'] > upper_bound)]
        num_outliers = len(outliers)
        total_values = len(df_filtrado)
        percentage_outliers = (num_outliers/total_values)*100

        #Guardo los resultados
        resultados[nombrehotel] = {'Cantidad de Outliers': num_outliers,
                                                            'Porcentaje de Outliers': round(percentage_outliers,2)
                                                            }
    return resultados

def incorporar_información_df_original (dataframe_a_rellenar, df_valores_correctos, columna_union, columna_valores_correctos, columna_a_rellenar, filtro_df_original=None):
    """
    Completa valores en un DataFrame a partir de otro DataFrame de referencia usando un diccionario de mapeo.

    Args:
        dataframe_a_rellenar (pd.DataFrame): DataFrame donde se rellenarán los valores.
        df_valores_correctos (pd.DataFrame): DataFrame con los valores correctos de referencia.
        columna_union (str): Nombre de la columna clave utilizada para la unión entre ambos DataFrames.
        columna_valores_correctos (str): Nombre de la columna en 'df_valores_correctos' con los valores a insertar.
        columna_a_rellenar (str): Nombre de la columna en 'dataframe_a_rellenar' donde se colocarán los valores correctos.
        filtro_df_original (pd.Series, optional): Filtro booleano para aplicar la actualización solo a ciertas filas. 
                                                  Si es None, se actualizará toda la columna. Default es None.

    Returns:
        None: Modifica 'dataframe_a_rellenar' directamente.
    """
    keys= df_valores_correctos[columna_union].to_list()
    values = df_valores_correctos[columna_valores_correctos].to_list()
    diccionario_creado =dict(zip(keys,values))
    if filtro_df_original is not None:
        dataframe_a_rellenar.loc[filtro_df_original,columna_a_rellenar] = dataframe_a_rellenar[columna_union].map(diccionario_creado)
    else:
        dataframe_a_rellenar[columna_a_rellenar] = dataframe_a_rellenar[columna_union].map(diccionario_creado)


def limpieza_datos(ruta_entrada, ruta_salida,ruta_escrapeo,url = "https://all.accor.com/ssr/app/ibis/hotels/madrid-spain/open/index.es.shtml?compositions=1&stayplus=false&snu=false&hideWDR=false&accessibleRooms=false&hideHotelDetails=false&dateIn=2025-03-01&nights=1&destination=madrid-spain"):
    """
    Realiza la limpieza y transformación de un dataset de reservas hoteleras, incluyendo:

    - Carga del archivo de entrada y eliminación de duplicados.
    - Conversión de columnas de fecha al formato datetime.
    - Creación de un ID único por cliente.
    - Relleno de valores nulos en fechas de estancia.
    - Creación y asignación de ID único por hotel.
    - Cálculo de valoración de hoteles en base a estrellas.
    - Relleno de valores nulos del precio por noche basado en promedios.
    - Web scraping para completar información de la competencia.
    - Eliminación de columnas innecesarias.
    - Guarda el dataset limpio en formato pickle.

    Args:
        ruta_entrada (str): Ruta del archivo de entrada en formato Parquet.
        ruta_salida (str): Ruta donde se guardará el DataFrame limpio en formato pickle.
        ruta_escrapeo (str): Ruta donde se guardarán los datos obtenidos mediante web scraping.
        url (str, opcional): URL utilizada para obtener información de la competencia mediante web scraping. 
                             Por defecto, apunta a una página de hoteles en Madrid.

    Returns:
        None: La función modifica y guarda el DataFrame limpio sin retornarlo.

    Raises:
        FileNotFoundError: Si el archivo de entrada no existe.
    """
    if not os.path.exists(ruta_entrada):
        print(f'Error: No se encontró el archivo {ruta_entrada}')
    else:
        df_raw=pd.read_parquet(ruta_entrada) #importo el fichero original
        df=df_raw.copy() #Hago una copia del df original para seguir trabajando con él
        df = df.drop_duplicates() #Elimino los duplicados
        print(f'El fichero original se ha importando y se han eliminado los duplicados')

        #CONVIERTO LAS COLUMNAS EN FORMATO FECHA
        if "fecha_reserva" in df.columns and "inicio_estancia" in df.columns and "final_estancia" in df.columns:
            col_fechas=["fecha_reserva","inicio_estancia","final_estancia"]
            sl.tran_data(col_fechas, df)
            print(f'Se han convertido las columnas {col_fechas} a formato fecha')

        #CREACIÓN ID CLIENTE ÚNICO:
        #Creo un id para cada cliente basandome la unión del nombre y apellido:
        if "nombre" in df.columns and "apellido" in df.columns:
            df["nombre_apellido"] = df['nombre'].str.lower() +'.' +df['apellido'].str.lower()
            df.nombre_apellido.unique()
            df_clientes_unicos = pd.DataFrame(df.nombre_apellido.unique())
            df_clientes_unicos['id_cliente_unico'] = None
            for indice in df_clientes_unicos.index:
                df_clientes_unicos.iloc[indice,1] = 'C'+str(indice)+df_clientes_unicos.iloc[indice,0].split('.')[0][0]+df_clientes_unicos.iloc[indice,0].split('.')[1][0]
            df_clientes_unicos=df_clientes_unicos.set_axis(['nombre_apellido', 'id_cliente_unico'], axis=1)

            sl.incorporar_información_df_original(df,df_clientes_unicos, 'nombre_apellido', 'id_cliente_unico', 'id_cliente_unico') #incorporo el id_cliente_unico al df original
            print(f'Se ha creado un id único para cada cliente. Se han creado {len(df_clientes_unicos)} ids de clientes únicos')

        #COMPLETAR VALORES NULOS DE LAS COLUMNAS INICIO Y FINAL ESTANCIA:
        if "inicio_estancia" in df.columns:
            df["inicio_estancia"] = df[df["inicio_estancia"].notna()].inicio_estancia[0]
        if "final_estancia" in df.columns:
            df["final_estancia"] =  df[df["final_estancia"].notna()].final_estancia[0]
        print(f'Se ha completado los nulos de las fechas de inicio estancia con el valor {df["inicio_estancia"][0]} y fin estancia con el valor {df["final_estancia"][0]}')

        #CREACIÓN ID HOTEL ÚNICO:
        #Creo un diccionario nuevo para darle un id a cada hotel propio según el nombre:
        if "competencia" in df.columns and "nombre_hotel" in df.columns and "id_hotel" in df.columns:
            df_no_competencia = df[df.competencia == False]
            lista_nombre_hoteles = df.nombre_hotel.unique().tolist()
            lista_ids_hoteles = df_no_competencia.id_hotel.unique().tolist()
            dict_id_nombre_hotel = {'id_hotel': [], 
                                'nombre_hotel': []}
            for hotel in lista_nombre_hoteles[1:]:
                dict_id_nombre_hotel["nombre_hotel"].append(hotel)
            for id in lista_ids_hoteles:
                dict_id_nombre_hotel["id_hotel"].append(id)
            dict_id_nombre_hotel
            df_id_hoteles_nombre_hoteles = pd.DataFrame(dict_id_nombre_hotel)

            sl.incorporar_información_df_original(df,df_id_hoteles_nombre_hoteles, 'nombre_hotel', 'id_hotel', 'id_hotel',df['competencia']==False ) #incorporo el id_hotel correcto al df original
            print(f'Se han creado los id de hoteles únicos propies. Existen {len(df_id_hoteles_nombre_hoteles)} hoteles')

        #CREACIÓN VALORACIÓN HOTEL:
        #Calculo la valoración para cada hotel como la media de las estrellas:
        if "estrellas" in df.columns and "nombre_hotel" in df.columns:
            df["estrellas"] = df["estrellas"].astype('Int64') #transformo la columna estrellas a tipo entero:
            df_valoracion = df_no_competencia.groupby('nombre_hotel')['estrellas'].mean().round(1) #creo un nuevo df para calcular la valoración de cada hotel según la media de las estrellas
            df_valoracion= df_valoracion.reset_index()

            sl.incorporar_información_df_original(df,df_valoracion, 'nombre_hotel', 'estrellas', 'valoracion',df['competencia']==False ) #incorporo la valoración al df original
            print('Se ha incorporado la información de valoración de los hoteles propios')

        #COMPLETAR VALORES NULOS DEL PRECIO POR NOCHE:
        #Calculo el precio medio de la noche teniendo en cuenta tanto el nombre como la fecha de la reserva:
        if "nombre_hotel" in df.columns and "fecha_reserva" in df.columns:
            df_precios_nulos_hoteles_propios = df_no_competencia[df_no_competencia.precio_noche.isnull()].groupby(['nombre_hotel', 'fecha_reserva'])['id_reserva'].count().reset_index()
            df_precios_nulos_hoteles_propios["precio_noche_medio"] = None
            for indice in range(0,len(df_precios_nulos_hoteles_propios)):
                nombre_hotel= df_precios_nulos_hoteles_propios.iloc[indice,:].nombre_hotel
                fecha_reserva=df_precios_nulos_hoteles_propios.iloc[indice,:].fecha_reserva
                df_precios_nulos_hoteles_propios.iloc[indice,3] = df[df.nombre_hotel == nombre_hotel][df[df.nombre_hotel == nombre_hotel].fecha_reserva == fecha_reserva].precio_noche.mean().round(2)

            #Obtengo los indices de los que tienen precios nulos y son hoteles propios:
            lista_indices_precios_nulos_hoteles_propios = df_no_competencia[df_no_competencia.precio_noche.isnull()].index

            #Sustituyo los precios que falta en el DF original teniendo en cuenta los precios medios calculados:
            for indice in lista_indices_precios_nulos_hoteles_propios:
                fila = df.iloc[indice,:]
                nombre_hotel = fila['nombre_hotel']
                fecha_reserva = fila['fecha_reserva']
                precio_medio = df_precios_nulos_hoteles_propios[(df_precios_nulos_hoteles_propios['nombre_hotel'] == nombre_hotel) & (df_precios_nulos_hoteles_propios['fecha_reserva'] == fecha_reserva)].precio_noche_medio
                df.iloc[indice,10]=precio_medio
            print('Se ha completado los precios por noche nulos existentes, según el precio medio por hotel y fecha de reserva')

            #COMPLETAR LOS DATOS FALTANTES DE LA COMPETENCIA CONSEGUIDOS CON EL ESCRAPEO:
            url1= url
            df_escrapeo = se.escrapeo(url1, ruta_escrapeo)
            print(f'Se ha importado la información de los hoteles de la competencia mediante escrapeo. Se ha importado la información de {len(df_escrapeo)} hoteles')

        #Creo un df con el nombre de los hoteles de la competencia y el id del hotel:
        if "competencia" in df.columns and "nombre_hotel" in df.columns and "id_hotel" in df.columns:
            df_competencia = df[df.competencia == True] 
            values = list(df_competencia.id_hotel.drop_duplicates())
            keys = list(df_escrapeo.nombre_hotel.unique())
            diccionario_idhotel_nombrehotel = dict(zip(keys,values))
            df_idhotel_nombrehotel_competencia = pd.DataFrame(list(diccionario_idhotel_nombrehotel.items()), columns=['nombre_hotel', 'id_hotel'])

            sl.incorporar_información_df_original(df_escrapeo,df_idhotel_nombrehotel_competencia, 'nombre_hotel', 'id_hotel', 'id_hotel') #Incorpor el id del hotel al df del escrapeo
            print(f'Se ha asignado el nombre del hotel de la competencia a los {len(df_idhotel_nombrehotel_competencia)} id de hoteles existentes.')
            #Renombro las columnas del df_escrapeo:
            df_escrapeo=df_escrapeo.rename(columns={
                'rating': 'valoracion',
                'fecha_escrapeo': 'fecha_reserva'
            })

            #Incorporo la información del df del escrapeo al df original
            for columna in df_escrapeo.columns[:-1]:
                sl.incorporar_información_df_original(df,df_escrapeo, 'id_hotel', columna, columna,df['competencia']==True )
            print(f'Se ha incorporado la información de las columnas {df_escrapeo.columns[:-1].to_list()} de los hoteles de la competencia en el fichero original')

        #ELIMINO LAS COLUMNAS QUE NO NECESITO:
        if "id_cliente" in df.columns and "estrelas" in df.columns and "nombre_apellido" in df.columns:
            df.drop(columns=['id_cliente', 'estrellas','nombre_apellido'],inplace=True)
            print(f'Se han eliminado las columnas que no son necesarias')

        #GUARDO EL DF COMO PICKLE:
        df.to_pickle(ruta_salida)
        print(f'Se ha guardado el fichero limpio en la ruta {ruta_salida}')
        print('LA LIMPIEZA DE LOS DATOS SE HA COMPLETADO SATISFACTORIAMENTE')