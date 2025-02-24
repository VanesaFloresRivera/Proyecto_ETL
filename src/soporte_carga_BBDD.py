import pandas as pd
import numpy as np
import psycopg2
import sys #permite navegar por el sistema
sys.path.append("../") #solo aplica al soporte
import src.soporte_api as sa
import src.soporte_carga_BBDD as sc


def insertar_datos_en_BBDD(insert_query,data_to_insert):
    """
    Inserta datos en una base de datos PostgreSQL utilizando una consulta SQL de inserción.

    Establece una conexión con la base de datos, ejecuta la consulta de inserción 
    y cierra la conexión una vez finalizada la operación.

    Args:
        insert_query (str): Consulta SQL de inserción con placeholders (%s).
        data_to_insert (list | tuple): Datos a insertar. Puede ser:
            - Una lista de tuplas (para insertar múltiples filas a la vez).
            - Una única tupla (para insertar una sola fila).

    Returns:
        None: La función ejecuta la inserción y confirma los cambios en la base de datos.
    
    Raises:
        psycopg2.DatabaseError: Si ocurre un error al conectar o insertar los datos en la base de datos.
    """
    #creo la conexión:
    conn = psycopg2.connect(
    dbname="Proyecto_ETL_Reservas_Madrid",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
    )

    cur= conn.cursor()#Creo el cursor:

    if isinstance(data_to_insert,list): # si data_to_insert es una lista
        cur.executemany(insert_query,data_to_insert) #ejecuto la acción de subida a la BBDD
    else: # si data_to_insert no es una lista
        cur.execute(insert_query,data_to_insert) #ejecuto la acción de subida a la BBDD
    conn.commit() #guardo la subida

    cur.close() #cierro el cursor
    conn.close() #cierro la conexión

def extraer_datos_de_BBDD(query_extracción):
    """
    Extrae datos de una base de datos PostgreSQL y los devuelve en un diccionario.

    Establece una conexión con la base de datos, ejecuta la consulta SQL proporcionada, 
    recupera los resultados y los devuelve en un diccionario.

    Args:
        query_extracción (str): Consulta SQL para extraer los datos.

    Returns:
        dict: Diccionario con los resultados de la consulta, donde las claves y valores 
        dependen de la estructura de la consulta SQL ejecutada.

    Raises:
        psycopg2.DatabaseError: Si ocurre un error al conectar o extraer los datos de la base de datos.
    """
    #creo la conexión:
    conn = psycopg2.connect(
    dbname="Proyecto_ETL_Reservas_Madrid",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
    )
    cur= conn.cursor()#Creo el cursor:

    cur.execute(query_extracción)
    diccionario = dict(cur.fetchall())

    cur.close() #cierro el cursor
    conn.close() #cierro la conexión

    return diccionario


def carga_completa_datos (ruta_importación_datos_limpios, ruta_entrada,ruta_guardar_eventos):
    """
    Carga los datos en varias tablas de la base de datos, procesando y organizando la información 
    extraída del archivo de reservas y eventos, y gestionando la inserción en las tablas 'ciudad', 
    'eventos', 'hoteles', 'clientes' y 'reservas' de la base de datos.

    Pasos del proceso:
    1. **Carga de la tabla 'ciudad'**: Se extrae el nombre de la ciudad desde el archivo de reservas y se inserta en la base de datos.
    2. **Carga de la tabla 'eventos'**: Se obtiene información de eventos desde una API, se asocia con las ciudades existentes y se inserta en la base de datos.
    3. **Carga de la tabla 'hoteles'**: Se extrae información sobre hoteles, incluyendo competencia y valoración, y se asocia con su respectiva ciudad.
    4. **Carga de la tabla 'clientes'**: Se extraen clientes únicos con sus datos personales y se almacenan en la base de datos.
    5. **Carga de la tabla 'reservas'**: Se vinculan las reservas con clientes y hoteles y se insertan en la base de datos.

    Parámetros:
    ruta_importación_datos_limpios : str
        Ruta del archivo pickle con los datos de reservas previamente limpiados.
    ruta_entrada : str
        Ruta del archivo inicial en bruto que se usa para extraer las fechas de los eventos desde la API.
    ruta_guardar_eventos : str
        Ruta donde se almacenará el archivo de eventos extraído de la API.

    Excepciones:
        Si ocurre un error durante la inserción de datos en las tablas 'clientes' o 'reservas', 
        se captura y se imprime un mensaje de error específico.

    """
    #CARGO LOS DATOS DE LA TABLA CIUDAD
    df_reservas = pd.read_pickle(ruta_importación_datos_limpios) #Importo la información del df de reservas ya limpio

    insert_query = "INSERT INTO ciudad(nombre_ciudad) VALUES(%s);" #creo la query de inserción
    data_to_insert = (df_reservas.ciudad[0],) #creo los datos a insertar
    print(f'Se ha cargado los datos en la tabla ciudad: Se han cargado:{len(data_to_insert)} registros')

    #CARGO LOS DATOS DE LA TABLA EVENTOS:
    sc.insertar_datos_en_BBDD(insert_query,data_to_insert) #Subo la información a la BBDD

    df_eventos = pd.DataFrame(sa.eventos_api(ruta_entrada,ruta_guardar_eventos)) #Extraigo la información de la api y lo convierto en un df
    print(f'Se han extraido los eventos de la api y se ha guardado un fichero en la ruta {ruta_guardar_eventos}')

    query_extracción = "SELECT nombre_ciudad, id_ciudad FROM ciudad" #creo la query para extraer los datos del id_ciudad de la BBDD
    ciudad_dict = sc.extraer_datos_de_BBDD(query_extracción) #Extraigo el id de la ciudad de la BBDD
    

    #Creo los datos para insertarlos en la BBDD:
    data_to_insert=[]
    for _,row in df_eventos.iterrows():
        nombre_evento = row["nombre_evento"]
        url_evento = row["url_evento"]
        codigo_postal = row['codigo_postal']
        direccion = row['direccion']
        horario = row['horario']
        fecha_inicio = row['fecha_inicio']
        fecha_fin= row['fecha_fin']
        organizacion= row['organizacion']
        ciudad = row['ciudad'].capitalize()
        id_ciudad = ciudad_dict.get(ciudad)
        data_to_insert.append([nombre_evento, url_evento, codigo_postal, direccion, horario, fecha_inicio, fecha_fin, organizacion, id_ciudad])

    #Creo la query de inserción:
    insert_query = """
    INSERT INTO eventos(nombre_evento, url_evento, codigo_postal, direccion, horario, fecha_inicio, fecha_fin, organizacion, id_ciudad)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """ 
    sc.insertar_datos_en_BBDD(insert_query, data_to_insert) #Subo la información a la BBDD
    print(f'Se ha cargado los datos en la tabla eventos: Se han cargado:{len(data_to_insert)} registros')

    #CARGO LOS DATOS DE LA TABLA HOTELES:
    tabla_hoteles = df_reservas[['nombre_hotel', 'competencia', 'valoracion', 'ciudad']] #Creo la tabla hoteles

    #Creo los datos para insertarlos en la BBDD:
    data_to_insert=[]
    for _,row in tabla_hoteles.drop_duplicates().iterrows():
        nombre_hotel = row["nombre_hotel"]
        competencia = row["competencia"]
        valoracion = row['valoracion']
        ciudad = row['ciudad'].capitalize()
        id_ciudad = ciudad_dict.get(ciudad)
        data_to_insert.append([nombre_hotel, competencia, valoracion, id_ciudad])

    #Creo la query de inserción:
    insert_query = """
    INSERT INTO hoteles (nombre_hotel,competencia,valoracion,id_ciudad)
    VALUES (%s, %s, %s, %s)
    """
    sc.insertar_datos_en_BBDD(insert_query, data_to_insert) #Subo la información a la BBDD
    print(f'Se ha cargado los datos en la tabla hoteles: Se han cargado:{len(data_to_insert)} registros')

    #CARGO LOS DATOS DE LA TABLA CLIENTES:
    tabla_clientes = df_reservas[['id_cliente_unico', 'nombre', 'apellido', 'mail']] #Creo la tabla clientes

    #Creo los datos para insertarlos en la BBDD:
    data_to_insert=[]
    for _,row in  tabla_clientes.drop_duplicates().iterrows():
        id_cliente = row["id_cliente_unico"]
        nombre = row["nombre"]
        apellido = row['apellido']
        mail = row['mail']
        data_to_insert.append([id_cliente, nombre, apellido, mail])

    #Creo la query de inserción:
    insert_query = """
    INSERT INTO clientes (id_cliente, nombre, apellido, mail)
    VALUES (%s, %s, %s, %s)
    """
    try:
        sc.insertar_datos_en_BBDD(insert_query, data_to_insert) #Subo la información a la BBDD
        print(f'Se ha cargado los datos en la tabla clientes: Se han cargado:{len(data_to_insert)} registros')
    except Exception as e:
        print("No se pudo cargar los datos en la BBDD de la tabla clientes:", e)


    #CARGO LOS DATOS DE LA TABLA RESERVAS:
    tabla_reservas = df_reservas[['id_reserva', 'fecha_reserva', 'inicio_estancia', 'final_estancia', 'precio_noche', 'id_cliente_unico', 'nombre_hotel' ]] #Creo la tabla reservas:

    query_extracción="SELECT nombre_hotel, id_hotel FROM hoteles" #creo la query para extraer los datos del id_hotel de la BBDD
    hotel_dict=sc.extraer_datos_de_BBDD(query_extracción) #Extraigo el id de la ciudad de la BBDD

    #Creo los datos para insertarlos en la BBDD:
    data_to_insert=[]
    for _,row in tabla_reservas.drop_duplicates().iterrows():
        id_reserva = row["id_reserva"]
        fecha_reserva = row["fecha_reserva"]
        inicio_estancia = row['inicio_estancia']
        final_estancia = row['final_estancia']
        precio_noche = row['precio_noche']
        id_cliente = row['id_cliente_unico']
        nombre_hotel = row['nombre_hotel']
        id_hotel= hotel_dict.get(nombre_hotel)
        data_to_insert.append([id_reserva, fecha_reserva, inicio_estancia, final_estancia, precio_noche, id_cliente, id_hotel])

    #Creo la query de inserción:
    insert_query = """
    INSERT INTO reservas (id_reserva, fecha_reserva, inicio_estancia, final_estancia, precio_noche, id_cliente, id_hotel)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    try:
        sc.insertar_datos_en_BBDD(insert_query, data_to_insert) #Subo la información a la BBDD
        print(f'Se ha cargado los datos en la tabla reservas: Se han cargado:{len(data_to_insert)} registros')
    except Exception as e:
        print("No se pudo cargar los datos en la BBDD de la tabla reservas:", e)
    print(f'LA CARGA EN LA BBDD HA FINALIZADO')