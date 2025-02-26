import pandas as pd
import requests
import numpy as np
import pyarrow
from datetime import datetime
import sys #permite navegar por el sistema
sys.path.append("../") #solo aplica al soporte
import src.soporte_limpieza as sl
import os
from dotenv import load_dotenv

load_dotenv()

def eventos_api (ruta_entrada,ruta_guardar_eventos,url_api):
    """
    Obtiene información de eventos en Madrid desde una API pública, filtra los eventos según criterios específicos
    de fecha y recurrencia, y guarda los resultados en un archivo pickle.

    Parámetros:
        ruta_entrada (str): Ruta del archivo Parquet que contiene información sobre reservas de hoteles.
                            Se usa para determinar el rango de fechas de los eventos.
        ruta_guardar_eventos (str): Ruta donde se guardará el DataFrame con los eventos filtrados en formato pickle.
        url (str, opcional): URL del endpoint de la API de eventos. Por defecto, usa la API oficial de eventos de Madrid.

    Retorna:
        dict: Un diccionario con los eventos que cumplen los siguientes criterios:
            - Ocurren dentro del rango de fechas determinado por la estancia en el archivo Parquet.
            - Si son recurrentes, incluyen sábados o domingos.
            - Si no son recurrentes, ocurren el 01/03/2025 o el 02/03/2025.
            - No tienen los días 1 y 2 de marzo de 2025 excluidos.

        El diccionario contiene las siguientes claves:
            - 'id_evento': Identificador del evento.
            - 'nombre_evento': Nombre del evento.
            - 'url_evento': URL con más información sobre el evento.
            - 'codigo_postal': Código postal donde se realiza el evento.
            - 'direccion': Dirección del evento.
            - 'horario': Horario del evento.
            - 'fecha_inicio': Fecha de inicio del evento.
            - 'fecha_fin': Fecha de finalización del evento.
            - 'organizacion': Entidad organizadora del evento.
            - 'ciudad': Ciudad donde se realiza el evento.

    Excepciones:
        - Si la API no responde con un código 200, la función vuelve a intentar la solicitud.
        - Si faltan datos en el evento, se manejan con valores por defecto.

    Nota:
        Los datos extraídos se guardan en la ruta especificada en `ruta_guardar_eventos` en formato pickle.
    """
    #Obtengo la respuesta del end point:
    respuesta = requests.get(url_api)
    if respuesta.status_code == 200:
        data = respuesta.json() #convierto la respuesta en json

        df_inicial=pd.read_parquet(ruta_entrada)
        sl.tran_data(['inicio_estancia'], df_inicial)
        fecha_inicio = df_inicial.inicio_estancia[0]
        fecha_fin = fecha_inicio+pd.Timedelta(days=2)

        dict_eventos={'id_evento' : [],
        'nombre_evento':  [],
        'url_evento' :  [],
        'codigo_postal':  [],
        'direccion':  [],
        'horario':  [],
        'fecha_inicio':  [],
        'fecha_fin':  [],
        'organizacion':  [],
        'ciudad':  []
        }
        for evento in data.get("@graph"):
            fecha_inicio_evento= datetime.strptime(evento.get("dtstart"),"%Y-%m-%d %H:%M:%S.%f")
            fecha_fin_evento= datetime.strptime(evento.get("dtend"),"%Y-%m-%d %H:%M:%S.%f")
            if fecha_inicio_evento<=fecha_fin and fecha_fin_evento>=fecha_inicio: #para quedarme solo con los eventos que están dentro del rango de la fecha indicada
                try:
                    recurrencia_evento_dias = evento.get("recurrence").get("days")
                except:
                    recurrencia_evento_dias= 'Sin recurrencia'
                if ('SA' in recurrencia_evento_dias or 'SU' in recurrencia_evento_dias) or (recurrencia_evento_dias== 'Sin recurrencia' and fecha_inicio_evento>=fecha_inicio): #para quedarme solo con los eventos que: 1. cuando tienen recurrencia, se repiten en sabado y domingo (porque el 01/03 cae en sabado) y 2. de los que no son recurrentes, quedarme solo con los que son del día 01/03 y 02/03.
                    try:
                        dias_excluidos = evento.get("excluded-days")
                    except:
                        dias_excluidos= None
                    if '1/3/2025' not in dias_excluidos and '2/3/2025' not in dias_excluidos: #para quedarme solo con los eventos en los que el día 1 y 2 de marzo no están excluidos.
                        id_evento = evento.get("id")
                        dict_eventos['id_evento'].append(id_evento)
                        titulo_evento = evento.get("title")
                        dict_eventos['nombre_evento'].append(titulo_evento)
                        url_evento = evento.get("link")
                        dict_eventos['url_evento'].append(url_evento)
                        try:
                            codigo_postal_evento = evento.get("address").get("area").get("postal-code")
                        except:
                            codigo_postal_evento= 'Sin información'
                        dict_eventos['codigo_postal'].append(codigo_postal_evento)
                        try: 
                            dirección_evento=evento.get("address").get("area").get("street-address")
                        except:
                            dirección_evento = 'Sin información'
                        dict_eventos['direccion'].append(dirección_evento)
                        hora_evento = evento.get("time")
                        if hora_evento=='':
                            hora_evento='Sin información'
                        else:
                            hora_evento=hora_evento
                        dict_eventos['horario'].append(hora_evento)
                        dict_eventos['fecha_inicio'].append(fecha_inicio_evento)
                        dict_eventos['fecha_fin'].append(fecha_fin_evento)
                        try:
                            organizacion_evento = evento.get("organization").get("organization-name")
                        except:
                            organizacion_evento='Sin información'
                        dict_eventos['organizacion'].append(organizacion_evento)
                        try:
                            localidad_evento= evento.get("address").get("area").get("locality").split()[0]
                        except:
                            localidad_evento= 'MADRID'
                        dict_eventos['ciudad'].append(localidad_evento)
        df_eventos_api_final = pd.DataFrame(dict_eventos)
        df_eventos_api_final.to_pickle(ruta_guardar_eventos)
    else:
        print("La respuesta no ha sido 200, se vuelve a intentar")
        eventos_api()
    return dict_eventos