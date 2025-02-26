import pandas as pd
import requests as rq
import numpy as np
import pyarrow
import os
import sys #permite navegar por el sistema
sys.path.append("../") #solo aplica al soporte
import src.soporte_api as sa
import src.soporte_limpieza as sl
import src.soporte_escrapeo as se
import src.soporte_carga_BBDD as sc
import psycopg2
from dotenv import load_dotenv

load_dotenv()

ARCHIVO_RAW= os.getenv("ARCHIVO_RAW")
ARCHIVO_SALIDA = os.getenv("ARCHIVO_SALIDA")
URL_ESCRAPEO = os.getenv("URL_ESCRAPEO")
RUTA_SERVICE = os.getenv("RUTA_SERVICE")
ARCHIVO_GUARDAR_ESCRAPEO= os.getenv("ARCHIVO_GUARDAR_ESCRAPEO")
URL_API = os.getenv("URL_API")
ARCHIVO_GUARDAR_EVENTOS_API = os.getenv("ARCHIVO_GUARDAR_EVENTOS_API")
DB_NAME = os.getenv("DB_NAME")
DB_USER=os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST= os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

sl.limpieza_datos(ARCHIVO_RAW, ARCHIVO_SALIDA,URL_ESCRAPEO,RUTA_SERVICE,ARCHIVO_GUARDAR_ESCRAPEO) #Limpia los datos
sc.carga_completa_datos(ARCHIVO_SALIDA, ARCHIVO_RAW, ARCHIVO_GUARDAR_EVENTOS_API, URL_API, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT) #Carga los datos en la BBDD