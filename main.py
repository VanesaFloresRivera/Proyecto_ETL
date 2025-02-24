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

ruta_entrada = 'data/reservas_hoteles.parquet'
ruta_salida = 'data/datos_parquet_limpios.pkl'
ruta_guardar_escrapeo = 'data/scrapeo.pkl'
ruta_guardar_eventos = 'data/eventos_api.pkl'
sl.limpieza_datos(ruta_entrada, ruta_salida,ruta_guardar_escrapeo)
sc.carga_completa_datos(ruta_salida,ruta_entrada,ruta_guardar_eventos)