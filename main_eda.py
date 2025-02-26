import pandas as pd
import numpy as np
import psycopg2
import os
from dotenv import load_dotenv
import sys #permite navegar por el sistema
sys.path.append("../") #solo aplica al soporte
import src.soporte_carga_BBDD as sc
import src.soporte_EDA as se
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import seaborn as sns

load_dotenv()

#Llamamos a la variable de entorno
DB_NAME = os.getenv("DB_NAME")
DB_USER=os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST= os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

#Creamos la conexión:
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

#Creamos el cursor:
cur= conn.cursor()

#Ejecuto la primera query para el análisis de los precios
query_extracción= "SELECT r.id_hotel, h.nombre_hotel, h.competencia, r.precio_noche FROM reservas r INNER JOIN hoteles h ON r.id_hotel =h.id_hotel "
cur.execute(query_extracción)
resultados = cur.fetchall()
df = pd.DataFrame(resultados, columns=["id_hotel", "nombre_hotel", "competencia", "precio_noche"])
cur.close() #cierro el cursor
conn.close() #cierro la conexión

#VISUALIZACIONES
#PRIMERA VISUALIZACIÓN Peso hoteles propios y competencia
#df que muestra la proporcion del num. de hoteles, reservas realizadas e importe recaudado
print("ANALISIS DE PRECIOS DE LA COMPETENCIA Y HOTELES PROPIOS ")
df_importes_agrupación_competencia_nombrehotel_3= df.groupby(['competencia', 'nombre_hotel'])['precio_noche'].sum().reset_index()
df_hoteles = df_importes_agrupación_competencia_nombrehotel_3.groupby('competencia')['precio_noche'].agg([ "sum", "count"]).reset_index()
df_hoteles['Peso_categoria']=round(df_hoteles.iloc[:,2]/df_hoteles.iloc[:,2].sum()*100,2)
df_hoteles['Peso_total_valor_recaudado']=round(df_hoteles.iloc[:,1]/df.precio_noche.sum()*100,2)

df_importes_agrupación_competencia_nombrehotel1= df.groupby('competencia')['precio_noche'].agg(["mean", "sum", "count"]).reset_index()
df_importes_agrupación_competencia_nombrehotel1['Peso_categoria']=round(df_importes_agrupación_competencia_nombrehotel1.iloc[:,3]/df_importes_agrupación_competencia_nombrehotel1.iloc[:,3].sum()*100,2)
df_importes_agrupación_competencia_nombrehotel1['Peso_total_valor_recaudado']=round(df_importes_agrupación_competencia_nombrehotel1.iloc[:,2]/df.precio_noche.sum()*100,2)

# Gráfico
fig, axes = plt.subplots(1, 3, sharex= True, sharey=False, figsize= (20,7))
fig.suptitle('Peso hoteles propios y competencia', fontsize=18)

axes[0].pie("Peso_categoria", labels='competencia', autopct='%1.1f%%', data=df_hoteles, colors=['darkgreen','violet'])
plt.xticks(rotation=90, fontsize=10)
axes[0].set_title('Num. hoteles', fontsize = 14)

axes[1].pie("Peso_categoria", labels='competencia', autopct='%1.1f%%', data=df_importes_agrupación_competencia_nombrehotel1, colors=['darkgreen','violet'])
plt.xticks(rotation=90, fontsize=10)
axes[1].set_title('Num. reservas realizadas', fontsize = 14)

axes[2].pie("Peso_total_valor_recaudado", labels='competencia', autopct='%1.1f%%', data=df_hoteles,colors=['darkgreen','violet'])
plt.xticks(rotation=90, fontsize=10)
axes[2].set_title('Importe valor recaudado total', fontsize = 14);

plt.show()

#SEGUNDA VISUALIZACIÓN Peso hoteles propios y competencia
#Obtención datos necesarios
df_importes_agrupación_competencia_nombrehotel= df.groupby(['competencia', 'nombre_hotel'])['precio_noche'].agg(["mean", "sum", "count"]).reset_index()
df_importes_agrupación_competencia_nombrehotel['Peso_categoria']=round(df_importes_agrupación_competencia_nombrehotel.iloc[:,4]/df_importes_agrupación_competencia_nombrehotel.iloc[:,4].sum()*100,2)
df_importes_agrupación_competencia_nombrehotel['Peso_total_valor_recaudado']=round(df_importes_agrupación_competencia_nombrehotel.iloc[:,3]/df.precio_noche.sum()*100,2)

#Gráfico
orden_hoteles = df_importes_agrupación_competencia_nombrehotel.nombre_hotel.values.tolist()
fig, axes = plt.subplots(1, 2, sharex= False, sharey=True, figsize= (20,7))
fig.suptitle('Peso del num. de reservas y valor recaudado por hoteles', fontsize=16)


sns.barplot(data=df_importes_agrupación_competencia_nombrehotel, x='nombre_hotel', y= 'Peso_categoria', hue='competencia',palette='cubehelix',ax=axes[0], order= orden_hoteles)
axes[0].tick_params(axis='x', rotation=90, labelsize=10)
axes[0].set_ylabel('%', fontsize = 12)
axes[0].set_xlabel('Hotel', fontsize = 12)
axes[0].set_title('Peso num. reservas ', fontsize = 14)
axes[0].spines['right'].set_visible(False)
axes[0].spines['top'].set_visible(False)
axes[0].legend().remove()


sns.barplot(data=df_importes_agrupación_competencia_nombrehotel, x='nombre_hotel', y= 'Peso_total_valor_recaudado', palette='cubehelix',ax=axes[1],  hue='competencia', order= orden_hoteles)
axes[1].tick_params(axis='x', rotation=90, labelsize=10)
axes[1].set_ylabel('%', fontsize = 12)
axes[1].set_xlabel('Hotel', fontsize = 12)
axes[1].set_title('Peso Importe Valor recaudado total', fontsize = 14)
axes[1].spines['right'].set_visible(False)
axes[1].spines['top'].set_visible(False)
axes[1].legend(title = 'Tipo de hotel', title_fontsize =10,bbox_to_anchor=(1.05,1), loc = "upper left");

plt.show()

#TERCERA VISUALIZACIÓN 'Dispersión'
#df necesario:
df_importes_agrupación_competencia_nombrehotel_4= df.groupby(['competencia', 'nombre_hotel'])['precio_noche'].count().reset_index()

#Gráfico
fig, axes = plt.subplots(3, 1, sharex= False, sharey=False, figsize= (20,30))

sns.boxplot(data=df_importes_agrupación_competencia_nombrehotel_4, x='competencia', y='precio_noche', hue='competencia', palette='cubehelix',ax=axes[0])
axes[0].set_title('Dispersión Num. reservas hoteles propios y de la competencia', fontsize = 14)
axes[0].spines['right'].set_visible(False)
axes[0].spines['top'].set_visible(False)
axes[0].spines['left'].set_visible(False)
axes[0].set_xlabel('', fontsize = 0)
axes[0].set_ylabel("Num. reservas", fontsize=8);

sns.boxplot(data=df, x='competencia', y='precio_noche', hue='competencia', palette='cubehelix',ax=axes[1])
axes[1].set_title('Dispersión precios hoteles propios y de la competencia', fontsize = 14)
axes[1].spines['right'].set_visible(False)
axes[1].spines['top'].set_visible(False)
axes[1].spines['left'].set_visible(False)
axes[1].set_xlabel('', fontsize = 0)
axes[1].set_ylabel("Precio noche", fontsize=8);

sns.boxplot(data=df, x='nombre_hotel', y='precio_noche', hue='competencia', palette='cubehelix',ax=axes[2])
axes[2].set_title('Dispersión precios noche por hotel', fontsize = 14)
axes[2].spines['right'].set_visible(False)
axes[2].spines['top'].set_visible(False)
axes[2].spines['left'].set_visible(False)
axes[2].set_xlabel('', fontsize = 0)
axes[2].tick_params(axis='x', rotation=90, labelsize=10)
axes[2].set_ylabel("Precio noche", fontsize=8)
axes[2].legend(title = 'Tipo de hotel', title_fontsize =10,bbox_to_anchor=(1.05,1), loc = "upper left");

plt.show()

print("""CONCLUSIONES PRIMER ANÁLISIS: PRECIOS DE LA COMPETENCIA Y HOTELES PROPIOS
- Existen más hoteles propios que de la competencia, en concreto, el 65,5% de los hoteles son propios frente al 34,5% de la competencia.
- Esta misma proporción se mantiene en el número de reservas realizadas
- No obstante, en relación al importe del valor recaudad total, la proporción varía. Los hoteles propios reacaudan más que los hoteles de la competencia. Los propios recaudan un 77,3% frente a un 22,7% de la competencia. 
- Esta variación en la proporción se debe a que los precios de los hoteles propios son superiores. La media del precio de las reservas de los hoteles propios está en 275€ mientras que la media de las de los hoteles de la competencias es 153€. 
- Por tanto, podemos concluir que el precio de la noche de los hoteles no influye en el número de reservas realizadas. De hecho, la media del numero de reservas tanto en los hoteles propios como en los hoteles de la competencia es prácticamente la misma, 517 reservas por hotel.
- Por último, indicar que, cada hotel de la competencia tiene un precio único de reserva, sin embargo, en los hoteles propios, los precios varían. No obstante, en ninguno de ellos existen valores atípicos""")
df_recuento = round(df_importes_agrupación_competencia_nombrehotel_4.groupby('competencia')['precio_noche'].agg([ "sum", "count","mean"]).reset_index(),2)

#CUARTA VISUALIZACION Evolución histórica por dia y tipo de hotel'
print("ANALISIS EVOLUCIÓN HISTÓRICA FECHA DE RESERVA ")

#Creamos la conexión:
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

#Creamos el cursor:
cur= conn.cursor()

#Extraigo los datos necesarios de la BBDD:
query_extracción= "SELECT h.nombre_hotel, h.competencia, r.fecha_reserva, count(h.nombre_hotel) , sum(r.precio_noche) , avg(r.precio_noche) FROM reservas r INNER JOIN hoteles h ON r.id_hotel =h.id_hotel GROUP BY h.id_hotel,r.fecha_reserva"
cur.execute(query_extracción)
resultados = cur.fetchall()
df_fecha_reserva = pd.DataFrame(resultados, columns=["nombre_hotel", "Tipo_hotel","fecha_reserva", "Num_reservas","Total_recaudado","Precio_medio_noche" ])
cur.close() #cierro el cursor
conn.close() #cierro la conexión

#Creo los DF necesarios
df_fecha_reserva['Tipo_hotel'] = df_fecha_reserva["Tipo_hotel"].replace({True: 'Competencia', False: 'Propios'})
df_fecha_reserva['Total_recaudado'] = round(df_fecha_reserva['Total_recaudado'],2)
df_fecha_reserva['Precio_medio_noche'] = round(df_fecha_reserva['Precio_medio_noche'],2)
df_fecha_reserva['fecha_reserva'] = pd.to_datetime(df_fecha_reserva['fecha_reserva'])
df_fecha_reserva['Dia_reserva'] = df_fecha_reserva['fecha_reserva'].dt.day
df_fecha_reserva['Mes_reserva'] = df_fecha_reserva['fecha_reserva'].dt.month
print("Todas las reservas se han realizado en el mes de febrero")

#Gráfico
fig, axes = plt.subplots(1, 3, sharex= True, sharey=False, figsize= (25,7))
fig.suptitle('EVOLUCION HISTORICA FECHA RESERVA POR DIA Y HOTEL', fontsize=18)

#CREO EL PRIMER GRAFICO
sns.barplot(x='Dia_reserva', y='Num_reservas', data=df_fecha_reserva, hue='Tipo_hotel', estimator='sum', ci=95, linestyle = "dashed", errorbar = None, ax=axes[0])
axes[0].set_title('Num. reservas', fontsize = 14)

# añadimos el método 'plt.xlabel()' para ponerle nombre al eje x
axes[0].set_xlabel("Dia del mes de febrero")

# añadimos el método 'plt.ylabel()' para ponerlo nombre al eje y
axes[0].set_ylabel("Num. reservas")

axes[0].spines['right'].set_visible(False)
axes[0].spines['top'].set_visible(False)
axes[0].tick_params(axis='x', rotation=90, labelsize=10)
axes[0].legend().remove()

#CREO EL SEGUNDO GRAFICO
sns.barplot(x='Dia_reserva', y='Total_recaudado', data=df_fecha_reserva, hue='Tipo_hotel', estimator='sum', ci=95, linestyle = "dashed", errorbar = None, ax=axes[1])
axes[1].set_title('Importe total recaudado', fontsize = 14)

# añadimos el método 'plt.xlabel()' para ponerle nombre al eje x
axes[1].set_xlabel("Dia del mes de febrero")

# añadimos el método 'plt.ylabel()' para ponerlo nombre al eje y
axes[1].set_ylabel("Unidades monetarias")

axes[1].spines['right'].set_visible(False)
axes[1].spines['top'].set_visible(False)
axes[1].tick_params(axis='x', rotation=90, labelsize=10)
axes[1].legend().remove()

#CREO EL TERCER GRAFICO
sns.barplot(x='Dia_reserva', y='Precio_medio_noche', data=df_fecha_reserva, hue='Tipo_hotel', estimator='mean', ci=95, linestyle = "dashed", errorbar = None ,ax=axes[2])
axes[2].set_title('Precio medio noche', fontsize = 14)

# añadimos el método 'plt.xlabel()' para ponerle nombre al eje x
axes[2].set_xlabel("Dia del mes de febrero")

# añadimos el método 'plt.ylabel()' para ponerlo nombre al eje y
axes[2].set_ylabel("Unidades monetarias")

axes[2].spines['right'].set_visible(False)
axes[2].spines['top'].set_visible(False)
axes[2].tick_params(axis='x', rotation=90, labelsize=10)
plt.legend(fontsize = 8, title = 'Tipo_hotel', title_fontsize =10,bbox_to_anchor=(1.05,1), loc = "upper left");

plt.show()

print("""CONCLUSIONES SEGUNDO ANALISIS: EVOLUCIÓN HISTÓRICA FECHAS DE RESERVA
- Todas las reservas se han realizado en febrero
- Las reservas de los hoteles propios se han realizado en diferentes dias, desde el día 1 al 12, pero las reservas de los hoteles de la competencia se han realizado todas el día 25.
- El importe total recaudado por dia, en los hoteles propios, es similar.
- El precio medio por noche en los hoteles de la competencia es similar en todos los días, entorno a 270 unidades monetarias, sin embargo, en los hoteles de la competencia, la media del precio es de 150 unidades monetarias.""")

print('El análisis EDA ha finalizado')