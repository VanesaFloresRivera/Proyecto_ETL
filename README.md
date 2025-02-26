# Proyecto_ETL

# Proyecto ETL: Análisis de Reservas Hoteleras en Madrid

## Descripción del Proyecto
Nuestra empresa, dedicada al sector hotelero en Madrid, ha recibido un archivo en formato Parquet con información sobre reservas de hoteles propios y de la competencia. El objetivo de este proyecto es extraer, transformar y cargar (ETL) estos datos para generar insights relevantes.

## Requisitos del Proyecto

### 1. Extracción de Datos
- Leer el archivo `reservas_hoteles.parquet` para obtener información sobre hoteles de la competencia.
- Realizar web scraping de una página de hoteles dada.

### 2. Obtención de Datos de Eventos
- Consultar la API del Ayuntamiento de Madrid para obtener eventos en la ciudad durante las fechas de las reservas.

### 3. Transformación de Datos
- Limpiar y estructurar los datos extraídos de la web para que sean comparables con los datos del archivo original.
- Enriquecer los datos de reservas con información de eventos en Madrid en las mismas fechas.

### 4. Carga de Datos
- Almacenar los datos transformados en una base de datos SQL.

## Estructura de la Base de Datos
Se ha utilizado PostgreSQL para almacenar los datos en las siguientes tablas:
```sql
CREATE TABLE ciudad (
    id_ciudad SERIAL PRIMARY KEY,
    nombre_ciudad TEXT
);

CREATE TABLE eventos (
    id_evento SERIAL PRIMARY KEY,
    nombre_evento TEXT,
    url_evento TEXT,
    codigo_postal TEXT,
    direccion TEXT,
    horario TEXT,
    fecha_inicio DATE,
    fecha_fin DATE,
    organizacion TEXT,
    id_ciudad INT REFERENCES ciudad(id_ciudad) ON DELETE CASCADE
);

CREATE TABLE hoteles (
    id_hotel SERIAL PRIMARY KEY,
    nombre_hotel TEXT,
    competencia BOOL,
    valoracion FLOAT CHECK (valoracion BETWEEN 1 AND 5),
    id_ciudad INT REFERENCES ciudad(id_ciudad) ON DELETE CASCADE
);

CREATE TABLE clientes (
    id_cliente VARCHAR(50) PRIMARY KEY,
    nombre TEXT,
    apellido TEXT,
    mail TEXT UNIQUE CHECK (mail LIKE '%@%')
);

CREATE TABLE reservas (
    id_reserva VARCHAR(50) PRIMARY KEY,
    fecha_reserva DATE,
    inicio_estancia DATE,
    final_estancia DATE,
    precio_noche FLOAT CHECK (precio_noche >= 0),
    id_cliente VARCHAR(50) REFERENCES clientes(id_cliente) ON DELETE CASCADE,
    id_hotel INT REFERENCES hoteles(id_hotel) ON DELETE CASCADE
);
```

## Proceso ETL

### 1. Extracción y transformación
#### Archivo Parquet
- Se limpiaron los datos eliminando duplicados y asegurando la integridad de las columnas.
- Se transformaron las fechas al formato correcto.
- Se generaron IDs únicos para los clientes, corrigiendo inconsistencias en IDs y correos.
- Se corrigieron IDs de hoteles y nombres, asegurando una relación uno a uno.
- Se calculó la valoración media de cada hotel y se hizo un estudio de si existían valores outliers.
- Se imputaron a los valores nulos en los precios de las reservas el el promedio del precio por hotel y día.
- Se calculó la valoración de cada hotel propio en función al promedio de estrellas que tenía cada uno

#### Web Scraping
- Se intentó extraer información con BeautifulSoup, pero debido a la carga dinámica de precios, se utilizó Selenium.
- Se extrajo nombre del hotel, valoración, precio por noche.
- Se la asignó a la fecha de reserva la fecha en la que se realizó el escrapeo.
- Se le asignó a la ciudad el valor de Madrid.
- Los datos extraídos se guardaron en un archivo pickle.

### Continuación transformación en Archivo Parquet
- Se asignaron IDs de hotel aleatoriamente a los datos del scraping para unirlos con el dataset original.
- Se incorporó esta información al archivo original para tener toda la información en un mismo fichero.
- Se eliminó las columnas que no eran necesarias.
- Los datos transformados se guardadron en un archivo pickle para usarlos posteriormente en la carga a la BBDD.

#### API de Eventos de Madrid
- La información de los eventos fue extraída de la **API de eventos de Madrid**, aplicando los siguientes filtros:
  - **Eventos que empezaran antes de la fecha fin de la estancia**, incluyendo ese día (02/03 a las 23:59:59).
  - **Eventos que finalizaran después de la fecha inicio de la estancia**, es decir, después del 01/03 a las 00:00:00.
  - Para los **eventos recurrentes**, se incluyeron solo los eventos que se repitieran en los días relevantes (01/03 y 02/03, sábado y domingo).
  - Si estos días coincidían con los **días excluidos del evento**, no se tuvieron en cuenta.
  - Resultado: El 25/02/2025 se extrajeron **161 eventos**.

### 2. Carga en la Base de Datos

#### 1. Funciones de Soporte

Se crearon dos funciones de soporte para facilitar la carga de datos en la base de datos:

##### 1.1. **Función de Carga de Datos**
Esta función se encarga de:
- Crear la **conexión** y el **cursor**.
- Ejecutar la **query de inserción**.
- Recibir los siguientes parámetros:  query de inserción y los **datos a insertar**.
- Dependiendo de si los datos a insertar son una lista o no, ejecutar una subida o varias.
- Guardar los cambios para completar la subida.
- Cerrar el **cursor** y la **conexión**.

##### 1.2. **Función de Extracción de Datos**
Esta función realiza lo siguiente:
- Crear la **conexión** y el **cursor**.
- Ejecutar la **query de extracción**.
- Recibir la **query de extracción** y devolver los resultados como un **diccionario**.
- Cerrar el **cursor** y la **conexión** al finalizar.

---

#### 2. Subida de Datos a la Base de Datos

Una vez creadas las funciones de soporte, procedí a cargar los datos en cada una de las tablas de la base de datos en el siguiente orden:

##### 2.1. **Importación del DataFrame Limpio**
Primero, se importó el **DataFrame limpio**, que fue la base para obtener los datos correctos para la mayoría de las tablas.

##### 2.2. **Subida de Datos a las tablas**

###### 2.2.1. **Tabla Ciudad - Sin clave foránea**
- Se extrajo la información de la **tabla ciudad** desde el **DataFrame limpio**.
- Se construyó un **data_to_insert** con un valor único, subiendo solo el campo **ciudad**.
- El **id_ciudad** es de tipo **serial** en la base de datos, por lo que no fue necesario incluirlo.

###### 2.2.2. **Tabla Eventos - Con clave foránea**
- Para la **tabla eventos**, era necesario obtener el **id_ciudad**, por lo que primero se extrajo de la base de datos utilizando la función de extracción.
- La información de los eventos fue extraída de la **API de eventos de Madrid**, según el proceso indicado en la extracción de la API.
- Se construyó el **data_to_insert** combinando la información de la API con el **id_ciudad** extraído de la base de datos.
- Se creó la **query de inserción** y se procedió a la **subida de los datos** a la base de datos utilizando la función de inserción.
- El **id_evento** se generó de forma **seriada** en la base de datos.

###### 2.2.3. **Tabla Hoteles - Con clave foránea**
- Se creó la **tabla hoteles** a partir del **DataFrame limpio**, manteniendo solo las columnas necesarias.
- Se eliminaron los **duplicados de nombre de hoteles** y se añadió el **id_ciudad** extraído de la base de datos para asociarlo correctamente.
- Se creó el **data_to_insert** y la **query de inserción**.
- Se procedió a la **subida de los datos** utilizando la función de inserción.
- El **id_hotel** se generó de forma **seriada** en la base de datos.

###### 2.2.4. **Tabla Clientes - Sin clave foránea**
- Se creó la **tabla clientes** utilizando el **DataFrame limpio**, quedándose solo con las columnas necesarias.
- Se eliminaron los **duplicados de id_clientes**.
- Se creó la **query de inserción** y se procedió a la **subida de los datos**.
- En este caso, el **id_cliente** se subió ya que no era de tipo **serial** en la base de datos.

###### 2.2.5. **Tabla Reservas - Con clave foránea**
- Para la **tabla reservas**, se necesitaban tanto el **id_hotel** como el **id_cliente**. Por lo tanto, se extrajo primero el **id_hotel** de la base de datos.
- Se extrajo el resto de la información del **DataFrame limpio** y se incorporaron tanto el **id_ciudad** como el **id_hotel** extraídos previamente de la base de datos.
- Se creó la **query de inserción** y se procedió a la **subida de los datos**.
- El **id_reserva** se subió ya que no era de tipo **serial** en la base de datos.


## 3. Análisis y Consultas
## Consultas SQL
- Número total de hoteles: `29`
- Número total de reservas: `15000`
- Top 10 clientes que más gastaron:
    - Ceferino Sosa, Leandra Castañeda, Modesta Heras, Clarisa Coll, Abigaíl Ayala. Domingo Zabaleta, Leoncio Robledo, Consuela Folch, Samuel Arco, Ángeles Nuñez.
- Hotel con más recaudación:
    - **Competencia**: Novotel Madrid Center
    - **Propios**: Hotel Monte Verde
- Número total de eventos: `161`
- Día con más reservas: `06/02/2025` con `872` reservas.

## 📊 Análisis Exploratorio de Datos (EDA)

### 🔍 Análisis de Precios entre la Competencia y Nuestros Hoteles  
Una vez creada la base de datos, se realizaron varias consultas para construir un **DataFrame con los datos relevantes** y analizar la diferencia de precios entre **nuestros hoteles** y los **hoteles de la competencia**.  

#### 📈 **Hallazgos Principales**  
- 🏨 **Distribución de hoteles**:  
  - El **65,5%** de los hoteles son **propios**, mientras que el **34,5%** pertenece a la **competencia**.  
  - Esta misma proporción se mantiene en el número de **reservas realizadas**.  

- 💰 **Ingresos totales**:  
  - Aunque el número de hoteles y reservas es similar, la recaudación es **mayor en nuestros hoteles**.  
  - **Nuestros hoteles generan el 77,3% de los ingresos**, mientras que la competencia solo **22,7%**.  

- 💵 **Diferencia en precios**:  
  - El **precio promedio por reserva** en nuestros hoteles es **275€**, mientras que en los hoteles de la competencia es **153€**.  
  - Esto sugiere que **el precio por noche no afecta la cantidad de reservas**, ya que ambos grupos tienen una **media de 517 reservas por hotel**.  

- 📊 **Variabilidad en precios**:  
  - En los **hoteles de la competencia**, cada hotel tiene un **precio único** de reserva.  
  - En nuestros **hoteles propios**, los precios **varían entre reservas**, pero **sin valores atípicos**.  

---

### ⏳ Análisis Temporal de las Fechas de Reserva  

Se analizaron las fechas en las que se realizaron las reservas para identificar patrones temporales.  

#### 📆 **Hallazgos Principales**  
- 📅 **Todas las reservas se realizaron en febrero**.  
- 🏨 **Patrón de reservas por tipo de hotel**:  
  - Las **reservas en nuestros hoteles** están distribuidas en diferentes días.  
  - En los **hoteles de la competencia**, todas las reservas se realizaron **el día 25**.  
- 💸 **Ingresos diarios en nuestros hoteles**:  
  - El **importe total recaudado por día** es bastante **uniforme**.  
- 💰 **Evolución del precio medio por noche**:  
  - En nuestros hoteles, el precio medio ronda las **270 unidades monetarias** en todos los días.  
  - En los hoteles de la competencia, el precio medio se mantiene alrededor de **150 unidades monetarias**.  

Para más detalles ejecutar el fichero main_eda.py

## 🗂️ Estructura del Proyecto
│  
├── 📂 data/             *Datos crudos y procesados* 

├── 📂 notebooks/           *Notebooks de Jupyter con el análisis, visualizaciones y conclusiones*

├── 📂 Src/                *Scripts de procesamiento y modelado*

├── 📂 Venv/                 *Entorno virtual con dependencias instaladas*

├── 🐍 main.py               *Script principal para ejecutar el proceso ETL*

├── 🐍 main_eda.py           *Script para realizar el análisis exploratorio de datos*

├── 📄 requirements.txt      *Lista de dependencias necesarias para el proyecto*

├── 📄 README.md             # Documentación del proyecto y conclusiones  


## Requisitos de Instalación
Ejecutar:
```sh
pip install -r requirements.txt
```

## Ejecución del Proyecto
### Para ejecutar el proceso ETL:
```sh
python main.py
```
### Para ejecutar el proceso del Análisis exploratorio de los datos (EDA) posterior:
```sh
python main_eda.py
```
## 🛠️ Tecnologías Utilizadas  

Este proyecto ha sido desarrollado utilizando **Python** y las siguientes librerías:  

- **`pandas`**: Manejo y análisis de datos en estructuras tipo DataFrame.  
- **`numpy`**: Operaciones matemáticas y manejo de arrays.  
- **`requests`**: Conexión y extracción de datos desde APIs.  
- **`pyarrow`**: Soporte para formatos de datos optimizados.  
- **`psycopg2`**: Conexión y operaciones con bases de datos **PostgreSQL**.  
- **`dotenv`**: Manejo de variables de entorno.
- **`selenium`**: Automatización de navegación web para Web Scraping.
-**`beautifulsoup4`**: Extracción y procesamiento de datos desde HTML y XML.
- **`matplotlib`** y **`seaborn`**: Visualización de datos.  

Además, se han implementado módulos propios dentro de la carpeta `src/`, incluyendo:  
- **`soporte_api`**: Funciones para la extracción de datos desde la API del Ayuntamiento de Madrid.  
- **`soporte_limpieza`**: Procesamiento y limpieza de datos.  
- **`soporte_escrapeo`**: Funciones de Web Scraping.  
- **`soporte_carga_BBDD`**: Conexión y carga de datos en la base de datos.  
- **`soporte_EDA`**: Funciones para el análisis exploratorio de datos.  

## 📊 Otras tecnologías utilizadas:  
- **PostgreSQL**: Base de datos relacional para almacenar los datos procesados.  
- **APIs**: Extracción de datos en tiempo real desde fuentes oficiales, como el Ayuntamiento de Madrid.  
- **Entorno Virtual**: Se ha utilizado un entorno virtual para gestionar dependencias y asegurar la compatibilidad del código.- 

## Conclusiones
Este proyecto permitió estructurar los datos de reservas hoteleras, extrayendo datos relevantes de la competencia por Web Scraping, complementarlos con eventos de la ciudad mediante una extracción de una API y analizarlos para extraer insights valiosos sobre la relación entre ocupación hotelera y eventos en Madrid.

## 🔄 Próximos Pasos

   - Revisión del control de flujos y errores de la parte de la carga a la BBDD.


## 🤝 Contribuciones

   -  Las contribuciones son bienvenidas. Si deseas mejorar el proyecto, por favor abre un pull request o una issue, envía un correo a vanesafloresrivera87@gmail.com o contáctame través de [linkedin](https://www.linkedin.com/in/vanesa-flores-rivera/).


## ✒️ Autores

   - **Vanesa Flores Rivera**: [linkedin](https://www.linkedin.com/in/vanesa-flores-rivera/), [github](https://github.com/VanesaFloresRivera)


