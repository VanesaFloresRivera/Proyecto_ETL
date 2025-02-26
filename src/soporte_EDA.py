import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

def reporte (df_estudio):
    """
    Genera un reporte detallado sobre las columnas del DataFrame.

    Parámetros:
        df_estudio (pd.DataFrame): DataFrame a analizar.

    Retorno:
        pd.DataFrame: DataFrame con información sobre tipos de variables, conteo total, 
                      número de nulos, porcentaje de nulos, valores únicos y duplicados.
    """

    df_report = pd.DataFrame()

    df_report["tipo_variables"] = pd.DataFrame(df_estudio.dtypes)
    df_report["contador_total"] = pd.DataFrame(df_estudio.count ())
    df_report["numero_nulos"]=df_estudio.isnull().sum()
    df_report["porcentaje_nulos"] = round((df_estudio.isnull().sum()/df_estudio.shape[0])*100,2)
    df_report["valores_unicos"] = pd.DataFrame(df_estudio.nunique ())
    

    diccionario_duplicados = {}
    for indice in range (0, df_estudio.shape[1]):
        k= df_estudio.columns[indice]
        v= df_estudio.iloc[:,indice].duplicated().sum()
        diccionario_duplicados.update({k:v})
    
    serie_duplicados = pd.Series(diccionario_duplicados)

    df_report["duplicados"] = pd.DataFrame(serie_duplicados)
    


    print(f'La tabla tiene {df_estudio.shape[0]} filas y {df_estudio.shape[1]} columnas')

    print(f'La tabla tiene {df_estudio.duplicated().sum()} filas duplicadas ')

    return df_report


def reporte_1 (df_estudio):
    """
    Genera un reporte detallado sobre las columnas del DataFrame.

    Parámetros:
        df_estudio (pd.DataFrame): DataFrame a analizar.

    Retorno:
        pd.DataFrame: DataFrame con información sobre tipos de variables, conteo total, 
                      número de nulos, porcentaje de nulos, valores únicos y duplicados.
    """

    df_report = pd.DataFrame()

    df_report["tipo_variables"] = pd.DataFrame(df_estudio.dtypes)
    df_report["contador_total"] = pd.DataFrame(df_estudio.count ())
    df_report["numero_nulos"]=df_estudio.isnull().sum()
    df_report["porcentaje_nulos"] = round((df_estudio.isnull().sum()/df_estudio.shape[0])*100,2)
    df_report["valores_unicos"] = pd.DataFrame(df_estudio.nunique ())
    

    diccionario_duplicados = {}
    for indice in range (0, df_estudio.shape[1]):
        k= df_estudio.columns[indice]
        v= df_estudio.iloc[:,indice].duplicated().sum()
        diccionario_duplicados.update({k:v})
    
    serie_duplicados = pd.Series(diccionario_duplicados)

    df_report["duplicados"] = pd.DataFrame(serie_duplicados)
    

    return df_report


#Creación función para el análisis de las variables categóricas:
def analisis_descriptivos_categóricas (df_estudio):
    """Analiza las variables categóricas de un DataFrame.

    Parámetros:
        df_estudio (pd.DataFrame): DataFrame a analizar.

    Retorno:
        pd.DataFrame: DataFrame con estadísticas descriptivas de las columnas categóricas"""
    
    df_categóricas = df_estudio.select_dtypes(include = ["object"])
    print(f'Las columnas categóricas son {df_categóricas.columns}')
    print(f'Algunos ejemplos de filas son:')
    display(df_categóricas.sample(5))
    df_estudio_categóricas = df_estudio.describe(include = "object").T
    print(f'Las características de estas columnas son:')
    return df_estudio_categóricas



#Creación función para el análisis de las variables no categóricas:
def analisis_descriptivos_numéricas(df_estudio):
    """
    Analiza las variables no categóricas de un DataFrame.

    Parámetros:
        df_estudio (pd.DataFrame): DataFrame a analizar.

    Retorno:
        pd.DataFrame: DataFrame con estadísticas descriptivas de las columnas numéricas.
    """

    df_no_categoricas = df_estudio.select_dtypes(exclude = "object")      
    print(f'Las columnas no categóricas son {df_no_categoricas.columns}')
    print(f'Algunos ejemplos de filas son:')
    display(df_no_categoricas.sample(5))
    df_estudio_numéricas = np.round(df_no_categoricas.describe().T,2)
    print(f'Las características de estas columnas son:')
    return df_estudio_numéricas



def analisis_individual_columnas(df_estudio, columna_analisis):
    """
    Realiza un análisis detallado de una columna específica.

    Parámetros:
        df_estudio (pd.DataFrame): DataFrame a analizar.
        columna_analisis (str): Nombre de la columna a analizar.

    Retorno:
        None. Muestra detalles sobre la columna seleccionada.
    """

    df_columna = pd.DataFrame(df_estudio[columna_analisis].value_counts())
    print(f'La categoría {columna_analisis} tiene {df_columna.shape[0]} elementos diferentes: \n')

    print(f'Los elementos de la categoría son:')
    display(df_estudio[columna_analisis].unique())

    df_columna["Porcentaje_recuento"] = np.round(df_columna['count']/df_estudio.shape[0]*100,2)
    print(f'Los 10 {columna_analisis} que MAS aparecen son:')
    display(df_columna.head(10))

    print(f'Los 10 {columna_analisis} que MENOS aparecen son:')
    display(df_columna.tail(10))

    df_columna_contador_columnas = pd.DataFrame(df_columna['count'].value_counts())
    df_columna_contador_columnas['% repetición'] =df_columna_contador_columnas['count']/df_columna.shape[0]*100
    print(f' Las distribución de las repeticiones de los {columna_analisis} son:')
    display(df_columna_contador_columnas)


