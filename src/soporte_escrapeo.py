from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time
from datetime import datetime

def escrapeo(url,ruta_escrapeo):
    """
    Realiza un proceso de web scraping utilizando Selenium para obtener información sobre hoteles desde una página web.
    Extrae datos como el nombre del hotel, la puntuación de usuario, el precio por noche y la fecha de scraping, y 
    guarda los resultados en un archivo en formato pickle.

    Parámetros:
        url (str): La URL de la página web desde donde se extraerán los datos de los hoteles.
        ruta_escrapeo (str): La ruta donde se guardará el archivo pickle con los datos extraídos.

    Retorna:
        pd.DataFrame: Un DataFrame con la siguiente información:
            - 'nombre_hotel' (str): Nombre del hotel.
            - 'rating' (float): Puntuación del hotel según los usuarios.
            - 'precio_noche' (str): Precio por noche del hotel.
            - 'fecha_escrapeo' (datetime.date): Fecha en la que se realizó el scraping.
            - 'ciudad' (str): Ciudad en la que se encuentran los hoteles (por defecto, 'Madrid').

    Excepciones:
        - Si no se pueden encontrar los elementos de la página, la función imprimirá un mensaje de error y volverá a 
          intentar la ejecución recursivamente.
        - Puede fallar si la estructura de la página cambia o si el chromedriver no está configurado correctamente.

    Nota:
        Este scraping depende de la estructura HTML específica de la página web y de las clases CSS de los elementos,
        por lo que puede requerir ajustes si la web cambia su diseño.
    """

    # Configuración de Selenium (ajusta el path del driver según tu sistema)
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Para que se ejecute en segundo plano
    service = Service("C:/Users/van-2/.wdm/drivers/chromedriver/win64/133.0.6943.98/chromedriver-win32/chromedriver.exe")  # Ajusta esto con tu path del chromedriver
    driver = webdriver.Chrome(service=service, options=chrome_options)

    #genero el diccionario con los datos necesarios:
    dictio_scrap = {"nombre_hotel": [],
                                "rating":[],
                                "precio_noche":[],
                                "fecha_escrapeo": datetime.now().date(),
                                "ciudad": 'Madrid'}
    # Cargar la página
    driver.get(url)
    time.sleep(10)

    # Esperar a que aparezcan todos los precios públicos
    try:
        clase_hotelblock__content = driver.find_elements(By.CLASS_NAME, "hotelblock__content")

        for hotel in clase_hotelblock__content:
            nombre_hotel=hotel.find_element(By.CLASS_NAME,"title__link").text.split("\n")[0].strip()
            rating=float(hotel.find_element(By.CLASS_NAME,"ratings__score").text.split("/")[0])
            precio_noche=hotel.find_element(By.CLASS_NAME, "rate-details__flex-container").text.split("\n")[-1].strip("€")
            dictio_scrap["nombre_hotel"].append(nombre_hotel)
            dictio_scrap["rating"].append(rating)
            dictio_scrap["precio_noche"].append(precio_noche)
    except Exception as e:
        print("No se pudo encontrar los elementos:", e)
        escrapeo(url)
    df=pd.DataFrame(dictio_scrap)
    df.to_pickle(ruta_escrapeo)
    driver.close()
    driver.quit()
    return df
