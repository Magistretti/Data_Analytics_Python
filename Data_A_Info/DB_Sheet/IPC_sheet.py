'''import requests
from bs4 import BeautifulSoup


url = 'http://apis.datos.gob.ar/series/api/series/?ids=103.1_I2N_2016_M_15,101.1_I2NG_2016_M_22,102.1_I2S_ABRI_M_18,102.1_I2B_ABRI_M_15,103.1_I2R_2016_M_18,103.1_I2E_2016_M_21&format=csv'
response = requests.get(url)

# Verifica si la solicitud fue exitosa (código de respuesta 200)
if response.status_code == 200:
    # Parsea el contenido HTML de la página con BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')
    # A partir de aquí, puedes buscar y extraer información de la página web
else:
    print('No se pudo acceder a la página web.')


# Supongamos que queremos extraer todos los enlaces (<a>) de la página
enlaces = soup.find_all('a')

# Imprime los enlaces encontrados
for enlace in enlaces:
    print(enlace.get('href'))
'''


import requests
import time
import pandas as pd
import json
from oauth2client.service_account import ServiceAccountCredentials
import gspread

scope= ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
rutaJson='https://api.bluelytics.com.ar/v2/evolution.json'
ruta2="curious-athlete-393417-45edffc04e53.json"


def calculoIpc(df):
    r=1
    for r in df.index:
        if r >= 1:
            df.loc[r,'IPC %'] = (df.loc[r, 'ipc_2016_nivel_general'] / df.loc[r - 1, 'ipc_2016_nivel_general'])-1
        else:
            df['IPC %'] = None

    return df


def cargar(df):
    credenciales = ServiceAccountCredentials.from_json_keyfile_name(ruta2, scope)
    cliente = gspread.authorize(credenciales)
    sheet= cliente.open("Valor Dolar").get_worksheet_by_id(805299893)
    sheet.clear()
    archivo['indice_tiempo'] = pd.to_datetime(archivo['indice_tiempo'])
    df['indice_tiempo'] = df['indice_tiempo'].dt.strftime('%Y-%m-%d')

    sheet.append_rows([df.columns.values.tolist()]+ df.values.tolist())
    print('Carga completa')

bandera = True


ruta="http://apis.datos.gob.ar/series/api/series/?ids=103.1_I2N_2016_M_15,101.1_I2NG_2016_M_22,102.1_I2S_ABRI_M_18,102.1_I2B_ABRI_M_15,103.1_I2R_2016_M_18,103.1_I2E_2016_M_21&format=csv"

# Hacemos una petición HTTP a la API de bluelytics
response = requests.get(ruta, timeout=60)
#print(response.content)



from io import StringIO

# Si la petición fue exitosa, obtenemos la respuesta
if response.status_code == 200:
    
    # Utiliza StringIO para convertir la cadena en un archivo "virtual"
    csv_file = StringIO(response.text)

    # Luego, usa pd.read_csv() para leer el archivo virtual
    archivo = pd.read_csv(csv_file)

    '''archivo = pd.read_html(response)
    '''

    
    '''# Crear un DataFrame con todas las fechas en el rango deseado
    date_range = pd.date_range(start=archivo['indice_tiempo'].min(), end=archivo['indice_tiempo'].max(), freq='D')
    all_dates_df = pd.DataFrame({'date': date_range})'''
    
    # Combinar los DataFrames utilizando merge_asof
    archivo = archivo.sort_values(by='indice_tiempo')
    archivo= calculoIpc(archivo)
    print(archivo)

        
    if bandera == True:
        cargar(archivo)
