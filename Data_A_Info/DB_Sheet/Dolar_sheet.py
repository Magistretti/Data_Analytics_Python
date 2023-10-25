
import requests
import time
import pandas as pd
import json
from oauth2client.service_account import ServiceAccountCredentials
import gspread

scope= ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
ruta="curious-athlete-393417-45edffc04e53.json"
rutaJson='https://api.bluelytics.com.ar/v2/evolution.json'

def cargar(df):
    credenciales = ServiceAccountCredentials.from_json_keyfile_name(ruta, scope)
    cliente = gspread.authorize(credenciales)
    sheet= cliente.open("Valor Dolar").sheet1
    sheet.clear()
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')

    sheet.append_rows([df.columns.values.tolist()]+ df.values.tolist())
    print('Carga completa')

bandera = True

# Hacemos una petición HTTP a la API de bluelytics
response = requests.get(rutaJson, timeout=60)







# Si la petición fue exitosa, obtenemos la respuesta
if response.status_code == 200:
    # Convertimos la respuesta a un objeto JSON
    response_json = response.json()
    
    archivo = pd.DataFrame(response_json)
    archivo['date'] = pd.to_datetime(archivo['date'])

    
    # Crear un DataFrame con todas las fechas en el rango deseado
    date_range = pd.date_range(start=archivo['date'].min(), end=archivo['date'].max(), freq='D')
    all_dates_df = pd.DataFrame({'date': date_range})
    
    # Combinar los DataFrames utilizando merge_asof
    archivo = archivo.sort_values(by='date')
        
    if bandera == True:
        #archivo= pd.read_csv('C:/Users/gmartinez/Desktop/new/Informes/PRESUPUESTO/dolar/evolution.csv')
        archivo= archivo.loc[archivo['source']== 'Blue']
        archivo = archivo.drop(columns=['source'])
        df_new = pd.merge_asof(all_dates_df, archivo, on='date')

        cargar(df_new)
        
        
        
'''        
# Repetimos la petición cada segundo
while True:
    time.sleep(1)
    response = requests.get("https://api.bluelytics.com.ar/v2/latest")

    if response.status_code == 200:
        response_json = response.json()
        price = response_json["precio"]

        print(f"El valor del dólar blue es: {price}")'''