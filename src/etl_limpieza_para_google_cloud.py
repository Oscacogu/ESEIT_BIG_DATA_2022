  # Pseudo codigo
 # 1. leer archivo .csv
 # 2. eliminar duplicados
 # 3. reemplazar en la columna UNIDAD los nulos por SIN_DATO
 # 4. convertir campo 'FECHA_INICIO_DESPLAZAMIENTO_MOVIL'a datetime
 # 5. corregir campo 'RECEPCION', crenado 'RECEPCION CORREGIDA'
 # 6. reemplazar SIN_DATO por un valor nulo de tipo numerico en el campo 'EDAD'
 # 7. limpiar el campo 'LOCALIDAD' 
 # 8. convierte todo el campo 'GENERO' a mayusculas
 # 9. corregir el campo 'PRIORIDAD'   
 # 10. guardar el resumen de formato .csv


from fileinput import filename
import pandas as pd
import os
from pathlib import Path
from dateutil.parser import parse
import numpy as np
from google.cloud import storage
import google.cloud.storage
#import json



def main():

    #leer archivo
    data = get_data(folder = "data")
    #eliminar duplicados
    df_sin_duplicados = elimina_duplicados(data)
    #reemplaza nulos por SIN_DATO
    df_UNIDAD_ok = reemplaza_nulos(df_sin_duplicados)
    #'FECHA_INICIO_DESPLAZAMIENTO_MOVIL' a formato datetime 
    df_FECHA_INICIO_ok = formatea_dt(df_UNIDAD_ok)
    #crea 'RECEPCION CORREGIDA'
    df_RECEPCION_ok = corregir_recepcion(df_FECHA_INICIO_ok)
    #corrige campo 'EDAD'
    df_EDAD_ok = corregir_edad(df_RECEPCION_ok)
    #corregir 'LOCALIDAD'
    df_final = corregir_localidad(df_EDAD_ok)
    #corregir 'GENERO'
    df_def = corregir_genero(df_final)
    #corregir 'PRIORIDAD'
    df_def = corregir_prioridad(df_def)
    # guarde el resumen
    save_data(df_def, filename = "llamadas123_processed.csv")

def save_data(df_def, filename):
    bucket = 'gs://oscorrea_big_data'
    out_name = "resumen_" + filename
    root_dir = Path(".").resolve()
    out_path = os.path.join(bucket, 'data', 'processed', out_name)
    df_def.to_csv(out_path)
    
def corregir_prioridad(df_def):
    df_def['PRIORIDAD'] = df_def['PRIORIDAD'].replace(['CRITCA'], 'CRITICA')
    return df_def
    
def corregir_genero(df_final):
    df_final['GENERO'] = df_final['GENERO'].str.upper()
    df_def = df_final
    return df_def

def corregir_localidad(df):
    Localidades = {1:'USAQUEN', 2:'CHAPINERO', 3:'SANTA FE', 4:'SAN CRISTOBAL',  
    5:'USME', 6:'TUNJUELITO', 7:'BOSA', 8: 'KENNEDY', 9: 'FONTIBON', 10: 'ENGATIVA', 11: 'SUBA', 12: 'BARRIOS UNIDOS',
    13: 'TEUSAQUILLO', 14: 'LOS MARTIRES', 15: 'ANTONIO NARIÑO', 16: 'PUENTE ARANDA', 17: 'LA CANDELARIA', 18: 'RAFAEL URIBE URIBE',
    19: 'CIUDAD BOLIVAR', 20: 'SUMAPAZ'}

    df['LOCALIDAD'] = df['CODIGO_LOCALIDAD'].map(Localidades)

    df_final = df
    
    return df_final     

def corregir_edad(df):
    df['EDAD']=df['EDAD'].fillna('SIN_DATO')
    #f = lambda x: x if pd.isna(x) == True else int(x)
    #df['EDAD']=df['EDAD'].apply(f)
    df_EDAD_ok = df
    return df_EDAD_ok

def corregir_recepcion(df):
    def convertir_formato_fecha(str_fecha):
        val_datetime = parse(str_fecha, dayfirst=False, yearfirst = False)
        return val_datetime

    list_fecha = list()
    n_fila = df.shape[0] 

    for i in range(0,n_fila):

        str_fecha = df['RECEPCION'][i]

        try:
            val_datetime = convertir_formato_fecha(str_fecha=str_fecha)
            list_fecha.append(val_datetime)
        except Exception:
            list_fecha.append(str_fecha)
            continue
    df['RECEPCION_CORREGIDA'] = list_fecha
    df['RECEPCION_CORREGIDA']= pd.to_datetime(df['RECEPCION_CORREGIDA'], errors='coerce')
    df_RECEPCION_ok = df    
    return df_RECEPCION_ok     

def formatea_dt(df):
    col = 'FECHA_INICIO_DESPLAZAMIENTO_MOVIL'
    df[col]=pd.to_datetime(df[col], errors='coerce')
    df_FECHA_INICIO_ok = df
    return df_FECHA_INICIO_ok   

def  reemplaza_nulos(df):
    df['UNIDAD'] = df['UNIDAD'].fillna('SIN_DATO')
    df_UNIDAD_ok = df
    return df_UNIDAD_ok

def elimina_duplicados(data):
    df_sin_duplicados = data.drop_duplicates()
    df_sin_duplicados = df_sin_duplicados.reset_index() 
    return df_sin_duplicados    

def get_data(folder):
    data = list()
    client = storage.Client()
    bucket=storage.Bucket(client, 'oscorrea_big_data')
    for blob in client.list_blobs('oscorrea_big_data', prefix='data/raw/'):
        file_path=os.path.join('gs://oscorrea_big_data', blob.name)
        #print(str(blob.name))
        try:
            data_f = pd.read_csv(file_path, encoding = 'latin-1', sep = ';')
            data.append(data_f)
        except Exception:
            pass
    data = pd.concat(data, ignore_index= True)
    return data   


if __name__ == '__main__':
    main() 