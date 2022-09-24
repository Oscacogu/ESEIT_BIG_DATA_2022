 # Pseudo codigo
 # 1. leer archivo .csv
 # 2. eliminar duplicados
 # 3. reemplazar en la columna UNIDAD los nulos por SIN_DATO
 # 4. convertir campo 'FECHA_INICIO_DESPLAZAMIENTO_MOVIL'a datetime
 # 5. corregir campo 'RECEPCION', crenado 'RECEPCION CORREGIDA'
 # 6. reemplazar SIN_DATO por un valor nulo de tipo numerico en el campo 'EDAD'
 # 7. eliminar espacios al inicio y al final del campo 'LOCALIDAD' 
 # 3. guardar el resumen de formato .csv


from fileinput import filename
import pandas as pd
import os
from pathlib import Path
from dateutil.parser import parse
import numpy as np

def main():

    #leer archivo
    data = get_data(filename = "llamadas123_julio_2022.csv")
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
    #quita espacios en campo 'LOCALIDAD'
    df_final = quitar_espacios(df_EDAD_ok)
    # guarde el resumen
    save_data(df_final, filename = "llamadas123_julio_2022_processed.csv")

def save_data(df, filename):
    out_name = "resumen_" + filename
    root_dir = Path(".").resolve()
    out_path = os.path.join(root_dir, 'data', 'processed', out_name)

    df.to_csv(out_path)

def quitar_espacios(df):
    df['LOCALIDAD']= df['LOCALIDAD'].apply(lambda x: x.strip())
    df_final = df
    return df_final     

def corregir_edad(df):
    df['EDAD']=df['EDAD'].replace({'SIN_DATO': np.nan})
    f = lambda x: x if pd.isna(x) == True else int(x)
    df['EDAD']=df['EDAD'].apply(f)
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
        except Exception as e:
            print(i, e)
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

def get_data(filename):
    data_dir = "raw"
    root_dir = Path(".").resolve()
    file_path=os.path.join(root_dir, "data", data_dir, filename)

    data = pd.read_csv(file_path, encoding = 'latin-1', sep = ';')
    return data   


if __name__ == '__main__':
    main() 