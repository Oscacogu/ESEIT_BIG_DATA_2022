 # Pseudo codigo
 # 1. leer archivo .csv
 # 2. extraer el resumen
 # 3. guardar el resumen de formato .csv

from fileinput import filename
import pandas as pd
import os
from pathlib import Path

def main(): 

    #leer archivo
    data = get_data(filename = "llamadas123_julio_2022.csv")
    # extraer resumen
    df_resumen = get_summary(data)
    # guarde el resumen
    save_data(df_resumen, filename = "llamadas123_julio_2022.csv")

def save_data(df, filename):
    out_name = "resumen_" + filename
    root_dir = Path(".").resolve()
    out_path = os.path.join(root_dir, 'data', 'processed', out_name)

    df.to_csv(out_path)

def get_summary(data):
    # Crear un diccionario vacio
    dict_resumen = dict()

    for col in data.columns:
        valores_unicos = data[col].unique()
        n_valores = len(valores_unicos)
        # print(col, n_valores)
        dict_resumen[col] = n_valores

    df_resumen = pd.DataFrame.from_dict(dict_resumen, orient= 'index')
    df_resumen.rename({0: 'Count'}, axis = 1, inplace=True)
    
    return df_resumen

def get_data(filename):
    data_dir = "raw"
    root_dir = Path(".").resolve()
    file_path=os.path.join(root_dir, "data", data_dir, filename)

    data = pd.read_csv(file_path, encoding = 'latin-1', sep = ';')
    return data


if __name__ == '__main__':
    main() 

