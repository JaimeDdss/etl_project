
# Imports
import pandas as pd
import os
from config.config import CSV_FILE_PATH1, XLS_FILE_PATH2, API_URL, OUTPUT_PATH

# função que carrega os dados csv e xls
def get_data_from_files():
    # referência do path dos arquivos
    file_paths = [ CSV_FILE_PATH1 , XLS_FILE_PATH2 ]

    # Parte que lê o arquivo csv
    df = pd.read_csv(file_paths[0])
       # Parte que lê o arquivo xls
    df1 = pd.read_excel(file_paths[1])
    
    return df, df1

# Teste da função 
if __name__ == '__main__':
    df_csv, df_xls = get_data_from_files()


