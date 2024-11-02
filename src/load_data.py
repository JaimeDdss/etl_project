
# Imports
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os
from config.config import CSV_FILE_PATH1, XLS_FILE_PATH2

# função que carrega os dados csv e xls
def get_data_from_files():
    # referência do path dos arquivos
    file_paths = [ CSV_FILE_PATH1 , XLS_FILE_PATH2 ]

    # Parte que lê o arquivo csv
    df = pd.read_csv(file_paths[0])
    print('dados csv: ')
    print(df.head())

    # Parte que lê o arquivo xls
    df1 = pd.read_excel(file_paths[1])
    print('dados xls: ')
    print(df1.head())

    return df, df1

# Teste da função 
if __name__ == '__main__':
    get_data_from_files()


