# Imports
import pandas as pd
import numpy as np
import requests

# Função que extrai os dados da API
def extract_data(file_path):
    try:
        response = requests.get(file_path)
        if response.status_code == 200:
            print(f"status: {response.status_code}")
            data = response.json()
            df = pd.DataFrame(data)
            return df
        else:
            print(f"Erro ao acessar a URL: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Erro na requisição: {e}")
        return None

# Função para carregar os dados no terminal
def load_data(df):
    if df is not None:
        print(df.head())
    else:
        (f"Data frame vazio ou não carregou")

if __name__ == "__main__":
    # URL da API
    file_path = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.10844/dados?formato=json"
    
    # Extrair dados e carregar no terminal
    df = extract_data(file_path)
    load_data(df)

