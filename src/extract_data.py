#Imports
import pandas as pd
import numpy as np
import requests
from config.config import API_URL


# Função que extrai os dados da API
def get_data_from_api(file_path):
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

if __name__ == "__main__":
    # URL da API
    file_path = API_URL
    
    # Extrair dados e carregar no terminal
    df = get_data_from_api(file_path)

   

