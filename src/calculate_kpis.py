# imports dos módulos de load_data e extract_data
from src.extract_data import get_data_from_api
from src.load_data import get_data_from_files
from config.config import API_URL, CSV_FILE_PATH1, XLS_FILE_PATH2

# carregando os dados da API
api_url = API_URL
df_api = get_data_from_api(api_url)
print(df_api.head())

# carregando os dados dos arquivos xls e csv
# dados do csv
df_final = get_data_from_files(CSV_FILE_PATH1)
# dados xsl 
df_xls = get_data_from_files(XLS_FILE_PATH2)

# criação dos KPI's



