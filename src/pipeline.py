# Imports dos módulos + caminho da pasta dos arquivos
import os
import sys
import pandas as pd
from datetime import datetime
sys.path.append('../src')  # 
from src.extract_data import get_data_from_api
from src.load_data import get_data_from_files
from config.config import API_URL, CSV_FILE_PATH1, XLS_FILE_PATH2, OUTPUT_PATH

# Definindo padrão de valores das colunas
DEFAULT_VALUES = {
    'nome_cmdty': 'Boi_Gordo',
    'tipo_cmdty': 'Indicador do Boi Gordo CEPEA/B3',
    'cmdty_um': '15 Kg/carcaça'
}

def data_from_api(api_url):
    # chamando a funçao que extrai os dados da api e guarda num df
    df_api = get_data_from_api(api_url)
    
    return df_api.copy() # devolve uma copi dos dados da api
    
def load_clean_xls_data(df_xls):
    n = 2
    df_xls_copy = df_xls.iloc[n:].reset_index(drop=True)
    df_xls_copy.columns = df_xls_copy.iloc[0]
    df_xls_copy = df_xls_copy[1:].reset_index(drop=True)
    df_xls_copy = df_xls_copy.iloc[:, :2]
    df_xls_copy['Data'] = pd.to_datetime(df_xls_copy['Data'], format='%m/%Y', errors='coerce')
    return df_xls_copy
    
    
def xls_transformation_data(df_xls_copy):  
    date_range = pd.date_range(start=df_xls_copy['Data'].min(), end=df_xls_copy['Data'].max(), freq='MS')
    date_df = pd.DataFrame(date_range, columns=['Data'])
    merged_df = date_df.merge(df_xls_copy, on='Data', how='left')
    merged_df['Data'] = merged_df['Data'].dt.strftime("%m/%Y") 
    merged_df['À vista R$'] = merged_df['À vista R$'].astype('float64')
    merged_df.set_index('Data',inplace=True)
    merged_df['À vista R$'] = merged_df['À vista R$'].ffill().round(2)
    merged_df = merged_df.reset_index()
    merged_df['Data'] = pd.to_datetime(merged_df['Data'], format='%m/%Y', errors='coerce' ) + pd.offsets.MonthBegin(1)
    
    return merged_df
    
def ipca_transformation_data(df_api_copy, merged_df):
    df_api_copy = df_api_copy.rename(columns={'data': 'Data','valor': 'IPCA'})
    df_api_copy['Data'] = pd.to_datetime(df_api_copy['Data'], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
    df_api_copy['IPCA'] = df_api_copy['IPCA'].astype('float64')
    merged_df['Data'] = pd.to_datetime(merged_df['Data'])
    df_api_copy['Data'] = pd.to_datetime(df_api_copy['Data'])
    df_ipca = pd.merge(merged_df, df_api_copy, how='right', on= 'Data')
    df_ipca['IPCA_acum'] = df_ipca['IPCA'].cumsum()
    df_ipca['Data'] = pd.to_datetime(df_ipca['Data'], errors='coerce')
    data_recente = df_ipca['Data'].max()
    ipca_acum_dez_2022 = df_ipca.loc[df_ipca['Data'] == '2022-12-01', 'IPCA'].values[0]
    ipca_acum_atual = df_ipca.loc[df_ipca['Data'] == data_recente, 'IPCA'].values[0]
    df_ipca['Real'] = df_ipca['À vista R$'] * (1 + (ipca_acum_dez_2022 - ipca_acum_atual) / 100)
    df_ipca['Real'] = df_ipca['Real'].round(2)
    
    return df_ipca
   
def upsert(df_csv_copy, df_ipca, default_values):
    df_csv_copy['dt_cmdty'] = pd.to_datetime(df_csv_copy['dt_cmdty'])
    df_ipca = df_ipca.rename(columns={'Data': 'dt_cmdty'})
    df_ipca["cmdty_var_mes_perc"] = df_ipca["Real"].pct_change()
    df_ipca["cmdty_vl_rs_um"] = df_ipca["Real"] 
    df_ipca = df_ipca[["dt_cmdty", "cmdty_vl_rs_um", "cmdty_var_mes_perc"]].dropna()
    df_final = pd.concat([df_csv_copy.set_index("dt_cmdty"), df_ipca.set_index("dt_cmdty")])
    df_final = df_final[~df_final.index.duplicated(keep='last')].reset_index()
    df_final['nome_cmdty'] = df_final['nome_cmdty'].fillna(default_values['nome_cmdty'])
    df_final['tipo_cmdty'] = df_final['tipo_cmdty'].fillna(default_values['tipo_cmdty'])
    df_final['cmdty_um'] = df_final['cmdty_um'].fillna(default_values['cmdty_um'])
    df_final['cmdty_var_mes_perc'] = df_final['cmdty_var_mes_perc'].round(2)
    df_final['dt_etl'] = datetime.today().strftime('%Y-%m-%d')

    return df_final
    
 # Função pipeline 
def run_pipeline():
    # Extração e tratamento de dados
    df_api_copy = data_from_api(API_URL)
    df_csv, df_xls = get_data_from_files()
    df_csv_copy = df_csv.copy()
    df_xls_copy = load_clean_xls_data(df_xls)
    
    # preenchimento e merge de datas
    merged_df = xls_transformation_data(df_xls_copy)
    
     # processamento dos dados IPCA
    df_ipca = ipca_transformation_data(df_api_copy, merged_df)
    
    # configuração de valores padrões e finalização
    df_final = upsert(df_csv_copy, df_ipca, DEFAULT_VALUES)
    
    # Salvando o resultado em parquet
    #df_final.to_parquet('upsert_boi_gordo_Vfinal.parquet', engine='pyarrow', compression='snappy', index=False)
    
    return df_final
 
# Executando a função principal
if __name__ == '__main__':
    df_result = run_pipeline()
    print(f'Dados salvos com sucesso: \n {df_result.shape}')
    