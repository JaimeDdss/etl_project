
# Imports dos módulos + caminho da pasta dos arquivos
import os
import sys
import pandas as pd
from datetime import datetime
sys.path.append('../src')  # 
from src.extract_data import get_data_from_api
from src.load_data import get_data_from_files
from config.config import API_URL, CSV_FILE_PATH1, XLS_FILE_PATH2


def calculo_kpis():
    # chamando a funçao que extrai os dados da api e guarda num df
    api_url = API_URL
    df_api = get_data_from_api(api_url)
    # Criando uma copia dos dados da api
    df_api_copy = df_api.copy()

    # chamando a função que lê os arquivos csv e xls
    df_csv, df_xls = get_data_from_files()
    # Criando uma copia dos arquivos csv e xls
    df_csv_copy = df_csv.copy()

    df_xls_copy = df_xls.copy()
    # Tratando os dados xls
    # definindo o numero de linhas para dropar
    n = 2
    # Pegando a partir da 4 linha e fazedno o reset do index
    df_xls_copy = df_xls_copy.iloc[n:].reset_index(drop=True)
    # Definindo o nome da primeira linha como header
    df_xls_copy.columns = df_xls_copy.iloc[0]
    # Removendo a linha que era header e fazendo o reset do index
    df_xls_copy = df_xls_copy[1:].reset_index(drop=True)
    # selcionando apenas as duas primeiras colunas do df
    df_xls_copy = df_xls_copy.iloc[:, :2]
    # mudando o tipo de dado da coluna de data
    df_xls_copy['Data'] = pd.to_datetime(df_xls_copy['Data'], format='%m/%Y', errors='coerce') 
    # criando um df auxiliar para preencher a data com a sequencia crono
    date_range = pd.date_range(start=df_xls_copy['Data'].min(), end=df_xls_copy['Data'].max(), freq='MS')
    date_df = pd.DataFrame(date_range, columns=['Data'])
    # fazendo o merge do df auxiliar com o principal
    merged_df = date_df.merge(df_xls_copy, on='Data', how='left')
    merged_df['Data'] = merged_df['Data'].dt.strftime("%m/%Y") 
    # Mudando o tipo de dados para float64 
    merged_df['À vista R$'] = merged_df['À vista R$'].astype('float64')

    # Complete o campo de valor que não estejam preenchidos com o valor do mês anterior;
    merged_df.set_index('Data',inplace=True)
    merged_df['À vista R$'] = merged_df['À vista R$'].ffill().infer_objects(copy=False)
    merged_df['À vista R$'] = merged_df['À vista R$'].round(2)
    merged_df = merged_df.reset_index()

    # forçando a transformação para data 
    merged_df['Data'] = pd.to_datetime(merged_df['Data'], format='%m/%Y', errors='coerce' ) + pd.offsets.MonthBegin(1)
    # renomeando as colunas do df_api_copy
    df_api_copy = df_api_copy.rename(columns={'data': 'Data','valor': 'IPCA'})

    # mudando os tipos de dados
    df_api_copy['Data'] = pd.to_datetime(df_api_copy['Data'], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
    df_api_copy['IPCA'] = df_api_copy['IPCA'].astype('float64')
    # garantindo que as colunas vão estar em formato de data
    merged_df['Data'] = pd.to_datetime(merged_df['Data'])
    df_api_copy['Data'] = pd.to_datetime(df_api_copy['Data'])

    # merge das bases de ipca com indicador boi gordo
    df_ipca = pd.merge(merged_df, df_api_copy, how='right', on= 'Data')
    # calculando o IPCA acumulado
    df_ipca['IPCA_acum'] = df_ipca['IPCA'].cumsum()
    # Forçando o formato de data
    df_ipca['Data'] = pd.to_datetime(df_ipca['Data'], errors='coerce')

    # definindo os valores de ipca
    data_recente = df_ipca['Data'].max()
    ipca_acum_dez_2022 = df_ipca.loc[df_ipca['Data'] == '2022-12-01', 'IPCA'].values[0]
    ipca_acum_atual = df_ipca.loc[df_ipca['Data'] == data_recente, 'IPCA'].values[0]

    # calculo do valor real 
    df_ipca['Real'] = df_ipca['À vista R$'] * (1 + (ipca_acum_dez_2022 - ipca_acum_atual) / 100)
    df_ipca['Real'] = df_ipca['Real'].round(2)

    # mudando para data o tipo da coluna dt_cmdty
    df_csv_copy['dt_cmdty'] = pd.to_datetime(df_csv_copy['dt_cmdty'])
    # Renomeando a coluna data
    df_ipca = df_ipca.rename(columns={'Data': 'dt_cmdty'})

    # renomeando a coluna Real e calculando a var percentual
    df_ipca["cmdty_var_mes_perc"] = df_ipca["Real"].pct_change()
    df_ipca["cmdty_vl_rs_um"] = df_ipca["Real"]
    # Dropna nas colunas selecionadas 
    df_ipca = df_ipca[["dt_cmdty", "cmdty_vl_rs_um", "cmdty_var_mes_perc"]].dropna()

    # Fazendo o upsert
    df_final = pd.concat([df_csv_copy.set_index("dt_cmdty"), df_ipca.set_index("dt_cmdty")])
    df_final = df_final[~df_final.index.duplicated(keep='last')].reset_index()
    # valores padrão
    valor1 = 'Boi_Gordo'
    valor2 = 'Indicador do Boi Gordo CEPEA/B3'
    valor3 = '15 Kg/carcaça'

    # replicando os valores para o restante das linhas
    df_final['nome_cmdty'] = df_final['nome_cmdty'].fillna(valor1)
    df_final['tipo_cmdty'] = df_final['tipo_cmdty'].fillna(valor2)
    df_final['cmdty_um'] = df_final['cmdty_um'].fillna(valor3)

    # round limitado com duas casas decimais da coluna cmdty_var_mes_perc
    df_final['cmdty_var_mes_perc'] = df_final['cmdty_var_mes_perc'].round(2)

    ## preenchendo a dt_etl com a data de hoje
    data_atual = datetime.today().strftime('%Y-%m-%d')
    df_final['dt_etl'] = data_atual

    df_final.to_parquet('upsert_boi_gordo_Vfinal.parquet', engine='pyarrow', compression='snappy', index=False)

    
    return df_final

df_final = calculo_kpis()
print(f'O arquivo parquet foi gerado com sucesso: \n {df_final.head()}')