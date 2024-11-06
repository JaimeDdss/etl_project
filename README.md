# Projeto de ETL de Commodities

Este projeto tem como objetivo criar um Data Warehouse (DW) para armazenar e analisar dados de commodities, utilizando uma arquitetura moderna de ETL (Extract,Transform, Load). O projeto inclui:

[Arquivo Parquet final](data)

1. **Parte de Extract**: Responsável por extrair dados de uma API do BC e carregar em memória.
2. **Parte de Load**: Carrega dados de movimentações de commodities a partir de arquivos CSVe XLS.
3. **Parte de pipeline**: Processa as funções de extração/ carga de dados, transforma os dados e salva um arquivo parquet.

### Ideia de ETL

graph LR;
    A[Extract] -->|Extrai Dados da API| B[Load]
    B -->|Carrega Dados de Arquivos CSV e XLS| C[Pipeline]
    C -->|Limpa e Transforma os Dados, depois Salva em Parquet|
