"""
Orquestrador do Pipeline de Dados Jira: Bronze -> Silver -> Gold.

Este script centraliza a execução de todas as etapas do processo de ETL:
1. Bronze: Realiza o download dos dados brutos do Azure Blob Storage.
2. Silver: Limpa, normaliza e converte os tipos de dados do arquivo JSON.
3. Gold: Aplica regras de negócio (SLA), busca feriados e gera relatórios finais.

O pipeline depende de um arquivo .env configurado com as credenciais do Azure
e conexão com a internet para consulta à API de feriados.
"""

from src.bronze.ingest_bronze import download_jira_data
from src.silver.transform_silver import process_bronze_to_silver
from src.gold.build_gold import process_silver_to_gold

###########################################
# execução da camada bronze
###########################################

download_jira_data()

###########################################
# execução da camada silver
###########################################

df_silver = process_bronze_to_silver()

###########################################
# execução da camada gold
###########################################

process_silver_to_gold(df_silver)