import pandas as pd
import json
import os

def process_bronze_to_silver():
    """
    Realiza a limpeza, normalização e tipagem dos dados da camada Bronze para a Silver.

    A função lê o arquivo JSON bruto, extrai a lista de chamados ('issues') e achata 
    estruturas aninhadas (analista e datas). Realiza a conversão de tipos para 
    datetime, remove registros inconsistentes e exporta o resultado no formato 
    colunar Parquet para otimizar o armazenamento e performance.

    Args:
        Não recebe argumentos diretos. Depende da existência do arquivo:
        - 'data/bronze/bronze_issues.json'

    Returns:
        pd.DataFrame: DataFrame processado contendo o layout final com as colunas 
            selecionadas (issue_id, issue_type, status, priority, analyst, 
            created_at, resolved_at).
    """

    print("Iniciando processamento da camada silver...")

    # caminho do arquivo fonte
    path_input = os.path.join("data", "bronze", "bronze_issues.json")

    # leitura do arquivo fonte
    with open(path_input, 'r', encoding='utf-8') as f:
        data_source = json.load(f)

    # gera um data frame da lista 'issues' dentro do json
    df = pd.json_normalize(
        data=data_source, 
        record_path=['issues']
    )

    # cria novas colunas no df
    # dentro do primeiro elemento da lista 'assignee', pega o nome do analista
    df['analyst'] = df['assignee'].str[0].str.get('name')    
    # dentro do primeiro elemento da lista 'timestamps', pega a data de criação do chamado
    df['created_at'] = df['timestamps'].str[0].str.get('created_at')    
    # dentro do primeiro elemento da lista 'timestamps', pega a data de fechamento do chamado
    df['resolved_at'] = df['timestamps'].str[0].str.get('resolved_at')

    # conversão de str para datetime: qualquer valor que não seja possível de converter para datetime, será configurado como NaT (coerce)
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['resolved_at'] = pd.to_datetime(df['resolved_at'], errors='coerce')

    # deleta chamados com datas de criação ausentes
    df = df.dropna(subset=['created_at'])

    # selecionando colunas desejadas para o layout final
    columns_to_keep = [
        'id', 'issue_type', 'status', 'priority', 'analyst', 'created_at', 'resolved_at'
    ]

    # renomeia colunas
    columns_rename = {
        'id': 'issue_id'
    }

    # executa layout final e rename das colunas
    df_silver = df[columns_to_keep].rename(columns=columns_rename)

    # caminho do arquivo destino
    output_path = os.path.join("data", "silver", "silver_issues.parquet")

    # grava localmente o data frame em formato parquet
    df_silver.to_parquet(output_path, index=False)
    
    print(f"    Arquivo salvo em: {output_path}")
    print("Camada silver finalizada com sucesso!")

    # retorna o data frame final
    return df_silver