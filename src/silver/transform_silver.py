import pandas as pd
import json
import os

def process_bronze_to_silver(blob_name):

    # gera path do arquivo fonte
    path_input = os.path.join("data", "bronze", blob_name)

    # Exemplo de salvamento na camada silver local    
    output_path = os.path.join("data", "silver", "silver_issues.parquet")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(path_input, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 1. Normaliza focando na lista de 'issues'
    # 'record_path' entra na lista de chamados
    # 'meta' traz dados do projeto para todas as linhas (opcional)
    df = pd.json_normalize(
        data, 
        record_path=['issues'], 
        meta=[['project', 'project_name']]
    )

    # 2. Tratando listas internas (Assignee e Timestamps)
    # Como são listas de 1 item, pegamos o primeiro elemento [0]
    df['analista_nome'] = df['assignee'].apply(lambda x: x[0]['name'] if x else None)
    df['created_at'] = df['timestamps'].apply(lambda x: x[0]['created_at'] if x else None)
    df['resolved_at'] = df['timestamps'].apply(lambda x: x[0]['resolved_at'] if x else None)

    # 3. Limpeza de Datas (Requisito do desafio)
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['resolved_at'] = pd.to_datetime(df['resolved_at'], errors='coerce')

    # Remove chamados com datas de criação inválidas
    df = df.dropna(subset=['created_at'])

    # 4. Selecionando e renomeando colunas finais para a Silver
    columns_to_keep = [
        'id', 'issue_type', 'status', 'priority', 
        'analista_nome', 'created_at', 'resolved_at'
    ]
    df_silver = df[columns_to_keep]

    # Salva em Parquet
    df_silver.to_parquet(output_path, index=False)
    return df_silver