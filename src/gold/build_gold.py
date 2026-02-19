import pandas as pd
import os
import numpy as np
from src.sla_calculation import get_holidays, calculate_working_hours, define_expected_sla, verify_sla_status

def process_silver_to_gold(df_silver):
    """
    Transforma os dados da camada Silver em indicadores de negócio na camada Gold.

    A função filtra apenas chamados finalizados, identifica os anos envolvidos para 
    buscar feriados nacionais via API e calcula o SLA real em horas úteis. Além disso, 
    atribui o SLA esperado por prioridade e verifica o cumprimento da meta. Ao final, 
    gera e exporta três arquivos CSV: a base detalhada por chamado e dois relatórios 
    agregados (por analista e por tipo de chamado).

    Args:
        df_silver (pd.DataFrame): DataFrame proveniente da camada Silver, já limpo 
            e com as colunas de data devidamente tipadas.

    Returns:
        A função realiza o salvamento físico de múltiplos arquivos CSV no 
            diretório 'data/gold' e exibe os resultados através de arquivos locais.
    """
    
    print("Iniciando processamento da camada gold...")

    # cria um novo data frame (.copy()), filtrando apenas chamados finalizados (Done ou Resolved)
    df_gold = df_silver[df_silver['status'].isin(['Done', 'Resolved'])].copy()

    # criação da lista com feriados
    # lista dos anos distintos do campo created_at
    year_created_at = df_gold['created_at'].dt.year.dropna().astype(int).unique()
    # lista dos anos distintos do campo resolved_at
    year_resolved_at = df_gold['resolved_at'].dt.year.dropna().astype(int).unique()
    # junta todos os anos distintos, combinando as 2 listas acima
    year_list = np.union1d(year_created_at, year_resolved_at)
    
    print("    Buscando feriados...")
    # lista com os feriados
    holidays = get_holidays(year_list)

    print("    Calculando tempo de resolução em horas úteis...")
    # cria novas colunas no df_gold
    # tempo de resolução em horas úteis
    df_gold['resolution_hours'] = df_gold.apply(
        lambda row: calculate_working_hours(row['created_at'], row['resolved_at'], holidays), axis=1
    ).astype('Int64')

    print("    Calculando SLA esperado (em horas)...")
    # SLA esperado (em horas)
    df_gold['sla_expected_hours'] = df_gold['priority'].apply(define_expected_sla)

    print("    Calculando indicador de SLA atendido ou não atendido...")
    # indicador de SLA atendido ou não atendido
    df_gold['is_sla_met'] = df_gold.apply(
        lambda row: verify_sla_status(row['resolution_hours'], row['sla_expected_hours']), axis=1
    )

    # reordena colunas do dataframe final
    df_gold = df_gold[['issue_id', 'issue_type', 'priority', 'analyst', 'created_at', 'resolved_at', 'resolution_hours', 'sla_expected_hours', 'is_sla_met']]

    ############################
    # expected output files
    ############################

    ############################
    # output 1: Tabela Final – SLA por Chamado
    print("    Gerando output 1: Tabela Final – SLA por Chamado...")

    # caminho do arquivo destino
    output_path = os.path.join("data", "gold", "gold_sla_issues.csv")

    # grava localmente o data frame em formato csv
    df_gold.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
    print(f"    Arquivo salvo em: {output_path}")

    ############################
    # output 2: SLA Médio por Analista
    print("    Gerando output 2: SLA Médio por Analista...")

    # agrupamento com Quantidade de chamados e SLA médio (em horas) por analista
    df_sla_by_analyst = df_gold.groupby('analyst').agg(
        issue_quantity=('issue_id', 'count'),
        sla_mean_hours=('resolution_hours', 'mean')
    ).reset_index().round({'sla_mean_hours': 2})

    # caminho do arquivo destino
    output_path = os.path.join("data", "gold", "gold_sla_by_analyst.csv")

    # grava localmente o data frame em formato csv
    df_sla_by_analyst.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
    print(f"    Arquivo salvo em: {output_path}")

    ############################
    # output 3: SLA Médio por Tipo de Chamado
    print("    Gerando output 3: SLA Médio por Tipo de Chamado...")

    # agrupamento com Quantidade de chamados e SLA médio (em horas) por tipo de chamado
    df_sla_by_issue_type = df_gold.groupby('issue_type').agg(
        issue_quantity=('issue_id', 'count'),
        sla_mean_hours=('resolution_hours', 'mean')
    ).reset_index().round({'sla_mean_hours': 2})

    # caminho do arquivo destino
    output_path = os.path.join("data", "gold", "gold_sla_by_issue_type.csv")

    # grava localmente o data frame em formato csv
    df_sla_by_issue_type.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
    print(f"    Arquivo salvo em: {output_path}")

    print("Camada gold finalizada com sucesso!")