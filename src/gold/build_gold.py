import pandas as pd
import os
import numpy as np
from src.sla_calculation import get_holidays, calculate_working_hours, define_expected_sla, verify_sla_status

def process_silver_to_gold(df_silver):
    # filtrar apenas chamados finalizados (Done ou Resolved)
    # regra do arquivo projeto.md
    df_gold = df_silver[df_silver['status'].isin(['Done', 'Resolved'])].copy()

    # lista dos anos distintos do campo created_at
    year_created_at = df_gold['created_at'].dt.year.dropna().astype(int).unique()

    # lista dos anos distintos do campo resolved_at
    year_resolved_at = df_gold['resolved_at'].dt.year.dropna().astype(int).unique()

    # junta todos os anos distintos, combinando as 2 listas acima
    year_list = np.union1d(year_created_at, year_resolved_at)

    # cria lista com os feriados
    holidays = get_holidays(year_list)

    # 3. Aplicar cálculos
    # Tempo de resolução em horas uteis
    df_gold['resolution_hours'] = df_gold.apply(lambda row: calculate_working_hours(row['created_at'], row['resolved_at'], holidays), axis=1).astype('Int64')

    # SLA Esperado
    df_gold['sla_expected_hours'] = df_gold['priority'].apply(define_expected_sla)

    # Status do SLA
    df_gold['is_sla_met'] = df_gold.apply(
        lambda row: verify_sla_status(row['resolution_hours'], row['sla_expected_hours']), axis=1
    )

    # sort columns
    df_gold = df_gold[['id', 'issue_type', 'priority', 'analista_nome', 'created_at', 'resolved_at', 'resolution_hours', 'sla_expected_hours', 'is_sla_met']]

    # rename columns
    df_gold = df_gold.rename(columns={'id': 'issue_id', 'analista_nome': 'assignee_name'})


    ############################
    # expected output files
    ############################

    ############################
    # table

    # Exemplo de salvamento na camada gold local
    output_path = os.path.join("data", "gold", "gold_sla_issues.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # 4. Salvar Tabela Final
    df_gold.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')

    ############################
    # report 1

    # 1. SLA Médio por Analista
    df_sla_by_analyst = df_gold.groupby('assignee_name').agg(
        issue_quantity=('issue_id', 'count'),
        sla_mean_hours=('resolution_hours', 'mean')
    ).reset_index().round({'sla_mean_hours': 2})

    # Exemplo de salvamento na camada gold local
    output_path = os.path.join("data", "gold", "gold_sla_by_analyst.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # 4. Salvar Tabela Final
    df_sla_by_analyst.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')

    ############################
    # report 2

    # 2. SLA Médio por Tipo de Chamado
    df_sla_by_issue_type = df_gold.groupby('issue_type').agg(
        issue_quantity=('issue_id', 'count'),
        sla_mean_hours=('resolution_hours', 'mean')
    ).reset_index().round({'sla_mean_hours': 2})

    # Exemplo de salvamento na camada gold local
    output_path = os.path.join("data", "gold", "gold_sla_by_issue_type.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # 4. Salvar Tabela Final
    df_sla_by_issue_type.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')