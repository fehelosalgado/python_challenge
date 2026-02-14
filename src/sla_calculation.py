import pandas as pd
import requests
from datetime import datetime

def get_holidays(year_list):
    """Busca feriados nacionais via API pública."""
    # create list to receive all holydays
    all_holidays = []

    try:        
        for year in year_list:
            response = requests.get(f"https://brasilapi.com.br/api/feriados/v1/{year}")        
            if response.status_code == 200:
                holidays = [f['date'] for f in response.json()]
                all_holidays.extend(holidays)
        return all_holidays
    except Exception as e:
        print(f"Erro ao buscar feriados: {e}")
        return []

def calculate_working_hours(start_date, end_date, holidays_issues):
    """Calcula a diferença em horas apenas em dias úteis."""
    if pd.isna(start_date) or pd.isna(end_date):
        return None

    # Criar um range de dias entre as datas
    days = pd.bdate_range(start=start_date, end=end_date, freq='C', holidays=holidays_issues)

    # Se abriu e fechou no mesmo dia útil
    if len(days) <= 1:
        diff = end_date - start_date
        return max(0, diff.total_seconds() / 3600)

    # Cálculo: (Horas do 1º dia) + (Dias intermediários * 24h) + (Horas do último dia)
    # Nota: O desafio não especifica "horário comercial" (ex: 9h às 18h), 
    # então consideraremos 24h úteis por dia de semana.
    total_hours = (len(days) - 2) * 24

    # Horas do primeiro dia (até meia-noite)
    first_day_end = start_date.replace(hour=23, minute=59, second=59)
    total_hours += (first_day_end - start_date).total_seconds() / 3600

    # Horas do último dia (desde a meia-noite)
    last_day_start = end_date.replace(hour=0, minute=0, second=0)
    total_hours += (end_date - last_day_start).total_seconds() / 3600

    return int(round(total_hours, 0))

def define_expected_sla(priority):
    """Retorna o SLA em horas conforme a regra de negócio."""
    regras = {
        'High': 24,
        'Medium': 72,
        'Low': 120
    }
    return regras.get(priority)

def verify_sla_status(spent_hours, expected_hours):
    """Indica se o SLA foi atendido ou violado."""
    if spent_hours is None:
        return None
    return True if spent_hours <= expected_hours else False