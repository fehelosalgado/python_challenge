import pandas as pd
import requests
from datetime import datetime

def get_holidays(year_list):

    # create list to receive all holydays
    all_holidays = []

    """Busca feriados nacionais via API pública."""
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