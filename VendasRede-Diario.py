import requests
import pyodbc
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor
import os
from calendar import monthrange

"""Config dotenv"""
from dotenv import load_dotenv
from pathlib import Path
def localizar_env(diretorio_raiz="PRIVATE_BAG.ENV"):
    path = Path(__file__).resolve()
    for parent in path.parents:
        possible = parent / diretorio_raiz / ".env"
        if possible.exists():
            return possible
    raise FileNotFoundError(f"Arquivo .env não encontrado dentro de '{diretorio_raiz}'.")
env_path = localizar_env()
load_dotenv(dotenv_path=env_path)

class TokenManager:
    def __init__(self):
        self.access_token = None
        self.refresh_token = None

    def update_token(self):
        print("Reautenticando para obter novo token...")
        new_access_token, new_refresh_token = self.get_tokens()
        if new_access_token:
            self.access_token = new_access_token
            self.refresh_token = new_refresh_token
            print("Token atualizado com sucesso.")
        else:
            print("Falha ao atualizar token.")

    def get_tokens(self):
        url = os.getenv("TOKEN_URL_REDE")
        body = {
            "grant_type": "password",
            "username": os.getenv("API_USERNAME_REDE"),
            "password": os.getenv("API_PASSWORD_REDE")
        }
        headers = {
            "Authorization": os.getenv("API_AUTH_HEADER_REDE"),
        }

        response = requests.post(url, data=body, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return data.get("access_token"), data.get("refresh_token")
        else:
            print("Erro ao obter tokens:", response.status_code, response.text)
            return None, None


def connect_to_database():
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('DB_SERVER_EXCEL')},{os.getenv('DB_PORT_EXCEL')};"
        f"DATABASE={os.getenv('DB_DATABASE_EXCEL')};"
        f"UID={os.getenv('DB_USER_EXCEL')};"
        f"PWD={os.getenv('DB_PASSWORD_EXCEL')}"
    )
    try:
        conn = pyodbc.connect(connection_string)
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None


def insert_transactions_batch(cursor, transactions):
    query = """
        INSERT INTO BD_Vendas_Rede (
            Data_Movimentacao, Codigo_Autorizacao, Tipo_Captura, Valor_Liquido, Valor_Total, Status,
            TID, Data_Venda, Hora_Venda, NSU, Dispositivo, Tipo_Dispositivo, Taxa_MDR, Valor_MDR,
            Numero_Cartao, Numero_Token, Numero_Empresa, Nome_Documento, CREDIT, Parcelas
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cursor.executemany(query, [
            (
                transaction['Data_Movimentacao'], transaction['Codigo_Autorizacao'], transaction['Tipo_Captura'],
                transaction['Valor_Liquido'], transaction['Valor_Total'], transaction['Status'], transaction['TID'],
                transaction['Data_Venda'], transaction['Hora_Venda'], transaction['NSU'], transaction['Dispositivo'],
                transaction['Tipo_Dispositivo'], transaction['Taxa_MDR'], transaction['Valor_MDR'],
                transaction['Numero_Cartao'], transaction['Numero_Token'], transaction['Numero_Empresa'],
                transaction['Nome_Documento'], transaction['CREDIT'], transaction['Parcelas']
            ) for transaction in transactions
        ])
    except Exception as e:
        print(f"Erro ao inserir transações em batch: {e}")


def fetch_transactions_for_company(company_number, token_manager, start_date, end_date, batch_size):
    conn = connect_to_database()
    if not conn:
        return

    cursor = conn.cursor()
    print(f"Processando empresa: {company_number} para o período de {start_date} a {end_date}...")
    url = "https://api.userede.com.br/redelabs/merchant-statement/v2/sales"
    headers = {"Authorization": f"Bearer {token_manager.access_token}"}
    params = {
        "startDate": start_date,
        "endDate": end_date,
        "parentCompanyNumber": company_number,
        "subsidiaries": company_number,
        "pageKey": None,
        "size": 100
    }

    transactions_batch = []
    while True:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if 'content' in data and 'transactions' in data['content']:
                transactions = data['content']['transactions']
                transactions_batch.extend([
                    {
                        'Data_Movimentacao': t.get('movementDate', 'N/A'),
                        'Codigo_Autorizacao': t.get('authorizationCode', 'N/A'),
                        'Tipo_Captura': t.get('captureType', 'N/A'),
                        'Valor_Liquido': t.get('netAmount', 0),
                        'Valor_Total': t.get('amount', 0),
                        'Status': t.get('status', 'N/A'),
                        'TID': t.get('tid', 'N/A'),
                        'Data_Venda': t.get('saleDate', 'N/A'),
                        'Hora_Venda': t.get('saleHour', 'N/A'),
                        'NSU': t.get('nsu', 'N/A'),
                        'Dispositivo': t.get('device', 'N/A'),
                        'Tipo_Dispositivo': t.get('deviceType', 'N/A'),
                        'Taxa_MDR': t.get('mdrFee', 0),
                        'Valor_MDR': t.get('mdrAmount', 0),
                        'Numero_Cartao': t.get('cardNumber', 'N/A'),
                        'Numero_Token': t.get('tokenNumber', 'N/A'),
                        'Numero_Empresa': t.get('merchant', {}).get('companyNumber', 'N/A'),
                        'Nome_Documento': t.get('merchant', {}).get('documentName', 'N/A'),
                        'CREDIT': t.get('modality', {}).get('type', 'N/A'),
                        'Parcelas': t.get('installmentQuantity', 0)
                    } for t in transactions
                ])

                if len(transactions_batch) >= batch_size:
                    insert_transactions_batch(cursor, transactions_batch[:batch_size])
                    conn.commit()
                    transactions_batch = transactions_batch[batch_size:]

                if 'cursor' in data and data['cursor'].get('hasNextKey', False):
                    params['pageKey'] = data['cursor']['nextKey']
                else:
                    break
            else:
                break
        elif response.status_code == 401:
            print(f"Token expirado para empresa {company_number}. Reautenticando...")
            token_manager.update_token()
            headers["Authorization"] = f"Bearer {token_manager.access_token}"
        else:
            print(f"Erro na requisição para empresa {company_number}: {response.status_code}")
            break

    if transactions_batch:
        insert_transactions_batch(cursor, transactions_batch)
        conn.commit()

    conn.close()
    print(f"Processamento para empresa {company_number} concluído.")


def remove_duplicates():
    conn = connect_to_database()
    if not conn:
        return

    cursor = conn.cursor()
    print("Removendo duplicatas da tabela...")
    dedup_query = """
        WITH CTE AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY 
                    Data_Movimentacao, Codigo_Autorizacao, Tipo_Captura, Valor_Liquido, Valor_Total, Status,
                    TID, Data_Venda, Hora_Venda, NSU, Dispositivo, Tipo_Dispositivo, Taxa_MDR, Valor_MDR,
                    Numero_Cartao, Numero_Token, Numero_Empresa, Nome_Documento, CREDIT, Parcelas
                ORDER BY (SELECT NULL)
            ) AS RN
            FROM BD_Vendas_Rede
        )
        DELETE FROM CTE WHERE RN > 1
    """
    try:
        cursor.execute(dedup_query)
        conn.commit()
        print("Remoção de duplicatas concluída.")
    except Exception as e:
        print(f"Erro ao remover duplicatas: {e}")
    finally:
        conn.close()


def process_daily_transactions(token_manager, start_date, end_date, companyNumbers, batch_size):
    print(f"Iniciando processamento de transações para o período de {start_date} a {end_date}...")
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        tasks = [
            executor.submit(fetch_transactions_for_company, company_number, token_manager, start_date, end_date, batch_size)
            for company_number in companyNumbers
        ]
        for task in tasks:
            task.result()
    print("Processamento diário concluído.")
    remove_duplicates()

def delete_previous_month_data(cursor, previous_month_start, previous_month_end):
    """
    Apaga os registros do mês anterior na tabela.
    """
    delete_query = """
        DELETE FROM BD_Vendas_Rede
        WHERE Data_Movimentacao BETWEEN ? AND ?
    """
    try:
        cursor.execute(delete_query, (previous_month_start, previous_month_end))
        cursor.connection.commit()
        print(f"Dados de {previous_month_start} a {previous_month_end} removidos com sucesso.")
    except Exception as e:
        print(f"Erro ao apagar dados do mês anterior: {e}")

def process_previous_month(token_manager, batch_size, companyNumbers):
    """
    Identifica o mês anterior, apaga seus registros da tabela e processa os dados novamente.
    """
    today = datetime.today()
    previous_month = today.month - 1 or 12
    year = today.year if previous_month != 12 else today.year - 1

    # Calcula o início e o fim do mês anterior
    previous_month_start = datetime(year, previous_month, 1).strftime("%Y-%m-%d")
    last_day_of_month = monthrange(year, previous_month)[1]
    previous_month_end = datetime(year, previous_month, last_day_of_month).strftime("%Y-%m-%d")

    print(f"Processando o mês anterior: {previous_month_start} a {previous_month_end}...")

    # Conecta ao banco de dados
    conn = connect_to_database()
    if not conn:
        return

    cursor = conn.cursor()

    # Remove os dados existentes do mês anterior
    delete_previous_month_data(cursor, previous_month_start, previous_month_end)

    # Processa as transações para o mês anterior
    process_daily_transactions(token_manager, previous_month_start, previous_month_end, companyNumbers, batch_size)

    conn.close()
    print(f"Processamento do mês anterior concluído: {previous_month_start} a {previous_month_end}.")


def main():
    token_manager = TokenManager()
    token_manager.update_token()

    company_numbers_str = os.getenv("COMPANY_NUMBERS_REDE")
    if company_numbers_str:
        companyNumbers = [int(num.strip()) for num in company_numbers_str.split(',') if num.strip()]
    else:
        companyNumbers = []

    if not token_manager.access_token:
        print("Erro ao obter token de autenticação.")
        return

    # Configuração do batch size
    batch_size = 100

    # Menu de opções
    print("Escolha uma opção para executar:")
    print("1. Rodar D-1")
    print("2. Rodar um dia específico")
    print("3. Rodar um intervalo de dias")
    print("4. Rodar os últimos 7 dias")
    print("5. Reprocessar o mês anterior")  # Nova opção adicionada

    option = '4'
    #option = input("Digite o número da opção escolhida: ")

    today = datetime.today()
    if option == "1":
        start_date = end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    elif option == "2":
        date_input = input("Digite a data no formato YYYYMMDD: ")
        start_date = end_date = datetime.strptime(date_input, "%Y%m%d").strftime("%Y-%m-%d")
    elif option == "3":
        start_date_input = input("Digite a data inicial no formato YYYYMMDD: ")
        end_date_input = input("Digite a data final no formato YYYYMMDD: ")
        start_date = datetime.strptime(start_date_input, "%Y%m%d").strftime("%Y-%m-%d")
        end_date = datetime.strptime(end_date_input, "%Y%m%d").strftime("%Y-%m-%d")
    elif option == "4":
        start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    elif option == "5":
        process_previous_month(token_manager, batch_size, companyNumbers)
        return
    else:
        print("Opção inválida!")
        return


    process_daily_transactions(token_manager, start_date, end_date, companyNumbers, batch_size)


if __name__ == "__main__":
    main()
