import requests
import pyodbc
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
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
        new_access_token, new_refresh_token = get_tokens()
        if new_access_token:
            self.access_token = new_access_token
            self.refresh_token = new_refresh_token
            print("Token atualizado com sucesso.")
        else:
            print("Falha ao atualizar token.")


# Conexão ao banco de dados
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
        print("Conexão ao banco de dados realizada com sucesso.")
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None


# Obter tokens de autenticação
def get_tokens():
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
        access_token = data.get("access_token", "")
        refresh_token = data.get("refresh_token", "")
        return access_token, refresh_token
    else:
        print("Falha na obtenção do token. Status code:", response.status_code)
        return None, None

def fetch_installments_parallel(rows, token_manager):
    def fetch_single(row):
        row_id, parent_company_number, payment_id = row
        print(f"Buscando parcelas para Payment ID: {payment_id}, Empresa: {parent_company_number}")
        return fetch_installments_by_payment_id(token_manager, parent_company_number, payment_id), row_id

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:  
        results = list(executor.map(fetch_single, rows))

    return results

# Inserir dados de pagamentos no banco
def insert_payments_batch(cursor, payments):
    query = """
        INSERT INTO BD_PagamentosConsolidados (
            paymentId, paymentDate, bankCode, bankBranchCode, accountNumber,
            brandCode, parentCompanyNumber, documentNumber, companyName, tradeName,
            netAmount, status, statusCode, type, typeCode
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        for payment in payments:
            print(f"    Inserindo pagamento ID: {payment.get('paymentId')}")
        cursor.executemany(query, [
            (
                payment.get('paymentId', None),
                payment.get('paymentDate', None),
                payment.get('bankCode', None),
                payment.get('bankBranchCode', None),
                payment.get('accountNumber', None),
                payment.get('brandCode', None),
                payment.get('companyNumber', None),
                payment.get('documentNumber', None),
                payment.get('companyName', None),
                payment.get('tradeName', None),
                payment.get('netAmount', 0.0),
                payment.get('status', None),
                payment.get('statusCode', None),
                payment.get('type', None),
                payment.get('typeCode', None)
            ) for payment in payments
        ])
    except Exception as e:
        print(f"Erro ao inserir pagamentos em batch: {e}")


# Atualizar parcelas no banco de dados
def update_installments_batch(cursor, installments, row_ids):
    query = """
        UPDATE BD_PagamentosConsolidados
        SET
            installmentQuantity = ?, 
            installmentNumber = ?, 
            saleAmount = ?, 
            authorizationCode = ?, 
            brand = ?, 
            cardNumber = ?, 
            expirationDate = ?, 
            flexFee = ?, 
            mdrAmount = ?, 
            feeTotal = ?, 
            nsu = ?
        WHERE id = ?
    """
    try:
        cursor.executemany(query, [
            (
                installment.get('installmentQuantity', 0),
                installment.get('installmentNumber', 0),
                installment.get('saleAmount', 0.0),
                installment.get('authorizationCode', "N/A"),
                installment.get('brand', "N/A"),
                installment.get('cardNumber', "N/A"),
                installment.get('expirationDate', None),
                installment.get('flexFee', 0.0),
                installment.get('mdrAmount', 0.0),
                installment.get('feeTotal', 0.0),
                installment.get('nsu', 0),
                row_id
            ) for installment, row_id in zip(installments, row_ids)
        ])
    except Exception as e:
        print(f"Erro ao atualizar parcelas em batch: {e}")


# Processar pagamentos para uma única empresa em um dia
def process_company_payments(company_number, token_manager, day, conn_string, batch_size):
    print(f"  Processando empresa: {company_number} para o dia {day}")
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()
    url = "https://api.userede.com.br/redelabs/merchant-statement/v1/payments"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token_manager.access_token}"}

    params = {
        "startDate": day,
        "endDate": day,
        "parentCompanyNumber": company_number,
        "subsidiaries": company_number,
        "pageKey": None
    }

    payments_batch = []

    while True:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if 'content' in data and 'payments' in data['content']:
                payments = data['content']['payments']
                payments_batch.extend(payments)

                if len(payments_batch) >= batch_size:
                    insert_payments_batch(cursor, payments_batch[:batch_size])
                    payments_batch = payments_batch[batch_size:]

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

    if payments_batch:
        insert_payments_batch(cursor, payments_batch)

    conn.commit()
    conn.close()

# Remover duplicatas no banco de dados
def remove_duplicates(cursor):
    query = """
    DELETE FROM BD_PagamentosConsolidados
    WHERE id NOT IN (
        SELECT MIN(id)
        FROM BD_PagamentosConsolidados
        GROUP BY 
            paymentId, 
            paymentDate, 
            bankCode, 
            bankBranchCode, 
            accountNumber, 
            brandCode, 
            parentCompanyNumber, 
            documentNumber, 
            companyName, 
            tradeName, 
            netAmount, 
            status, 
            statusCode, 
            type, 
            typeCode
    )
    """
    try:
        cursor.execute(query)
        cursor.commit()
        print("Linhas duplicadas removidas com sucesso.")
    except Exception as e:
        print(f"Erro ao remover duplicatas: {e}")

# Processar pagamentos e parcelas para todas as empresas de um dia
def process_daily_payments_and_installments(day, companyNumbers, token_manager, conn_string, batch_size):
    print(f"Iniciando processamento para o dia: {day}")

    # Processar pagamentos
    with ProcessPoolExecutor() as executor:
        tasks = [
            executor.submit(process_company_payments, company_number, token_manager, day, conn_string, batch_size)
            for company_number in companyNumbers
        ]
        for task in tasks:
            task.result()

    # Remover duplicatas após processamento de pagamentos
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()
    remove_duplicates(cursor)
    conn.close()

    # Buscar parcelas paralelamente, somente para linhas onde NSU está nulo
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()
    print(f"Buscando parcelas para os pagamentos do dia {day} onde NSU está nulo...")

    cursor.execute("""
        SELECT id, parentCompanyNumber, paymentId 
        FROM BD_PagamentosConsolidados 
        WHERE paymentDate = ? AND nsu IS NULL
    """, (day,))
    rows = cursor.fetchall()

    installments_results = fetch_installments_parallel(rows, token_manager)

    # Atualizar parcelas no banco
    installments_batch = []
    row_ids = []
    for installments_data, row_id in installments_results:
        if installments_data and 'content' in installments_data and 'installments' in installments_data['content']:
            for installment in installments_data['content']['installments']:
                installments_batch.append(installment)
                row_ids.append(row_id)

                if len(installments_batch) >= batch_size:
                    update_installments_batch(cursor, installments_batch[:batch_size], row_ids[:batch_size])
                    installments_batch = installments_batch[batch_size:]
                    row_ids = row_ids[batch_size:]

    if installments_batch:
        update_installments_batch(cursor, installments_batch, row_ids)

    conn.commit()
    conn.close()


def fetch_installments_by_payment_id(token_manager, parent_company_number, payment_id):
    url = f"https://api.userede.com.br/redelabs/merchant-statement/v2/payments/installments/{parent_company_number}/{payment_id}"
    headers = {"Authorization": f"Bearer {token_manager.access_token}", "Content-Type": "application/json"}
    
    retries = 3  # Número de tentativas
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 401:
                print("Token expirado ao buscar parcelas. Reautenticando...")
                token_manager.update_token()
                headers["Authorization"] = f"Bearer {token_manager.access_token}"
                response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Erro na API de parcelas por Payment ID. Status code: {response.status_code}")
                return None
        except requests.exceptions.Timeout:
            print(f"Tentativa {attempt + 1} de {retries} falhou devido a timeout. Retentando...")
        except requests.exceptions.ConnectionError as e:
            print(f"Tentativa {attempt + 1} de {retries} falhou devido a erro de conexão: {e}. Retentando...")
    print("Falha após múltiplas tentativas.")
    return None

def delete_previous_month_data(cursor, previous_month_start, previous_month_end):
    """
    Apaga os registros do mês anterior na tabela.
    """
    delete_query = """
        DELETE FROM BD_PagamentosConsolidados
        WHERE paymentDate BETWEEN ? AND ?
    """
    try:
        cursor.execute(delete_query, (previous_month_start, previous_month_end))
        cursor.connection.commit()
        print(f"Dados de {previous_month_start} a {previous_month_end} removidos com sucesso.")
    except Exception as e:
        print(f"Erro ao apagar dados do mês anterior: {e}")

def process_previous_month(token_manager, companyNumbers, conn_string, batch_size):
    """
    Processa o mês anterior: apaga os dados do mês anterior e os reprocessa.
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

    # Apaga os dados existentes do mês anterior
    delete_previous_month_data(cursor, previous_month_start, previous_month_end)

    # Processa os pagamentos para cada dia do mês anterior
    date_range = [
        (datetime.strptime(previous_month_start, "%Y-%m-%d") + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((datetime.strptime(previous_month_end, "%Y-%m-%d") - datetime.strptime(previous_month_start, "%Y-%m-%d")).days + 1)
    ]

    for day in date_range:
        process_daily_payments_and_installments(day, companyNumbers, token_manager, conn_string, batch_size)

    conn.close()
    print(f"Processamento do mês anterior concluído: {previous_month_start} a {previous_month_end}.")

# Principal
def main():
    conn_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('DB_SERVER_EXCEL')},{os.getenv('DB_PORT_EXCEL')};"
        f"DATABASE={os.getenv('DB_DATABASE_EXCEL')};"
        f"UID={os.getenv('DB_USER_EXCEL')};"
        f"PWD={os.getenv('DB_PASSWORD_EXCEL')}"
    )

    conn = connect_to_database()
    if not conn:
        return

    batch_size = 5  # Configuração do tamanho do batch

    company_numbers_str = os.getenv("COMPANY_NUMBERS_REDE")
    if company_numbers_str:
        companyNumbers = [int(num.strip()) for num in company_numbers_str.split(',') if num.strip()]
    else:
        companyNumbers = []

    token_manager = TokenManager()
    token_manager.update_token()  # Inicializar o token

    if not token_manager.access_token:
        print("Erro ao obter token de autenticação.")
        return

    # Menu de opções
    print("Escolha uma opção para executar:")
    print("1. Rodar D-1")
    print("2. Rodar um dia específico")
    print("3. Rodar um intervalo de dias")
    print("4. Rodar os últimos 7 dias")
    print("5. Reprocessar o mês anterior") 

    option = '4'
    #option = input("Digite o número da opção escolhida: ")

    # Calcular datas conforme a opção
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
        process_previous_month(token_manager, companyNumbers, conn_string, batch_size)
        return
    else:
        print("Opção inválida!")
        return

    # Converter datas para objetos datetime
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

    # Gerar o intervalo de datas
    date_range = [
        (start_date_obj + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((end_date_obj - start_date_obj).days + 1)
    ]

    # Processar cada dia no intervalo
    for day in date_range:
        process_daily_payments_and_installments(day, companyNumbers, token_manager, conn_string, batch_size)

    print("Processo concluído.")


if __name__ == "__main__":
    main()