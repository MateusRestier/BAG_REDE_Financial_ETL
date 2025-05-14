import requests
import datetime
import pyodbc
import concurrent.futures
import os

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


# --------------------------------------------------------------------------- #
# 1. Conexão com o SQL Server
# --------------------------------------------------------------------------- #
def create_connection(driver, server, database, user, password, port):
    try:
        connection = pyodbc.connect(
            f'DRIVER={{{driver}}};'
            f'SERVER={server},{port};'
            f'DATABASE={database};'
            f'UID={user};'
            f'PWD={password}'
        )
        print("Connection to SQL Server successful RM")
        return connection
    except pyodbc.Error as e:
        print(f"The error '{e}' occurred RM")
        return None


# --------------------------------------------------------------------------- #
# 2. Inserção de dados
# --------------------------------------------------------------------------- #
def insert_data(connection, startdate, enddate, companyNumber,
                amount, total):
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO BD_RECEBIVEIS_MENSAL
               (StartDate, EndDate, CompanyNumber,
                ValorTotal, Quantidade)
        VALUES  (?, ?, ?, ?, ?)
        """
        cursor.execute(
            insert_query,
            (startdate, enddate, companyNumber,
             amount, total)
        )
        connection.commit()
    except pyodbc.Error as e:
        print(f"Erro ao inserir dados: {e} RM")


# --------------------------------------------------------------------------- #
# 3. Autenticação / Renovação de token
# --------------------------------------------------------------------------- #
def get_tokens():
    url = os.getenv("TOKEN_URL_REDE")
    body = {
        "grant_type": "password",
        "username": os.getenv("API_USERNAME_REDE"),
        "password": os.getenv("API_PASSWORD_REDE")
    }
    headers = {
        "Authorization": os.getenv("API_AUTH_HEADER_REDE")
    }
    response = requests.post(url, data=body, headers=headers)
    if response.status_code == 200:
        data = response.json()
        return (data.get("access_token"), data.get("refresh_token"),
                data.get("token_type"), data.get("expires_in"),
                data.get("scope"))
    else:
        print("Falha na obtenção do token. Status code:",
              response.status_code, "RM")
        return None, None, None, None, None


def refresh_access_token(refresh_token):
    url = "https://api.userede.com.br/redelabs/oauth/token"
    body = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }
    headers = {
        "Authorization":
        "Basic N2I3OWIyNjUtNjFjMi00YmJiLThlNmItZGE2NDNjMDliMThiOjI3bWJXMnpDeFE="
    }
    response = requests.post(url, data=body, headers=headers)
    if response.status_code == 200:
        data = response.json()
        return data.get("access_token"), data.get("refresh_token")
    else:
        print("Falha na atualização do token. Status code:",
              response.status_code, "RM")
        return None, None


# --------------------------------------------------------------------------- #
# 4. Processo por empresa (1 thread por CNPJ)
# --------------------------------------------------------------------------- #
def process_company(companyNumber, data, url,
                    access_token, refresh_token,
                    driver, server, database, user, password, port):
    connection = create_connection(driver, server, database, user, password,
                                   port)
    if not connection:
        return

    for i in range(13):
        month = (data.month + i - 1) % 12 + 1
        year_offset = (data.month + i - 1) // 12
        current_year = data.year + year_offset
        dayfin = [
            31,
            29 if (current_year % 4 == 0 and
                   (current_year % 100 != 0 or current_year % 400 == 0))
            else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
        ][month - 1]

        startdate = f"{current_year:04d}-{month:02d}-01"
        enddate = f"{current_year:04d}-{month:02d}-{dayfin:02d}"

        params = {
            "startDate": startdate,
            "endDate": enddate,
            "parentCompanyNumber": companyNumber
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + access_token
        }

        try:
            response = requests.get(url,
                                    params=params,
                                    headers=headers,
                                    timeout=10)

            if response.status_code == 200:
                content = response.json().get('content', [])
                if content:
                    amount = content[0]['amount']
                    total = content[0]['total']
                else:
                    amount = total = 0
                insert_data(connection, startdate, enddate,
                            companyNumber, amount, total)
                print("Valores inseridos para a empresa",
                      companyNumber, "RM")

            elif response.status_code == 401:
                (new_access_token,
                 new_refresh_token) = refresh_access_token(refresh_token)
                if new_access_token:
                    access_token = new_access_token
                    refresh_token = new_refresh_token
                    headers["Authorization"] = "Bearer " + access_token
                else:
                    print("Falha na atualização do token. RM")
                    break
            else:
                print(f"Erro para a empresa {companyNumber}: "
                      f"{response.status_code} RM")

        except requests.exceptions.Timeout:
            print(f"Timeout ao tentar obter dados "
                  f"para a empresa {companyNumber} RM")


# --------------------------------------------------------------------------- #
# 5. Orquestração principal
# --------------------------------------------------------------------------- #
def job():
    driver = "ODBC Driver 17 for SQL Server"
    server = os.getenv("DB_SERVER_EXCEL")
    user = os.getenv("DB_USER_EXCEL")
    password = os.getenv("DB_PASSWORD_EXCEL")
    database = os.getenv("DB_DATABASE_EXCEL")
    port = int(os.getenv("DB_PORT_EXCEL"))

    connection_test = create_connection(driver, server, database,
                                        user, password, port)
    if not connection_test:
        print("Não foi possível estabelecer a conexão com o banco de dados."
              " RM")
        return

    access_token, refresh_token, _, _, _ = get_tokens()
    if not access_token or not refresh_token:
        print("Falha na obtenção do token. RM")
        return

    url = ("https://api.userede.com.br/redelabs/"
           "merchant-statement/v2/receivables/summary")

    company_numbers_str = os.getenv("COMPANY_NUMBERS_REDE", "")
    companyNumbers = [int(num.strip()) for num in company_numbers_str.split(',')
                      if num.strip()]

    data_base = datetime.datetime.now()

    max_workers = max(1, (os.cpu_count() or 2) - 1)
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                process_company,
                companyNumber,               # empresa
                data_base,                   # mês/ano de referência
                url,
                access_token,
                refresh_token,
                driver, server, database,
                user, password, port          
            ) for companyNumber in companyNumbers
        ]
        for future in concurrent.futures.as_completed(futures):
            future.result()

if __name__ == "__main__":
    job()
