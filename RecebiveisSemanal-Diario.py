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

def job():
    def create_connection(driver, server, database, user, password, port):
        connection = None
        try:
            connection = pyodbc.connect(
                f'DRIVER={{{driver}}};'
                f'SERVER={server},{port};'
                f'DATABASE={database};'
                f'UID={user};'
                f'PWD={password}'
            )
            print("Connection to SQL Server successful")
        except pyodbc.Error as e:
            print(f"The error '{e}' occurred")

        return connection

    def truncate_table(connection):
        try:
            with connection.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE BD_RECEBIVEIS_SEMANAL")
                connection.commit()
                print("Tabela foi truncada com sucesso. RSD")
        except pyodbc.Error as e:
            print(f"O erro foi: {e} RSD")

    def insert_data(connection, startdate, enddate, companyNumber, amount, total):
        try:
            cursor = connection.cursor()
            insert_query = """
            INSERT INTO BD_RECEBIVEIS_SEMANAL (StartDate, EndDate, CompanyNumber, ValorTotal, Quantidade)
            VALUES (?, ?, ?, ?, ?)
            """
            cursor.execute(insert_query, (startdate, enddate, companyNumber, float(amount), int(total)))
            connection.commit()
        except pyodbc.Error as e:
            print(f"Erro ao inserir dados: {e} RSD")

    driver = "ODBC Driver 17 for SQL Server"
    server = os.getenv("DB_SERVER_EXCEL")
    user = os.getenv("DB_USER_EXCEL")
    password = os.getenv("DB_PASSWORD_EXCEL")
    database = os.getenv("DB_DATABASE_EXCEL")
    port = int(os.getenv("DB_PORT_EXCEL"))

    connection = create_connection(driver, server, database, user, password, port)

    if connection:
        print("Código está correto e a conexão foi estabelecida com sucesso. RSD")
    else:
        print("Código está correto, mas não foi possível estabelecer a conexão. RSD")

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
            access_token = data.get("access_token", "")
            refresh_token = data.get("refresh_token", "")
            token_type = data.get("token_type", "")
            expires_in = data.get("expires_in", "")
            scope = data.get("scope", "")

            return access_token, refresh_token, token_type, expires_in, scope
        else:
            print("Falha na obtenção do token. Status code:", response.status_code)
            return None, None, None, None, None

    def refresh_access_token(refresh_token):
        url = "https://api.userede.com.br/redelabs/oauth/token"
        body = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        }

        headers = {
            "Authorization": os.getenv("API_AUTH_HEADER_REDE")
        }

        response = requests.post(url, data=body, headers=headers)

        if response.status_code == 200:
            data = response.json()
            access_token = data.get("access_token", "")
            refresh_token = data.get("refresh_token", "")

            return access_token, refresh_token
        else:
            print("Falha na atualização do token. Status code:", response.status_code)
            return None, None

    def fetch_and_insert_data(companyNumber, access_token, refresh_token, driver, server, database, user, password, port):
        connection = create_connection(driver, server, database, user, password, port)
        if not connection:
            return

        url = "https://api.userede.com.br/redelabs/merchant-statement/v2/receivables/summary"
        data = datetime.datetime.now()

        for i in range(40):
            day = data + datetime.timedelta(days=i)
            if day.weekday() != 5 and day.weekday() != 6:
                startdate = f"{day.year:02d}-{day.month:02d}-{day.day:02d}"
                enddate = f"{day.year:02d}-{day.month:02d}-{day.day:02d}"

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
                    response = requests.get(url, params=params, headers=headers, timeout=10)
                    if response.status_code == 200:
                        content = response.json().get('content')
                        if content:
                            amount = content[0]['amount']
                            total = content[0]['total']
                            print(f"Empresa {companyNumber}, Data {startdate}: Amount = {amount}, Total = {total} RSD")
                            insert_data(connection, startdate, enddate, companyNumber, amount, total)
                        else:
                            print(f"Empresa {companyNumber}, Data {startdate}: Sem dados. RSD")
                            insert_data(connection, startdate, enddate, companyNumber, 0, 0)
                    elif response.status_code == 401:
                        new_access_token, new_refresh_token = refresh_access_token(refresh_token)
                        if new_access_token:
                            access_token = new_access_token
                            refresh_token = new_refresh_token
                            headers["Authorization"] = "Bearer " + access_token
                            response = requests.get(url, params=params, headers=headers, timeout=10)
                            if response.status_code == 200:
                                content = response.json().get('content')
                                if content:
                                    amount = content[0]['amount']
                                    total = content[0]['total']
                                    print(f"Empresa {companyNumber}, Data {startdate}: Amount = {amount}, Total = {total} RSD")
                                    insert_data(connection, startdate, enddate, companyNumber, amount, total)
                                else:
                                    print(f"Empresa {companyNumber}, Data {startdate}: Sem dados. RSD")
                                    insert_data(connection, startdate, enddate, companyNumber, 0, 0)
                            else:
                                print(f"Erro para a empresa {companyNumber}: {response.status_code} RSD")
                        else:
                            print("Falha na atualização do token. RSD")
                    else:
                        print(f"Erro para a empresa {companyNumber}: {response.status_code} RSD")
                except requests.exceptions.Timeout:
                    print(f"Timeout ao tentar obter dados para a empresa {companyNumber} RSD")

    def main():
        access_token, refresh_token, _, _, _ = get_tokens()

        if not access_token or not refresh_token:
            print("Falha na obtenção do token. RSD")
            return

        truncate_table(connection)

        company_numbers_str = os.getenv("COMPANY_NUMBERS_REDE")
        if company_numbers_str:
            companyNumbers = [int(num.strip()) for num in company_numbers_str.split(',') if num.strip()]
        else:
            companyNumbers = []

        max_workers = os.cpu_count() - 1

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_and_insert_data, companyNumber, access_token, refresh_token, driver, server, database, user, password, port) for companyNumber in companyNumbers]
            for future in concurrent.futures.as_completed(futures):
                future.result()

    main()  # Call the main function

if __name__ == "__main__":
    job()