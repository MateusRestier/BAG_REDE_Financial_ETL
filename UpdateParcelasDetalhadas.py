import requests
import pyodbc
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import os
import time
from math import ceil
import pythoncom
from win32com.client import Dispatch
import logging

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

BATCH_SIZE = 150
BATCH_LOCK = threading.Lock()

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
            print("Erro ao obter token:", response.status_code, response.text)
            return None, None


def create_database_connection():
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


def remove_duplicate_rows():
    conn = create_database_connection()
    if not conn:
        return

    cursor = conn.cursor()
    try:
        print("Removendo linhas duplicadas...")
        delete_duplicates_query = """
            WITH CTE AS (
                SELECT 
                    id,
                    ROW_NUMBER() OVER (PARTITION BY NSU, merchantId, installmentNumber ORDER BY created_at DESC) AS row_num
                FROM BD_Parcelas_Detalhadas
            )
            DELETE FROM BD_Parcelas_Detalhadas
            WHERE id IN (
                SELECT id FROM CTE WHERE row_num > 1
            );
        """
        cursor.execute(delete_duplicates_query)
        conn.commit()
        print("Linhas duplicadas removidas com sucesso.")
    except Exception as e:
        print(f"Erro ao remover linhas duplicadas: {e}")
    finally:
        conn.close()


def load_processed_sales():
    conn = create_database_connection()
    if not conn:
        return set()

    cursor = conn.cursor()
    query = "SELECT NSU, merchantId FROM BD_Parcelas_Detalhadas"
    try:
        print("Carregando vendas já processadas em memória...")
        cursor.execute(query)
        processed_sales = set((row.NSU, row.merchantId) for row in cursor.fetchall())
        print(f"Total de vendas já processadas carregadas: {len(processed_sales)}")
        return processed_sales
    except Exception as e:
        print(f"Erro ao carregar vendas processadas: {e}")
        return set()
    finally:
        conn.close()


def fetch_installments(merchant_id, nsu, sale_date, token_manager):
    url = f"https://api.userede.com.br/redelabs/merchant-statement/v2/payments/installments/{merchant_id}"
    headers = {
        "Authorization": f"Bearer {token_manager.access_token}",
        "Content-Type": "application/json"
    }
    params = {
        "saleDate": sale_date,
        "nsu": nsu
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 401:
            print("Token expirado. Tentando atualizar...")
            token_manager.update_token()
            headers["Authorization"] = f"Bearer {token_manager.access_token}"
            response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erro na API para Merchant ID {merchant_id}, NSU {nsu}: {response.status_code}")
            return None
    except Exception as e:
        print(f"Erro ao conectar na API para NSU {nsu}: {e}")
        return None


def insert_installments(batch):
    conn = create_database_connection()
    if not conn:
        return

    cursor = conn.cursor()
    query = """
        INSERT INTO BD_Parcelas_Detalhadas (
            NSU, merchantId, saleDate, installmentNumber, installmentQuantity, 
            saleAmount, netAmount, discountAmount, flexFee, mdrAmount, feeTotal,
            authorizationCode, brand, cardNumber, expirationDate, status, paymentId, detalHash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cursor.executemany(query, batch)
        conn.commit()
        print(f"Batch com {len(batch)} parcelas inserido com sucesso no banco de dados.")
    except Exception as e:
        print(f"Erro ao inserir batch no banco de dados: {e}")
    finally:
        conn.close()


def update_installments_status():
    conn = create_database_connection()
    if not conn:
        return

    cursor = conn.cursor()
    try:
        print("Buscando parcelas pendentes para atualização...")
        query = """
            SELECT DISTINCT NSU, merchantId, saleDate
            FROM BD_Parcelas_Detalhadas
            WHERE status NOT IN ('PAID', 'ANTICIPATED')
        """
        cursor.execute(query)
        pending_installments = cursor.fetchall()

        # Inicializar o TokenManager
        token_manager = TokenManager()
        token_manager.update_token()

        print(f"Encontradas {len(pending_installments)} parcelas pendentes para atualização.")

        # Dividir os registros em batches
        total_batches = ceil(len(pending_installments) / BATCH_SIZE)
        processed_batches = 0

        # Variável para somar os tempos dos batches processados
        total_time_spent = 0

        for batch_start in range(0, len(pending_installments), BATCH_SIZE):
                    # Obter o lote atual
                    batch = pending_installments[batch_start:batch_start + BATCH_SIZE]

                    # Medir o tempo de processamento do batch
                    batch_start_time = time.time()

                    # Processar o batch com threads
                    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                        futures = [
                            executor.submit(update_single_installment, row, token_manager)
                            for row in batch
                        ]
                        for future in as_completed(futures):
                            try:
                                future.result()
                            except Exception as e:
                                print(f"Erro ao atualizar uma parcela: {e}")

                    batch_end_time = time.time()
                    batch_time = batch_end_time - batch_start_time
                    total_time_spent += batch_time
                    processed_batches += 1

                    # Calcular o tempo médio real até agora
                    avg_time_per_batch = total_time_spent / processed_batches

                    # Calcular o tempo restante com base nos batches restantes
                    remaining_batches = total_batches - processed_batches
                    estimated_remaining_time = avg_time_per_batch * remaining_batches

                    # Exibir informações sobre o batch e estimativa
                    print(f"Batch {processed_batches}/{total_batches} processado em {batch_time:.2f} segundos.")
                    print(f"Estimativa de tempo restante: {estimated_remaining_time / 60:.2f} minutos.")

    except Exception as e:
        print(f"Erro ao buscar parcelas pendentes: {e}")
    finally:
        conn.close()


def update_single_installment(row, token_manager):
    nsu, merchant_id, sale_date = row
    response = fetch_installments(merchant_id, nsu, sale_date, token_manager)
    if response and "content" in response and "installments" in response["content"]:
        installments = response["content"]["installments"]
        conn = create_database_connection()
        if not conn:
            return
        cursor = conn.cursor()
        try:
            for installment in installments:
                status = installment.get("status")
                installment_number = installment.get("installmentNumber", 0)

                # Verificar o status atual no banco
                select_query = """
                    SELECT status
                    FROM BD_Parcelas_Detalhadas
                    WHERE NSU = ? AND merchantId = ? AND installmentNumber = ?
                """
                cursor.execute(select_query, (nsu, merchant_id, installment_number))
                current_status = cursor.fetchone()

                # Atualizar somente se o status for diferente
                if current_status and current_status[0] != status:
                    update_query = """
                        UPDATE BD_Parcelas_Detalhadas
                        SET status = ?
                        WHERE NSU = ? AND merchantId = ? AND installmentNumber = ?
                    """
                    cursor.execute(update_query, (
                        status,
                        nsu,
                        merchant_id,
                        installment_number
                    ))
                    conn.commit()
                    print(f"Atualizado: NSU {nsu}, Merchant ID {merchant_id}, Parcela {installment_number} para status {status}.")
        except Exception as e:
            print(f"Erro ao atualizar parcelas para NSU {nsu}: {e}")
        finally:
            conn.close()


def process_single_sale(row, token_manager, batch, processed_sales):
    nsu = row.NSU
    merchant_id = row.Numero_Empresa

    if (nsu, merchant_id) in processed_sales:
        print(f"Venda NSU {nsu}, Merchant ID {merchant_id} já processada. Ignorando...")
        return

    try:
        sale_date = datetime.strptime(row.Data_Venda, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as e:
        print(f"Erro ao converter Data_Venda para NSU {nsu}: {e}")
        return

    api_response = fetch_installments(merchant_id, nsu, sale_date, token_manager)
    if api_response and "content" in api_response and "installments" in api_response["content"]:
        installments = api_response["content"]["installments"]

        with BATCH_LOCK:
            for installment in installments:
                amount_info = installment.get("amountInfo", {})
                batch.append((nsu, merchant_id, sale_date,
                            installment.get("installmentNumber", 0),
                            installment.get("installmentQuantity", 0),
                            amount_info.get("amount", 0.0),
                            amount_info.get("netAmount", 0.0),
                            amount_info.get("discountAmount", 0.0),
                            installment.get("flexFee", 0.0),
                            installment.get("mdrAmount", 0.0),
                            installment.get("feeTotal", 0.0),
                            installment.get("authorizationCode", None),
                            installment.get("brand", None),
                            installment.get("cardNumber", None),
                            installment.get("expirationDate", None),
                            installment.get("status", None),
                            installment.get("paymentId", None),
                            installment.get("detaillHash", None)))

            if len(batch) >= BATCH_SIZE:
                insert_installments(batch)
                batch.clear()


def setup_logging():
    log_file = "logparcelasdetalhadas.txt"
    logging.basicConfig(filename=log_file, level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')
    return log_file

def send_completion_email(total_processed_sales, total_new_rows_inserted, total_updated_installments, total_time, log_file):
    try:
        pythoncom.CoInitialize()  # Inicializa a COM
        outlook = Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)  # 0 é o código para e-mails
        mail.To = "mateus.restier@bagaggio.com.br"
        mail.Subject = "AUTOMÁTICO: PROCESSAMENTO DE PARCELAS DETALHADAS CONCLUÍDO"
        mail.Body = (
            "Olá,\n\n"
            "O processamento das vendas foi concluído com sucesso.\n\n"
            f"Total de vendas processadas: {total_processed_sales}\n"
            f"Total de novas linhas inseridas: {total_new_rows_inserted}\n"
            f"Total de parcelas atualizadas: {total_updated_installments}\n"
            f"Tempo total de execução: {total_time:.2f} segundos\n\n"
            "Atenciosamente,\n"
            "Automação"
        )
        # Attach the log file
        attachment = mail.Attachments.Add(log_file)
        attachment.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F", "logparcelasdetalhadas.txt")
        mail.Send()
        logging.info("E-mail enviado para notificar a conclusão do processamento.")
    except Exception as e:
        logging.error(f"Falha ao enviar e-mail de conclusão: {e}")
    finally:
        pythoncom.CoUninitialize()  # Desinicializa a COM
        # Clear the log file
        open(log_file, 'w').close()

def process_sales():
    start_time = time.time()
    log_file = setup_logging()
    conn = create_database_connection()
    if not conn:
        return

    cursor = conn.cursor()

    # Remover linhas duplicadas no início
    remove_duplicate_rows()

    # Inicializar o TokenManager
    token_manager = TokenManager()
    token_manager.update_token()
    if not token_manager.access_token:
        print("Erro ao obter token. Processo encerrado.")
        return

    # Carregar vendas já processadas
    processed_sales = load_processed_sales()

    # Selecionar apenas vendas pendentes (não processadas)
    print("Consultando dados da tabela de vendas...")
    query = """
        SELECT VR.NSU, VR.Numero_Empresa, VR.Data_Venda
        FROM BD_Vendas_Rede VR
        WHERE VR.Parcelas <> 0;
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    # Filtrar apenas vendas não processadas
    pending_sales = [
        row for row in rows if (row.NSU, row.Numero_Empresa) not in processed_sales
    ]

    print(f"Total de vendas encontradas: {len(rows)}")
    print(f"Total de vendas pendentes para processamento: {len(pending_sales)}")

    batch = []
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = [
            executor.submit(process_single_sale, row, token_manager, batch, processed_sales)
            for row in pending_sales
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Erro durante o processamento de uma venda: {e}")

    # Inserir o restante do batch, se existir
    if batch:
        insert_installments(batch)

    # Atualizar parcelas com status pendente
    pending_installments = update_installments_status()

    # Remover linhas duplicadas no final
    remove_duplicate_rows()

    total_processed_sales = len(rows)
    total_new_rows_inserted = len(pending_sales)
    total_updated_installments = len(pending_installments)
    total_time = time.time() - start_time

    print("Processamento concluído.")
    send_completion_email(total_processed_sales, total_new_rows_inserted, total_updated_installments, total_time, log_file)

if __name__ == "__main__":
    process_sales()