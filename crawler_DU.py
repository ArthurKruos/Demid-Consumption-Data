import time
import random
import requests
import pdfplumber
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# ==========================================================
# CONFIGURAÇÕES
# ==========================================================

ANO = 2025

PDF_DIR = Path("pdfs_doe_auniao")
PDF_DIR.mkdir(exist_ok=True)

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

PARQUET_PATH = DATA_DIR / "doe_raw.parquet"

MESES = {
    1: "janeiro", 2: "fevereiro", 3: "marco", 4: "abril",
    5: "maio", 6: "junho", 7: "julho", 8: "agosto",
    9: "setembro", 10: "outubro", 11: "novembro", 12: "dezembro"
}

# ==========================================================
# UTILITÁRIOS
# ==========================================================

def pausa_segura():
    time.sleep(random.uniform(1.2, 3.0))

def baixar_seguro(url, caminho):
    headers = {
        "User-Agent": "CrawlerSeguro-DO-PB/2.0 (contato@exemplo.com)"
    }

    for _ in range(3):
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            if resp.status_code == 200:
                with open(caminho, "wb") as f:
                    f.write(resp.content)
                return True
            elif resp.status_code == 404:
                return False
        except requests.exceptions.RequestException:
            pausa_segura()

    return False

def extrair_texto_pdf(caminho):
    texto = ""
    with pdfplumber.open(caminho) as pdf:
        for pagina in pdf.pages:
            t = pagina.extract_text()
            if t:
                texto += t + "\n"
    return texto.strip()

# ==========================================================
# CONTROLE INCREMENTAL
# ==========================================================

def obter_ultima_data_salva():
    if not PARQUET_PATH.exists():
        return None

    df = pd.read_parquet(PARQUET_PATH)
    if df.empty:
        return None

    return pd.to_datetime(df["data_doe"]).max()

def salvar_parquet_incremental(novo_df):
    if PARQUET_PATH.exists():
        df_existente = pd.read_parquet(PARQUET_PATH)
        df_final = pd.concat([df_existente, novo_df], ignore_index=True)
        df_final = df_final.drop_duplicates(subset=["data_doe"])
    else:
        df_final = novo_df

    df_final.to_parquet(
        PARQUET_PATH,
        engine="pyarrow",
        compression="zstd",
        index=False
    )

# ==========================================================
# CRAWLER PRINCIPAL INTELIGENTE
# ==========================================================

def executar_crawler(ANO):

    ultima_data = obter_ultima_data_salva()

    # ==============================================
    # PRIMEIRA EXECUÇÃO → varrer ano inteiro
    # ==============================================
    if not ultima_data:
        print("📂 Nenhum dado encontrado. Iniciando varredura completa do ano.")
        inicio = datetime(ANO, 1, 1)
        fim = datetime(ANO, 12, 31)
        modo_incremental = False
    else:
        print(f"🔎 Última data salva: {ultima_data.date()}")
        inicio = ultima_data + timedelta(days=1)
        fim = datetime.now()
        modo_incremental = True
        print(f"➡️ Buscando a partir de: {inicio.date()}")

    atual = inicio
    registros = []

    while atual <= fim:

        dia = atual.day
        mes_num = atual.month
        mes_nome = MESES[mes_num]

        nome_pdf = f"diario-oficial-{dia:02d}-{mes_num:02d}-{ANO}-portal.pdf"
        url = f"https://auniao.pb.gov.br/servicos/doe/{ANO}-1/{mes_nome}/{nome_pdf}"
        caminho_arquivo = PDF_DIR / nome_pdf

        print(f"\n📅 Verificando: {url}")

        sucesso = baixar_seguro(url, caminho_arquivo)

        if not sucesso:
            print("❌ Não existe edição neste dia.")
            
            # Se for modo incremental, parar no primeiro 404
            if modo_incremental:
                print("⛔ Nenhum novo DOE encontrado. Encerrando execução.")
                break

            atual += timedelta(days=1)
            continue

        pausa_segura()

        texto = extrair_texto_pdf(caminho_arquivo)

        registros.append({
            "data_doe": atual.strftime("%Y-%m-%d"),
            "mes": f"{mes_num:02d}/{ANO}",
            "data_extracao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "nome_pdf": nome_pdf,
            "conteudo": texto
        })

        print("✔️ DOE salvo com sucesso!")

        atual += timedelta(days=1)

    if registros:
        novo_df = pd.DataFrame(registros)
        salvar_parquet_incremental(novo_df)
        print("💾 Parquet atualizado com novos registros!")
    else:
        print("✅ Nenhuma atualização necessária.")
# ==========================================================
if __name__ == "__main__":
    executar_crawler(ANO)