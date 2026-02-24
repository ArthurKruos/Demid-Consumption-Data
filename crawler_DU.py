import os
import time
import random
import requests
import pdfplumber
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

# ==========================================================
# CONFIGURAÇÕES
# ==========================================================

ANO = 2025  # <- altere o ano aqui
PDF_DIR = Path("pdfs_doe_auniao")
PDF_DIR.mkdir(exist_ok=True)

# Site segue este padrão:
# https://auniao.pb.gov.br/servicos/doe/{ano}-{1}/{mes}/diario-oficial-DD-MM-ANO-portal.pdf

MESES = {
    1: "janeiro",
    2: "fevereiro",
    3: "marco",
    4: "abril",
    5: "maio",
    6: "junho",
    7: "julho",
    8: "agosto",
    9: "setembro",
    10: "outubro",
    11: "novembro",
    12: "dezembro"
}

# ==========================================================
# BANCO DE DADOS
# ==========================================================

def inicializar_banco():
    conn = sqlite3.connect("doe_auniao.db")
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS doe_extracao (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_doe TEXT UNIQUE,
            mes TEXT,
            data_extracao TEXT,
            nome_pdf TEXT,
            conteudo TEXT
        )
    """)

    conn.commit()
    conn.close()

def salvar_no_banco(data_doe, nome_pdf, conteudo):
    conn = sqlite3.connect("doe_auniao.db")
    c = conn.cursor()

    # INSERT OR IGNORE evita duplicidade baseada na coluna UNIQUE
    c.execute("""
        INSERT OR IGNORE INTO doe_extracao (data_doe, mes, data_extracao, nome_pdf, conteudo)
        VALUES (?, ?, ?, ?, ?)
    """, (
        data_doe,
        f"{data_doe[5:7]}/{data_doe[0:4]}",
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        nome_pdf,
        conteudo
    ))

    if c.rowcount == 0:
        print(f"⚠️ Aviso: {data_doe} já existe no banco. Pulando inserção.")
    else:
        print(f"💾 {data_doe} salvo com sucesso!")

    conn.commit()
    conn.close()

# ==========================================================
# UTILITÁRIOS SEGUROS
# ==========================================================

def pausa_segura():
    """
    Intervalo aleatório entre 1.2 e 3.0 segundos
    Para não sobrecarregar o servidor.
    """
    time.sleep(random.uniform(1.2, 3.0))

def baixar_seguro(url, caminho):
    """
    Download com user-agent, timeout, retries e pausas.
    """
    headers = {
        "User-Agent": "CrawlerSeguro-DO-PB/1.0 (email@example.com)"
    }

    for tentativa in range(3):
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            if resp.status_code == 200:
                with open(caminho, "wb") as f:
                    f.write(resp.content)
                return True

            elif resp.status_code == 404:
                return False  # PDF realmente não existe

            else:
                print(f"⚠️ Erro {resp.status_code} ao acessar {url}")
                pausa_segura()

        except requests.exceptions.RequestException:
            print("⚠️ Conexão falhou. Tentando novamente...")
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

def buscar_termo(termo):
    """
    Busca uma palavra ou frase nos diários oficiais salvos.
    """
    conn = sqlite3.connect("doe_auniao.db")
    c = conn.cursor()

    # O operador LIKE com % busca o termo em qualquer lugar do texto
    query = """
        SELECT data_doe, nome_pdf 
        FROM doe_extracao 
        WHERE conteudo LIKE ? 
        ORDER BY data_doe DESC
    """
    
    c.execute(query, (f'%{termo}%',))
    resultados = c.fetchall()
    conn.close()

    if resultados:
        print(f"\n🔍 Foram encontrados {len(resultados)} editais contendo '{termo}':")
        for data, arquivo in resultados:
            print(f"📌 {data} - Arquivo: {arquivo}")
    else:
        print(f"\n❌ Nenhuma menção a '{termo}' foi encontrada no banco.")

# ==========================================================
# CRAWLER PRINCIPAL
# ==========================================================

def executar_crawler(ANO): 
    inicializar_banco()

    inicio = datetime(ANO, 1, 1)
    fim = datetime(ANO, 12, 31)

    atual = inicio
    while atual <= fim:
        dia = atual.day
        mes_num = atual.month
        mes_nome = MESES[mes_num]

        nome_pdf = f"diario-oficial-{dia:02d}-{mes_num:02d}-{ANO}-portal.pdf"
        url = f"https://auniao.pb.gov.br/servicos/doe/{ANO}-1/{mes_nome}/{nome_pdf}"

        caminho_arquivo = PDF_DIR / nome_pdf

        print(f"\n📅 Tentando baixar: {url}")

        if caminho_arquivo.exists():
            print("✔️ Já existe, pulando download.")
        else:
            sucesso = baixar_seguro(url, caminho_arquivo)
            if not sucesso:
                print("❌ PDF não encontrado.")
                atual += timedelta(days=1)
                continue

            print("✔️ Baixado com sucesso.")
            pausa_segura()

        # Extrair texto
        texto = extrair_texto_pdf(caminho_arquivo)

        # Salvar no banco
        salvar_no_banco(atual.strftime("%Y-%m-%d"), nome_pdf, texto)
        print("💾 Salvo no banco com sucesso!")

        atual += timedelta(days=1)

if __name__ == "__main__":
    executar_crawler()
