import re
import unicodedata
import spacy

from collections import defaultdict

# =========================
# CARREGAR MODELO
# =========================

nlp = spacy.load(
    "pt_core_news_lg",
    disable=["parser", "tagger", "lemmatizer"]
)

# =========================
# PADRÕES DE RUÍDO
# =========================

PADROES_RUIDO = [
    r'DIÁRIO OFICIAL.*?\n',
    r'ESTADO DA PARAÍBA.*?\n',
    r'Nº\s*[\d\.]+\s*',
    r'Art\.\s*\d+[\wº°]*',
    r'§\s*\d+[\wº°]*',
    r'\bR\$\s*[\d\.,]+',
    r'\d{2}/\d{2}/\d{4}',
    r'CPF[:\s]+[\d\.-]+',
    r'CNPJ[:\s]+[\d\.\/-]+',
]

REGEX_RUIDO = re.compile(
    '|'.join(PADROES_RUIDO),
    re.IGNORECASE
)

TIPOS_ALVO = {'ORG', 'LOC', 'PER', 'PERSON'}

# =========================
# REMOVER ACENTOS
# =========================

def remover_acentos(texto):

    return ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )

# =========================
# LIMPEZA
# =========================

def preprocessar_texto(texto, max_chars=50000):

    if not isinstance(texto, str):
        return ''

    texto = texto[:max_chars]

    texto = remover_acentos(texto)

    texto = texto.lower()

    texto = REGEX_RUIDO.sub(' ', texto)

    texto = re.sub(r'\s+', ' ', texto)

    return texto.strip()

# =========================
# EXTRAIR ENTIDADES
# =========================

def extrair_entidades(texto):

    texto = preprocessar_texto(texto)

    doc = nlp(texto)

    resultado = defaultdict(list)

    for ent in doc.ents:

        if ent.label_ in TIPOS_ALVO:

            entidade = ent.text.strip()

            if len(entidade) > 2:

                resultado[ent.label_].append(entidade)

    return dict(resultado)