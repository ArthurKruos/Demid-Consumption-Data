"""
Lista-semente de subreddits para o mapeamento PIBIC — DEMID
============================================================
Esta lista é apenas um PONTO DE PARTIDA editável. A plataforma permite
escolher/adicionar quaisquer subreddits diretamente na interface.

Cada item: {"nome": "<subreddit sem r/>", "categoria": "<rótulo>"}

NOTA METODOLÓGICA: os rótulos de categoria são organizacionais, não
classificações validadas. A caracterização é resultado da análise de
conteúdo, não um pressuposto da coleta.
"""

SUBREDDITS_PIBIC = [
    # Magic: The Gathering (espelha o recorte do YouTube)
    {"nome": "magicTCG", "categoria": "Magic: The Gathering"},
    {"nome": "freemagic", "categoria": "Magic: The Gathering"},

    # Pokémon
    {"nome": "pokemon", "categoria": "Pokémon"},

    # Gaming em geral / cultura
    {"nome": "gaming", "categoria": "Gaming (geral)"},
    {"nome": "Games", "categoria": "Gaming (geral)"},

    # Gaming brasileiro
    {"nome": "gamesEcultura", "categoria": "Jogos BR"},
    {"nome": "brasilivre", "categoria": "Jogos BR"},
]


def listar_categorias():
    return sorted({s["categoria"] for s in SUBREDDITS_PIBIC})
