"""
Lista curada de canais para o mapeamento PIBIC — DEMID
=======================================================
Comunidades gamers selecionadas para análise de padrões discursivos
associados ao extremismo político (objetivo do projeto de IC).

Fonte: documento "CANAIS EXTREMISTAS" fornecido pela coordenação da pesquisa.

A plataforma permanece capaz de buscar QUALQUER comunidade (módulo de busca
por keyword). Esta lista define o recorte empírico fixo do PIBIC.

IMPORTANTE — nota metodológica:
Os rótulos de categoria abaixo refletem a hipótese de pesquisa registrada no
documento de origem. Servem como ponto de partida para a coleta, NÃO como
classificação validada. A caracterização ideológica efetiva é resultado da
análise de conteúdo (MAXQDA), não um pressuposto da coleta.
"""

# Cada canal: handle/URL, categoria de origem, e observação do documento.
CANAIS_PIBIC = [
    # --- Jogos brasileiros sinalizados no documento de origem ---
    {
        "url": "https://www.youtube.com/@ChiefMilGrauXbox",
        "categoria": "Jogos BR (sinalizado)",
        "obs": "Listado no documento de origem.",
    },
    {
        "url": "https://www.youtube.com/@radeck.hunter",
        "categoria": "Jogos BR (sinalizado)",
        "obs": "Listado no documento de origem.",
    },
    {
        "url": "https://www.youtube.com/@YTJohnWarlock",
        "categoria": "Jogos BR (sinalizado)",
        "obs": "Listado no documento de origem.",
    },
    {
        "url": "https://www.youtube.com/@CENTRALOficial",
        "categoria": "Jogos BR (sinalizado)",
        "obs": "Listado no documento de origem.",
    },
    {
        "url": "https://www.youtube.com/user/Gameplayrj",
        "categoria": "Jogos BR (sinalizado)",
        "obs": "Canal com username legado (não-handle).",
    },

    # --- Asmongold (streamer internacional) ---
    {
        "url": "https://www.youtube.com/@AsmonTV",
        "categoria": "Asmongold (streamer)",
        "obs": "Canal oficial. Falas conservadoras sobre questões políticas.",
    },
    {
        "url": "https://www.youtube.com/@AsmongoldClips",
        "categoria": "Asmongold (streamer)",
        "obs": "Canal de clipes.",
    },

    # --- Comunidade Magic: The Gathering ---
    {
        "url": "https://www.youtube.com/@nerdcrypt",
        "categoria": "Magic: The Gathering",
        "obs": "Crítico a aspectos artísticos e parcerias do jogo.",
    },
    {
        "url": "https://www.youtube.com/@RedBobcatGames",
        "categoria": "Magic: The Gathering",
        "obs": "Crítico a mudanças atuais no jogo.",
    },
    {
        "url": "https://www.youtube.com/@TolarianCommunityCollege",
        "categoria": "Magic: The Gathering",
        "obs": "Canal de referência da comunidade Magic.",
    },
    {
        "url": "https://www.youtube.com/@wayfadedmagic",
        "categoria": "Magic: The Gathering",
        "obs": "Crítico à direção artística do jogo.",
    },
    {
        "url": "https://www.youtube.com/@Thorge007",
        "categoria": "Magic: The Gathering",
        "obs": "Crítico às parcerias do jogo.",
    },
]

# Vídeos avulsos de interesse citados no documento (não pertencem a canais
# da lista, mas foram destacados para análise pontual).
VIDEOS_AVULSOS = [
    {"video_id": "P4QSjBzdp3E", "categoria": "Asmongold (streamer)", "obs": "Reação a adaptação de personagem."},
    {"video_id": "Gk_htz6ufHA", "categoria": "Magic: The Gathering", "obs": "Vídeo crítico (RedBobcatGames)."},
    {"video_id": "E0KOGsYAqXo", "categoria": "Magic: The Gathering", "obs": "Sobre as artes (nerdcrypt)."},
    {"video_id": "liiroyIADjg", "categoria": "Magic: The Gathering", "obs": "Sobre as parcerias (nerdcrypt)."},
    {"video_id": "zmrYq_KMl0c", "categoria": "Magic: The Gathering", "obs": "Direção de arte (wayfadedmagic)."},
    {"video_id": "Zns5WjLZ0zg", "categoria": "Magic: The Gathering", "obs": "Parcerias (Thorge007)."},
    {"video_id": "bbcPyiRnRJk", "categoria": "Magic: The Gathering", "obs": "Outros vídeos (Thorge007)."},
    {"video_id": "pbnJG_NAO1w", "categoria": "Pokémon", "obs": "Preocupação com 'Pokémon woke'."},
]


def listar_categorias():
    """Retorna as categorias únicas presentes na lista curada."""
    return sorted({c["categoria"] for c in CANAIS_PIBIC})


def canais_por_categoria(categoria):
    """Filtra canais por categoria."""
    return [c for c in CANAIS_PIBIC if c["categoria"] == categoria]
