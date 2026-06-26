# Guia do Usuário — Plataforma DEMID

*Para qualquer pessoa entender o que a plataforma faz e como usar cada parte — sem precisar saber programação.*

---

## O que é esta plataforma?

A DEMID é uma ferramenta de pesquisa que **coleta e analisa comunidades digitais**, com foco em **comunidades gamers do YouTube**. Ela ajuda a responder perguntas como:

- Quem são as pessoas mais ativas numa comunidade?
- Quais comunidades têm o mesmo público (se "conversam" entre si)?
- Que palavras e expressões essas pessoas mais usam?
- Como um termo específico (ex.: "woke") aparece nas conversas?

O objetivo é dar **material empírico** para análise de discurso e mapeamento de comunidades — por exemplo, em um projeto de iniciação científica.

---

## Como abrir a plataforma

Quem cuida da parte técnica roda um comando e te passa um endereço como **http://localhost:8501**. Você abre esse endereço no navegador (Chrome, Safari, etc.) e a plataforma aparece.

No lado esquerdo há um **menu** com 4 partes. A principal é a primeira:

| Menu | Para que serve |
|------|----------------|
| **Mapeamento YouTube** | ⭐ O foco do projeto. Coleta e analisa canais do YouTube. |
| **Mapeamento Reddit (opcional)** | Mesmo tipo de análise, mas para o Reddit (exige cadastro de API). |
| **Redes Sociais (coleta)** | Busca por palavra-chave em várias redes (uso geral). |
| **Diário Oficial** | Outra frente da pesquisa: políticas públicas sobre jogos. |

---

## Entendendo a ideia central: 2 etapas

Tudo funciona em **duas etapas separadas**:

1. **COLETAR** — você escolhe os canais e a plataforma baixa os vídeos e comentários da internet. Isso fica **guardado**.
2. **ANALISAR** — a plataforma faz as contas em cima do que **já foi guardado**.

> 💡 **Importante:** as análises mostram **apenas o que já foi coletado**. Se um canal nunca foi coletado, ele não aparece nas análises. Para incluí-lo, basta coletá-lo (explicado abaixo).

---

## Módulo "Mapeamento YouTube" (o principal)

### Como coletar canais

Na barra lateral esquerda, você escolhe a **fonte dos canais**:

- **Lista curada (PIBIC)** — uma lista de canais já preparada para a pesquisa. Você marca as categorias que quer.
- **Digitar meus canais** — você cola **qualquer** canal do YouTube que quiser analisar.

Para digitar um canal novo, use o formato:
```
https://www.youtube.com/@nomedocanal | NomeDaCategoria
```
Um canal por linha. A "categoria" é só um rótulo seu para organizar (ex.: "Magic", "Streamers BR").

Depois ajuste:
- **Vídeos por canal** — quantos vídeos baixar de cada canal
- **Coletar comentários** — se quer baixar os comentários (recomendado)
- **Comentários por vídeo** — limite por vídeo

E clique em **🚀 Iniciar Coleta**. Uma barra de progresso mostra o andamento.

> Os dados novos são **somados** aos antigos — coletar de novo não apaga o que já existe.

### As 6 abas de análise

Depois de coletar (ou se já há dados), aparecem **6 abas**. Aqui está o que cada uma mostra:

#### 📊 Visão geral
O retrato rápido: **quantos vídeos, comentários e canais** existem, um gráfico de **vídeos por categoria** e uma amostra dos vídeos. Use para ter a dimensão geral da base.

#### 👥 Público-alvo
Mostra **quem mais comenta** em cada comunidade — o "núcleo" de pessoas engajadas.
- A coluna **canais_distintos** revela quem comenta em vários canais ao mesmo tempo (pessoas que circulam entre comunidades).
- O **perfil por categoria** indica se a comunidade é concentrada (poucas pessoas muito ativas) ou dispersa (muita gente comentando pouco).

*Por que importa:* ajuda a identificar lideranças e o tipo de público de cada comunidade.

#### 🕸️ Mapa de comunidades
**A peça central do mapeamento.** Um gráfico de "bolinhas e linhas" onde:
- cada **bolinha é um canal**;
- uma **linha liga dois canais quando eles têm comentaristas em comum**.

Quanto mais grossa a ligação, mais público compartilhado. Isso revela, **com dados**, quais comunidades se sobrepõem — por exemplo, se o público de um canal de Magic também frequenta um canal de streamer.
- **Canais mais centrais** = os mais conectados (pontos de encontro).
- **Autores-ponte** = pessoas que transitam entre comunidades.

*Por que importa:* é o que transforma "canais soltos" em um **mapa de comunidades** de verdade.

#### 🗣️ Discurso (termos & KWIC)
Ferramentas para olhar **a linguagem** das comunidades:
- **Top termos** — as palavras mais usadas (já removendo palavras vazias como "que", "the", "para").
- **Bigramas/Trigramas** — as **expressões** de 2 ou 3 palavras mais repetidas (ex.: "secret lair", "they are").
- **Concordância (KWIC)** — você digita um termo (ex.: "woke") e vê **a frase ao redor** de cada vez que ele aparece. É a principal ferramenta para **análise de discurso**: mostra *como* a palavra é usada, não só quantas vezes.

*Por que importa:* revela os "repertórios linguísticos" da comunidade — como as pessoas falam, que termos codificados usam.

#### 🔍 Marcadores
Conta quantas vezes aparecem **termos de interesse** agrupados por tema (ex.: marcadores identitários, de diversidade, de hostilidade). Mostra também os **comentários com mais marcadores**, que são bons candidatos para leitura detalhada.

> ⚠️ **Atenção (importante):** essa aba **não rotula ninguém** nem classifica ideologia. Ela apenas **localiza palavras** para você inspecionar. A interpretação é sempre sua, como pesquisador(a). A lista de termos é editável.

*Por que importa:* ajuda a encontrar rapidamente os trechos que merecem análise qualitativa aprofundada.

#### 📤 Exportar
Gera um arquivo **CSV pronto para o MAXQDA** (software de análise de conteúdo). Cada linha é um "documento" (um comentário ou um vídeo) com suas informações: canal, categoria, autor, data, curtidas. Também dá para baixar os dados brutos.

*Por que importa:* conecta a coleta automática com a análise qualitativa manual que a metodologia da pesquisa prevê.

---

## Módulo "Mapeamento Reddit (opcional)"

Funciona **igualzinho** ao YouTube (as mesmas 6 abas), mas para subreddits do Reddit. A única diferença: o Reddit **exige um cadastro gratuito de desenvolvedor** para liberar o acesso. Se você tiver as credenciais (Client ID e Secret), cola na barra lateral e usa. Se não tiver, pode ignorar este módulo — o foco do projeto é o YouTube.

---

## Módulo "Redes Sociais (coleta)"

Uma busca mais geral: você digita palavras-chave e ele procura em Reddit, YouTube, Twitter/X e Telegram. É um coletor de uso amplo, separado do mapeamento focado de comunidades.

---

## Módulo "Diário Oficial"

Outra frente da pesquisa, não ligada às redes sociais: analisa documentos do Diário Oficial da Paraíba em busca de menções a **políticas públicas sobre jogos digitais**, com gráficos de evolução ao longo do tempo.

---

## Perguntas frequentes

**As análises usam dados da internet ao vivo?**
Não. Elas usam só o que já foi **coletado e guardado**. Coletar e analisar são passos separados.

**Se eu coletar um canal novo, perco o que já tinha?**
Não. O novo é **somado** ao que já existe.

**Posso analisar um canal que não está na lista da pesquisa?**
Sim. Use a opção **"Digitar meus canais"** e cole o endereço do canal.

**Preciso de senha/API para o YouTube?**
Não. O YouTube funciona **sem nenhum cadastro**. Só o Reddit exige.

**Onde ficam os dados guardados?**
Em arquivos dentro da pasta `data/` do projeto (formato `.parquet`).

---

*Para detalhes de programação, veja `docs/DOCUMENTACAO_TECNICA.md`.*
