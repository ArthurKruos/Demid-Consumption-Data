#!/bin/bash
# Script de inicialização do DEMID
# Uso: ./run.sh

set -e

ROOT="$(cd "$(dirname "$0")" && pwd)"
VENV="$ROOT/.venv"
APP="$ROOT/app.py"

# Garante que o venv existe
if [ ! -f "$VENV/bin/python" ]; then
  echo "❌ Ambiente virtual não encontrado. Execute primeiro:"
  echo "   export PATH=\"\$HOME/.local/bin:\$PATH\""
  echo "   uv venv .venv --python 3.11"
  echo "   uv pip install -r requirements.txt"
  echo "   .venv/bin/python -m spacy download pt_core_news_lg"
  exit 1
fi

echo "✅ Iniciando DEMID em http://localhost:8501"
echo "   Pressione Ctrl+C para parar"
echo ""

"$VENV/bin/streamlit" run "$APP"
