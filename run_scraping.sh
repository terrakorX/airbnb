#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRAPPING_DIR="$SCRIPT_DIR/scraping"
DATA_DIR="$SCRIPT_DIR/data"
DATE_DIR="$DATA_DIR/$(date +%Y-%m-%d)"

echo "=========================================="
echo "SCRAPING AIRBNB DATA"
echo "Date: $(date +%Y-%m-%d_%H:%M:%S)"
echo "=========================================="

mkdir -p "$DATE_DIR"

echo ""
echo "[1/3] Lancement du scraping..."
cd "$SCRIPT_DIR"
source env_data/bin/activate
python "$SCRIPT_DIR/scraping_data.py"

echo ""
echo "[2/3] Déplacement des CSV vers $DATE_DIR..."
if [ -d "$SCRAPPING_DIR/downloads" ]; then
    for csv in "$SCRAPPING_DIR/downloads"/*.csv; do
        if [ -f "$csv" ]; then
            mv "$csv" "$DATE_DIR/"
            echo "  $(basename "$csv") -> $DATE_DIR/"
        fi
    done
else
    echo "  Aucun fichier CSV trouvé dans $SCRAPPING_DIR/downloads"
fi

echo ""
echo "[3/3] Exécution du script de chargement en base..."
source env_data/bin/activate
python "$SCRIPT_DIR/load_airbnb_data.py"

echo ""
echo "=========================================="
echo "TERMINÉ"
echo "Données sauvegardées dans: $DATE_DIR"
echo "=========================================="
