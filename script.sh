#!/bin/bash

scripts=(
    "experimento-01-base.py"
    "experimento-02-descarte-tempo.py"
    "experimento-03-descarte-bloco.py"
    "experimento-04-descarte-global.py"
)

datasets=(
    "dblp"
    "ncvoter"
    "musicbrainz"
)

repeat=10
seed=20260907
output="results.txt"

for script in "${scripts[@]}"; do
    for dataset in "${datasets[@]}"; do
        # Extrair nome do experimento (ex: experimento-01-base.py -> 01-base)
        exp_name=$(echo "$script" | sed 's/experimento-//' | sed 's/\.py//')
        
        # Nome do arquivo CSV para esta combinação
        csv_file="resultados-exp-${exp_name}-${dataset}.csv"
        
        echo "========================================"
        echo "Script:  $script"
        echo "Dataset: $dataset"
        echo "CSV:     $csv_file"
        echo "========================================"
        
        uv run "$script" -d "$dataset" -r "$repeat" -s "$seed" -o "$csv_file"
    done
done