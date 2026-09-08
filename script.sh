#!/bin/bash

# Experimentos para testar
scripts_base=("experimento-01-base.py")
scripts_descarte=(
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

echo "========================================"
echo "ETAPA 1: Executando Baseline (Experimento 01)"
echo "========================================"

# Executar baseline primeiro
for dataset in "${datasets[@]}"; do
    script="${scripts_base[0]}"
    exp_name=$(echo "$script" | sed 's/experimento-//' | sed 's/\.py//')
    csv_file="resultados-exp-${exp_name}-${dataset}.csv"
    
    echo "========================================"
    echo "Script:  $script"
    echo "Dataset: $dataset"
    echo "CSV:     $csv_file"
    echo "========================================"
    
    uv run "$script" -d "$dataset" -r "$repeat" -s "$seed" -o "$csv_file" >> "$output" 2>&1
done

echo ""
echo "========================================"
echo "ETAPA 2: Comparando com Baseline"
echo "========================================"

# Executar experimentos com descarte, comparando com baseline
for script in "${scripts_descarte[@]}"; do
    for dataset in "${datasets[@]}"; do
        exp_name=$(echo "$script" | sed 's/experimento-//' | sed 's/\.py//')
        csv_file="resultados-exp-${exp_name}-${dataset}.csv"
        
        # CSV do baseline para comparação
        baseline_csv="resultados-exp-01-base-${dataset}.csv"
        
        echo "========================================"
        echo "Script:   $script"
        echo "Dataset:  $dataset"
        echo "CSV:      $csv_file"
        echo "Baseline: $baseline_csv"
        echo "========================================"
        
        uv run "$script" -d "$dataset" -r "$repeat" -s "$seed" -o "$csv_file" -b "$baseline_csv" >> "$output" 2>&1
    done
done