#!/bin/bash

datasets=(
    "dblp"
    "ncvoter"
    "musicbrainz"
)

repeat=10
seed=20260907
output="baseline-coleta.txt"

# Limpar arquivo de log anterior
> "$output"

# Cabeçalho
echo "╔════════════════════════════════════════════════════════════════╗" >> "$output"
echo "║         COLETA DE MÉTRICAS DE BASELINE - Experimento 01        ║" >> "$output"
echo "╚════════════════════════════════════════════════════════════════╝" >> "$output"
echo "" >> "$output"
echo "Data/Hora: $(date)" >> "$output"
echo "Datasets: ${datasets[@]}" >> "$output"
echo "Repetições: $repeat, Seed inicial: $seed" >> "$output"
echo "" >> "$output"

for dataset in "${datasets[@]}"; do
    script="experimento-01-base.py"
    csv_file="resultados-exp-01-base-${dataset}.csv"
    
    echo "║ Dataset: $dataset" >> "$output"
    echo "║ CSV:     $csv_file" >> "$output"
    
    uv run "$script" -d "$dataset" -r "$repeat" -s "$seed" -o "$csv_file" >> "$output" 2>&1
    
    # Verificar se arquivo foi criado
    if [ -f "$csv_file" ]; then
        linhas=$(wc -l < "$csv_file")
        echo "✓ Baseline salvo: $csv_file ($linhas linhas)" >> "$output"
    else
        echo "✗ ERRO: Arquivo não foi criado: $csv_file" >> "$output"
        exit 1
    fi
    
    echo "" >> "$output"
done

echo "╔════════════════════════════════════════════════════════════════╗" >> "$output"
echo "║                 ✓ BASELINE COLETADO COM SUCESSO                ║" >> "$output"
echo "╚════════════════════════════════════════════════════════════════╝" >> "$output"
echo "" >> "$output"
echo "Arquivos de baseline criados:" >> "$output"
ls -lh resultados-exp-01-base-*.csv 2>/dev/null >> "$output" || echo "Nenhum arquivo encontrado" >> "$output"
echo "" >> "$output"
echo "Próximo passo: Execute 'bash script-comparacao.sh' para comparar experimentos" >> "$output"
