#!/bin/bash

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
output="comparacao-resultados.txt"

# Limpar arquivo de log anterior
> "$output"

# Cabeçalho
echo "╔════════════════════════════════════════════════════════════════╗" >> "$output"
echo "║     COMPARAÇÃO ESTATÍSTICA COM BASELINE - Experimentos 02-04    ║" >> "$output"
echo "╚════════════════════════════════════════════════════════════════╝" >> "$output"
echo "" >> "$output"
echo "Data/Hora: $(date)" >> "$output"
echo "Datasets: ${datasets[@]}" >> "$output"
echo "Experimentos: ${scripts_descarte[@]}" >> "$output"
echo "Repetições: $repeat, Seed inicial: $seed" >> "$output"
echo "" >> "$output"
echo "Verificando arquivos de baseline..." >> "$output"
baseline_count=0
for dataset in "${datasets[@]}"; do
    baseline_csv="resultados-exp-01-base-${dataset}.csv"
    if [ -f "$baseline_csv" ]; then
        linhas=$(wc -l < "$baseline_csv")
        echo "✓ Encontrado: $baseline_csv ($linhas linhas)" >> "$output"
        baseline_count=$((baseline_count + 1))
    else
        echo "✗ ERRO: Arquivo de baseline não encontrado: $baseline_csv" >> "$output"
        echo "   Execute 'bash script-baseline.sh' primeiro!" >> "$output"
        exit 1
    fi
done

if [ "$baseline_count" -ne 3 ]; then
    echo "" >> "$output"
    echo "✗ ERRO: Faltam arquivos de baseline!" >> "$output"
    echo "   Esperado: 3 arquivos, encontrado: $baseline_count" >> "$output"
    exit 1
fi

echo "" >> "$output"
echo "✓ Todos os baseline foram encontrados!" >> "$output"
echo "" >> "$output"
echo "Iniciando comparação de $((${#scripts_descarte[@]} * ${#datasets[@]})) combinações..." >> "$output"
echo "Configuração: repeticoes=$repeat, seed=$seed" >> "$output"
echo "" >> "$output"


for script in "${scripts_descarte[@]}"; do
    for dataset in "${datasets[@]}"; do
        exp_name=$(echo "$script" | sed 's/experimento-//' | sed 's/\.py//')
        csv_file="resultados-exp-${exp_name}-${dataset}.csv"
        baseline_csv="resultados-exp-01-base-${dataset}.csv"
        
        echo "╔════════════════════════════════════════════════════════════════╗" >> "$output"
        echo "║ Experimento: $exp_name" >> "$output"
        echo "║ Dataset:     $dataset" >> "$output"
        echo "║ Arquivo:     $csv_file" >> "$output"
        echo "║ Baseline:    $baseline_csv" >> "$output"
        echo "╚════════════════════════════════════════════════════════════════╝" >> "$output"
        
        uv run "$script" -d "$dataset" -r "$repeat" -s "$seed" -o "$csv_file" -b "$baseline_csv" >> "$output" 2>&1
        
        # Verificar se arquivo foi criado
        if [ -f "$csv_file" ]; then
            linhas=$(wc -l < "$csv_file")
            echo "✓ Resultados salvos: $csv_file ($linhas linhas)" >> "$output"
        else
            echo "✗ ERRO: Arquivo não foi criado: $csv_file" >> "$output"
        fi
        
        echo "" >> "$output"
    done
done

echo "╔════════════════════════════════════════════════════════════════╗" >> "$output"
echo "║              ✓ COMPARAÇÃO CONCLUÍDA COM SUCESSO                ║" >> "$output"
echo "╚════════════════════════════════════════════════════════════════╝" >> "$output"
echo "" >> "$output"
echo "Arquivos de comparação criados:" >> "$output"
ls -lh resultados-exp-0[2-4]-*.csv 2>/dev/null | wc -l | xargs echo "Total de arquivos:" >> "$output"
echo "" >> "$output"
echo "Resumo dos logs:" >> "$output"
echo "  - Baseline coletado em:  baseline-coleta.txt" >> "$output"
echo "  - Resultados em:        comparacao-resultados.txt" >> "$output"
echo "" >> "$output"
echo "Para visualizar os resultados:" >> "$output"
echo "  tail -100 comparacao-resultados.txt | grep -A 10 'Comparação com Baseline'" >> "$output"
