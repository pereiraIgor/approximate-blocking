#!/usr/bin/env bash

set -o nounset -o pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

scripts_descarte=(
    "experimento-descarte-velho.py"
)

datasets=(
    "dblp"
    "ncvoter"
    "musicbrainz"
)

repeat="${REPETICOES:-10}"
seed="${SEED:-20260907}"
output="${LOG_ARQUIVO:-comparacao-resultados.txt}"

registrar() {
    printf '%s\n' "$*" | tee -a "$output"
}

validar_csv() {
    local arquivo="$1"
    local linhas
    linhas=$(wc -l < "$arquivo")
    [[ "$linhas" -ge 2 ]]
}

validar_amostra_baseline() {
    local arquivo="$1"
    local linhas
    linhas=$(wc -l < "$arquivo")
    [[ "$linhas" -ge 3 ]]
}

# Limpar arquivo de log anterior
: > "$output"

# Cabeçalho
registrar "Comparacao estatistica com baseline - Experimentos 02-04"
registrar "Data/Hora: $(date)"
registrar "Datasets: ${datasets[*]}"
registrar "Experimentos: ${scripts_descarte[*]}"
registrar "Repeticoes: $repeat, Seed inicial: $seed"
registrar ""
registrar "Verificando arquivos de baseline..."
baseline_count=0
for dataset in "${datasets[@]}"; do
    baseline_csv="resultados-exp-01-base-${dataset}.csv"
    if [[ -f "$baseline_csv" ]] && validar_amostra_baseline "$baseline_csv"; then
        linhas=$(wc -l < "$baseline_csv")
        registrar "Encontrado: $baseline_csv ($linhas linhas)"
        baseline_count=$((baseline_count + 1))
    else
        registrar "ERRO: Baseline ausente ou com menos de duas replicas: $baseline_csv"
        registrar "Execute REPETICOES=10 bash script-baseline.sh primeiro."
        exit 1
    fi
done

if [ "$baseline_count" -ne 3 ]; then
    registrar "ERRO: Esperado: 3 baselines; encontrado: $baseline_count"
    exit 1
fi

registrar "Todos os baselines foram encontrados."
registrar "Iniciando $((${#scripts_descarte[@]} * ${#datasets[@]})) combinacoes."
registrar ""


for script in "${scripts_descarte[@]}"; do
    for dataset in "${datasets[@]}"; do
        exp_name="${script#experimento-}"
        exp_name="${exp_name%.py}"
        csv_file="resultados-exp-${exp_name}-${dataset}.csv"
        baseline_csv="resultados-exp-01-base-${dataset}.csv"
        
        rm -f "$csv_file"
        registrar "Experimento: $exp_name | Dataset: $dataset"
        if ! uv run python "$script" -d "$dataset" -r "$repeat" -s "$seed" -o "$csv_file" -b "$baseline_csv" >> "$output" 2>&1; then
            registrar "ERRO: execucao falhou para $script ($dataset)."
            exit 1
        fi
        if ! [[ -f "$csv_file" ]] || ! validar_csv "$csv_file"; then
            registrar "ERRO: CSV ausente ou sem dados: $csv_file"
            exit 1
        fi
        registrar "Resultados salvos: $csv_file ($(wc -l < "$csv_file") linhas)"
        registrar ""
    done
done

registrar "Comparacao concluida com sucesso."
registrar "Log salvo em: $output"
