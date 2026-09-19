#!/usr/bin/env bash

set -o nounset -o pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

datasets=(dblp ncvoter musicbrainz)
repeticoes="${REPETICOES:-10}"
seed="${SEED:-20260907}"
output="${LOG_ARQUIVO:-baseline-coleta.txt}"
script="experimento-01-base.py"

registrar() {
    printf '%s\n' "$*" | tee -a "$output"
}

validar_csv() {
    local arquivo="$1"
    local linhas
    linhas=$(wc -l < "$arquivo")
    [[ "$linhas" -ge 2 ]]
}

: > "$output"
registrar "Coleta de metricas de baseline - Experimento 01"
registrar "Data/Hora: $(date)"
registrar "Datasets: ${datasets[*]}"
registrar "Repeticoes: $repeticoes, Seed inicial: $seed"
registrar ""

for dataset in "${datasets[@]}"; do
    csv_file="resultados-exp-01-base-${dataset}.csv"
    rm -f "$csv_file"

    registrar "Dataset: $dataset"
    registrar "CSV: $csv_file"
    if ! uv run python "$script" -d "$dataset" -r "$repeticoes" -s "$seed" -o "$csv_file" >> "$output" 2>&1; then
        registrar "ERRO: execucao falhou para $dataset."
        exit 1
    fi
    if ! [[ -f "$csv_file" ]] || ! validar_csv "$csv_file"; then
        registrar "ERRO: CSV ausente ou sem dados: $csv_file"
        exit 1
    fi
    registrar "Baseline salvo: $csv_file ($(wc -l < "$csv_file") linhas)"
    registrar ""
done

registrar "Baseline coletado com sucesso."
registrar "Proximo passo: bash script-comparacao.sh"
