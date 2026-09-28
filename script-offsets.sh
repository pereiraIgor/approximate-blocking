#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

datasets=(dblp ncvoter musicbrainz)
repeticoes="${REPETICOES:-10}"
seed="${SEED:-20260907}"
offset_a="${OFFSETS_A:-1000,3000,9000,27000}"
offset_b="${OFFSETS_B:-1000,3000,9000,27000}"
tamanho_bloco="${TAMANHO_BLOCO:-1000}"
saida_dir="${SAIDA_DIR:-resultados_v2}"
run_id="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
log="${LOG_ARQUIVO:-$saida_dir/execucao-offsets-$run_id.log}"
script="experimento-07-diferentes-offsets.py"

mkdir -p "$saida_dir"

registrar() {
    printf '%s\n' "$*" | tee -a "$log"
}

registrar "Experimento de offsets"
registrar "Data/hora: $(date)"
registrar "Datasets: ${datasets[*]}"
registrar "Offset A: $offset_a"
registrar "Offset B: $offset_b"
registrar "Repetições: $repeticoes"
registrar "Seed inicial: $seed"
registrar "Tamanho máximo dos blocos: $tamanho_bloco"
registrar ""

for dataset in "${datasets[@]}"; do
    csv="$saida_dir/resultados-exp-07-diferentes-offsets-${dataset}-${run_id}.csv"

    registrar "Iniciando dataset: $dataset"
    registrar "CSV: $csv"

    if ! uv run python "$script" \
        --dataset-type "$dataset" \
        --repeticoes "$repeticoes" \
        --seed "$seed" \
        --offset-a "$offset_a" \
        --offset-b "$offset_b" \
        --tamanho-bloco "$tamanho_bloco" \
        --saida-csv "$csv" >>"$log" 2>&1; then
        registrar "ERRO: execução falhou para $dataset."
        exit 1
    fi

    if [[ ! -s "$csv" ]]; then
        registrar "ERRO: CSV ausente ou vazio: $csv"
        exit 1
    fi

    registrar "Concluído: $dataset"
    registrar ""
done

registrar "Experimento concluído."
registrar "Log: $log"