#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

datasets=(dblp ncvoter musicbrainz)
repeticoes="${REPETICOES:-3}"
seed="${SEED:-20260907}"
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
registrar "Repetições: $repeticoes"
registrar "Seed inicial: $seed"
registrar "Tamanho máximo dos blocos: $tamanho_bloco"
registrar ""

for dataset in "${datasets[@]}"; do
    case "$dataset" in
        dblp)
            offsets_a_padrao="50,150,450,1350"
            offsets_b_padrao="1000,3000,9000,27000"
            ;;
        ncvoter)
            offsets_a_padrao="1000,3000,9000,27000"
            offsets_b_padrao="1000,3000,9000,27000"
            ;;
        musicbrainz)
            offsets_a_padrao="5000,15000,45000,135000"
            offsets_b_padrao="5000,15000,45000,135000"
            ;;
    esac
    offset_a="${OFFSETS_A:-$offsets_a_padrao}"
    offset_b="${OFFSETS_B:-$offsets_b_padrao}"
    csv="$saida_dir/resultados-exp-07-diferentes-offsets-${dataset}-${run_id}.csv"

    registrar "Iniciando dataset: $dataset"
    registrar "Offset A: $offset_a"
    registrar "Offset B: $offset_b"
    registrar "CSV: $csv"

    if ! uv run python -u "$script" \
        --dataset-type "$dataset" \
        --repeticoes "$repeticoes" \
        --seed "$seed" \
        --offset-a "$offset_a" \
        --offset-b "$offset_b" \
        --tamanho-bloco "$tamanho_bloco" \
        --saida-csv "$csv" 2>&1 | tee -a "$log"; then
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