#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

datasets=(dblp ncvoter musicbrainz)
repeticoes="${REPETICOES:-3}"
seed="${SEED:-20260907}"
saida_dir="${SAIDA_DIR:-resultados_v2}"
run_id="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
log="${LOG_ARQUIVO:-$saida_dir/execucao-exp-05-sem-descarte-$run_id.log}"
script="experimento-05-sem-descarte.py"

if ! [[ "$repeticoes" =~ ^[0-9]+$ ]] || (( repeticoes < 2 )); then
    printf 'REPETICOES deve ser um inteiro maior ou igual a 2.\n' >&2
    exit 1
fi

mkdir -p "$saida_dir"

registrar() {
    printf '%s\n' "$*" | tee -a "$log"
}

registrar "Experimento 05 - Sem descarte"
registrar "Data/hora: $(date)"
registrar "Datasets: ${datasets[*]}"
registrar "Repetições por dataset: $repeticoes"
registrar "Seed inicial: $seed"
registrar ""

for dataset in "${datasets[@]}"; do
    csv="$saida_dir/resultados-exp-05-sem-descarte-${dataset}-${run_id}.csv"

    registrar "Dataset: $dataset"
    registrar "CSV: $csv"

    if ! uv run python -u "$script" \
        --dataset-type "$dataset" \
        --repeticoes "$repeticoes" \
        --seed "$seed" \
        --saida-csv "$csv" 2>&1 | tee -a "$log"; then
        registrar "ERRO: execução falhou para $dataset."
        exit 1
    fi

    if [[ ! -s "$csv" ]]; then
        registrar "ERRO: CSV ausente ou vazio: $csv"
        exit 1
    fi

    linhas=$(wc -l < "$csv")
    if [[ "$linhas" -lt $((repeticoes + 1)) ]]; then
        registrar "ERRO: CSV contém menos réplicas que o esperado: $csv"
        exit 1
    fi

    registrar "Concluído: $dataset ($csv)"
    registrar ""
done

registrar "Experimento concluído."
registrar "Log: $log"