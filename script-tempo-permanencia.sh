#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

datasets=(dblp ncvoter musicbrainz)
estrategias=(base tempo bloco global sem-descarte)
script="experimento-06-analise-tempo-permanencia.py"
tamanho_bloco="${TAMANHO_BLOCO:-1000}"
output="${LOG_ARQUIVO:-resultados-tempo-permanencia.txt}"

registrar() {
    printf '%s\n' "$*" | tee -a "$output"
}

: > "$output"

registrar "Analise de tempo de permanencia"
registrar "Data/Hora: $(date)"
registrar "Datasets: ${datasets[*]}"
registrar "Estrategias: ${estrategias[*]}"
registrar "Tamanho do bloco: $tamanho_bloco"
registrar ""

for dataset in "${datasets[@]}"; do
    for estrategia in "${estrategias[@]}"; do
        registrar "Dataset: $dataset | Estrategia: $estrategia"

        if ! uv run python "$script" \
            --dataset-type "$dataset" \
            --estrategia "$estrategia" \
            --tamanho-bloco "$tamanho_bloco" 2>&1 | tee -a "$output"; then
            registrar "ERRO: falha em $dataset com estrategia $estrategia."
            exit 1
        fi

        registrar ""
    done
done

registrar "Analise concluida."
registrar "Log salvo em: $output"