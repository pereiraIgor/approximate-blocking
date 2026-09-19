import argparse
import math
from collections import deque

import mmh3
import pandas as pd


def str_to_minhash(texto, q, seed):
    return min(mmh3.hash(texto[indice:indice + q], seed) for indice in range(len(texto) - q + 1))


def carregar_registros(dataset_type):
    if dataset_type == "dblp":
        dataframe = pd.read_csv("./00-datasets/DBLP.csv", encoding="utf-8", keep_default_na=False)
        coluna_id = "id"
        atributos = ["title", "authors"]
    elif dataset_type == "ncvoter":
        dataframe = pd.read_csv("./00-datasets/ncvoter42.csv", encoding="utf-8", keep_default_na=False)
        coluna_id = "ncid"
        atributos = ["first_name", "last_name", "registr_dt", "age_at_year_end"]
    elif dataset_type == "musicbrainz":
        dataframe = pd.read_csv("./00-datasets/musicbrainz-200-A01.csv.dapo", encoding="utf-8", keep_default_na=False)
        coluna_id = "TID"
        atributos = ["title", "length", "artist", "album"]
    else:
        raise ValueError("Dataset desconhecido")

    colunas = [coluna_id, *atributos]
    return [
        (registro[0], (" " + " ".join(map(str, registro[1:]))).lower())
        for registro in dataframe[colunas].itertuples(index=False, name=None)
    ]


def registrar_descarte(bloco, tempo_atual, estatisticas):
    for _, tempo_entrada in bloco:
        estatisticas["soma_permanencia"] += tempo_atual - tempo_entrada
        estatisticas["entidades_descartadas"] += 1


def expirar_global(fila_global, blocos, tempo_minimo, tempo_atual, estatisticas):
    while fila_global and fila_global[0][1] < tempo_minimo:
        id_registro, _, chaves = fila_global.popleft()
        for chave, indice_hash in chaves:
            bloco = blocos[indice_hash].get(chave)
            if bloco and bloco[0][0] == id_registro:
                _, tempo_entrada = bloco.popleft()
                estatisticas["soma_permanencia"] += tempo_atual - tempo_entrada
                estatisticas["entidades_descartadas"] += 1


def calcular_tempo_permanencia(registros, estrategia, tamanho_bloco=1000):
    eps = 0.1
    delta = 0.1
    quantidade_hashes = math.ceil(math.log(1 / delta) / (2 * eps**2))
    blocos = [{} for _ in range(quantidade_hashes)]
    estatisticas = {"soma_permanencia": 0, "entidades_descartadas": 0}
    proximo_descarte_por_bloco = {}
    fila_global = deque()

    for tempo_atual, (id_registro, texto) in enumerate(registros[1:], start=1):
        chaves = []
        for indice_hash in range(quantidade_hashes):
            chave = str_to_minhash(texto, 2, indice_hash)
            bloco = blocos[indice_hash].get(chave)
            if bloco is None:
                bloco = blocos[indice_hash][chave] = deque()
                if estrategia == "tempo":
                    proximo_descarte_por_bloco[(chave, indice_hash)] = 50

            if estrategia == "base" and len(bloco) >= tamanho_bloco:
                _, tempo_entrada = bloco.popleft()
                estatisticas["soma_permanencia"] += tempo_atual - tempo_entrada
                estatisticas["entidades_descartadas"] += 1
            elif estrategia == "tempo" and len(bloco) >= tamanho_bloco:
                tempo_minimo = proximo_descarte_por_bloco[(chave, indice_hash)]
                while bloco and bloco[0][1] < tempo_minimo:
                    _, tempo_entrada = bloco.popleft()
                    estatisticas["soma_permanencia"] += tempo_atual - tempo_entrada
                    estatisticas["entidades_descartadas"] += 1
                proximo_descarte_por_bloco[(chave, indice_hash)] = tempo_minimo + 50
            elif estrategia in {"bloco", "global"} and len(bloco) >= tamanho_bloco:
                registrar_descarte(bloco, tempo_atual, estatisticas)
                bloco.clear()

            bloco.append((id_registro, tempo_atual))
            if estrategia == "global":
                chaves.append((chave, indice_hash))

        if estrategia == "global":
            fila_global.append((id_registro, tempo_atual, chaves))
            expirar_global(fila_global, blocos, tempo_atual - 2500, tempo_atual, estatisticas)

    return estatisticas


def main():
    parser = argparse.ArgumentParser(description="Calcula uma vez a permanencia media das entidades descartadas.")
    parser.add_argument("-d", "--dataset-type", choices=["dblp", "ncvoter", "musicbrainz"], default="musicbrainz")
    parser.add_argument("-e", "--estrategia", choices=["base", "tempo", "bloco", "global", "sem-descarte"], required=True)
    parser.add_argument("-w", "--tamanho-bloco", type=int, default=1000)
    args = parser.parse_args()

    registros = carregar_registros(args.dataset_type)
    if args.estrategia == "sem-descarte":
        print("Entidades descartadas: 0")
        print("Tempo medio de permanencia: nao aplicavel")
        return

    estatisticas = calcular_tempo_permanencia(registros, args.estrategia, args.tamanho_bloco)
    total = estatisticas["entidades_descartadas"]
    media = estatisticas["soma_permanencia"] / total if total else None
    print(f"Entidades descartadas: {total}")
    print(f"Tempo medio de permanencia (insercoes): {media if media is not None else 'nao aplicavel'}")


if __name__ == "__main__":
    main()
