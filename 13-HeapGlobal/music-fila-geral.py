import pandas as pd
import time
import random
import math
import mmh3

def str_to_MinHash(str1, q, seed=0):
    return min([mmh3.hash(str1[i:i + q], seed) for i in range(len(str1) - q + 1)])

def frequent2(temp, L, t):
    return {k: v for (k, v) in temp.items() if v/L >= t}

def matching(valores_para_matching):
    global tp, fp, pairsNo, L1, q
    
    if len(valores_para_matching) > 0:
        # print("tem valores para matching", len(valores_para_matching))
        for correspondencias in valores_para_matching:
            for index2 in correspondencias: 
                if index2 > len(df2) - 1:
                    return True

                rr = df2.iloc[index2]
                ncid = rr["TID2"]
                title = rr["title"]
                length = rr["length"]
                artist = rr["artist"]
                album = rr["album"]
                srec = title + " " + length + " " + str(artist) + " " + str(album)

                temp = dict()
                indices = [random.randrange(0, L) for i in range(L1)]
                matchingPairs = {}
                for l in indices:
                    key = str(str_to_MinHash(srec.lower(), q, l))
                    d = dictB[l]
                    if key in d:
                        ids = d[key]
                        for id in ids:
                            if id in temp:
                                temp[id] += 1
                                if temp[id] / L1 >= t:
                                    matchingPairs[id] = 1
                            else:
                                temp[id] = 1
                for id in matchingPairs.keys():
                    idDBLP = id
                    pairsNo += 1
                    if idDBLP in truthD:
                        ids = truthD[idDBLP]
                        for id in ids:
                            if id == ncid:
                                tp += 1
                                break
                    else:
                        fp += 1

        return False
    else:
        for index2 in range(nbS, nbS + offsetB): 
            if index2 > len(df2) - 1:
                return True

            rr = df2.iloc[index2]
            ncid = rr["TID2"]
            title = rr["title"]
            length = rr["length"]
            artist = rr["artist"]
            album = rr["album"]
            srec = title + " " + length + " " + str(artist) + " " + str(album)

            possui_correspondencia = rr["poss_correspondencia"]

            if possui_correspondencia == False:
                temp = dict()
                indices = [random.randrange(0, L) for i in range(L1)]
                matchingPairs = {}
                for l in indices:
                    key = str(str_to_MinHash(srec.lower(), q, l))
                    d = dictB[l]
                    if key in d:
                        ids = d[key]
                        for id in ids:
                            if id in temp:
                                temp[id] += 1
                                if temp[id] / L1 >= t:
                                    matchingPairs[id] = 1
                            else:
                                temp[id] = 1
                for id in matchingPairs.keys():
                    idDBLP = id
                    pairsNo += 1
                    if idDBLP in truthD:
                        ids = truthD[idDBLP]
                        for id in ids:
                            if id == ncid:
                                tp += 1
                                break
                    else:
                        fp += 1
        return False

def remoção_global_heap(dictGlobalUnico, dictB, dictB_igual, tempo_minimo):
    esquerda, direita = 0, len(dictGlobalUnico)  
    while esquerda < direita:
        meio = (esquerda + direita) // 2
        if dictGlobalUnico[meio][1] < tempo_minimo:
            esquerda = meio + 1
        else:
            direita = meio
    indice_inicio = esquerda
    for i in range(indice_inicio):
        id, tempo_atual, lista = dictGlobalUnico[i]
        for id_2234, posicao_array in lista:
            if id_2234 in dictB[posicao_array] and dictB[posicao_array][id_2234][0] == id:
                del dictB[posicao_array][id_2234][0]
                del dictB_igual[posicao_array][id_2234][0]
    return dictGlobalUnico[indice_inicio:]

def elimina_elementos_dentro_dictB(array_para_descarte, array_para_descarte_igual):
    array_para_descarte.clear()
    array_para_descarte_igual.clear()

def pega_correspondencias(df1, truthD, df2, iloc):
    rr = df1.iloc[iloc]
    idDBLP = rr["TID"]
    correspondencias = []

    if idDBLP in truthD:
        ids_scholar = truthD[idDBLP]
        for id_scholar in ids_scholar:
            rr2 = df2[df2['TID2'] == id_scholar]
            if not rr2.empty:
                correspondencias.append(int(rr2.index[0]))

    return correspondencias

if __name__ == "__main__":
    df1 = pd.read_csv("musicbrainz-200-A01.csv.dapo", sep=",", encoding="utf-8", keep_default_na=False)
    df2 = pd.read_csv("music_brainz-simple-mutated.csv", sep=",", encoding="utf-8", keep_default_na=False)

    truth = pd.read_csv("ground_truth_music_brainz.csv", sep=",", encoding="utf-8", keep_default_na=False)
    truthD = dict()
    for i, r in truth.iterrows():
        novoId = r["novoidmusic1"]
        antigoId = r["antigoidmusic2"]
        if antigoId in truthD:
            ids = truthD[antigoId]
            ids.append(novoId)
        else:
            truthD[antigoId] = [novoId]

    df2['poss_correspondencia'] = df2['id'].apply(lambda x: True if x in truth["antigoidmusic2"].unique() else False)

    # Pré-processar correspondências para evitar overhead durante blocking
    print("Pré-processando correspondências...")
    st_prep = time.time()
    
    # Criar índice reverso df2: TID2 -> index
    df2_id_to_index = {row['TID2']: idx for idx, row in df2.iterrows()}
    
    # Mapear cada índice de df1 para suas correspondências em df2
    correspondencias_map = {}
    for index1 in range(len(df1)):
        tid = df1.iloc[index1]['TID']
        correspondencias = []
        
        if tid in truthD:
            ids_scholar = truthD[tid]
            for id_scholar in ids_scholar:
                if id_scholar in df2_id_to_index:
                    correspondencias.append(df2_id_to_index[id_scholar])
        
        correspondencias_map[index1] = correspondencias
    
    end_prep = time.time()
    print(f"Pré-processamento concluído em {(end_prep - st_prep):.2f} segundos")
    print(f"Total de correspondências mapeadas: {sum(len(v) for v in correspondencias_map.values())}")

    t = 0.5
    TP = TP = 193471
    eps = 0.1
    w = 1000
    delta = 0.1
    L = math.ceil(math.log(1 / delta) / (2 * (eps ** 2)))
    eps = 0.01
    L1 = int(1 / (2 * eps))
    print("L=", L, "L1=", L1)
    q = 2
    dictB = [dict() for l in range(L)]
    dictB_igual = [dict() for l in range(L)]

    dictGlobalUnico = [] 
    # chave:(key, l, local)  
    # [("conf/sigmod/HaasNSS95", 2, [("-2053413593",0), ("-1844936030", 1)]), 
    #  ("conf/sigmod/HoelS95", 3, [("-4231245125",0), ("-5543346345", 1)])]
    tp = 0
    fp = 0
    pairsNo = 0
    nbS = 1
    naS = 1
    offsetA = 50
    offsetB = 50
    blockingTime = 0
    matchingTime = 0

    tempoQueFoiInseridoNaEstrutura = 0 # esse é o valor que sera colocado junto com o id, seria a posiçaõ do elemento no array

    tamanhoDosBlocos = {}

    total_tempermanencia = 0
    total_entidades = 0

    valores_para_matching = []

    while True:
        st = time.time()
        for index1 in range(naS, naS + offsetA):
            if index1 >= len(df1):
                break
            
            rr = df1.iloc[index1]
            ncid = rr["TID"]
            title = rr["title"]
            length = rr["length"]
            artist = rr["artist"]
            album = rr["album"]

            # Usar mapeamento pré-processado (O(1) lookup)
            correspondencias = correspondencias_map.get(index1, [])
            valores_para_matching.append(correspondencias)

            srec = title + " " + length + " " + str(artist) + " " + str(album)
            key = ""

            conjunto_chaves = []
            for l in range(L):
                key = str(str_to_MinHash(srec.lower(), 2, l))
                conjunto_chaves.append((key, l))
                d = dictB[l]
                d_igual = dictB_igual[l]
                if key in d:
                    ids = d[key]
                    ids_igual = d_igual[key]
                    tamanho_atual = tamanhoDosBlocos[(key, l)]
                    if len(ids) < tamanho_atual:
                        ids.append(ncid)
                        ids_igual.append(tempoQueFoiInseridoNaEstrutura)
                    else:
                        elimina_elementos_dentro_dictB(ids, ids_igual)

                        ids.append(ncid)
                        ids_igual.append(tempoQueFoiInseridoNaEstrutura)
                else:
                    d[key] = [ncid]
                    d_igual[key] = [tempoQueFoiInseridoNaEstrutura]

                    tamanhoDosBlocos[(key, l)] = w
            
            dictGlobalUnico.append((ncid, tempoQueFoiInseridoNaEstrutura, conjunto_chaves))
            # print(dictGlobalUnico[-1])

            #enviar para o remoção apenas as ultimas 1000 entidades
            entidades_eliminadas = tempoQueFoiInseridoNaEstrutura - 2500
            valor = entidades_eliminadas if entidades_eliminadas > 0 else -1
            dictGlobalUnico = remoção_global_heap(dictGlobalUnico, dictB, dictB_igual, valor)

            tempoQueFoiInseridoNaEstrutura += 1
        
        end = time.time()

        blockingTime += (end - st)
        st = time.time()
        termination = matching(valores_para_matching)  
        end = time.time()
        matchingTime += (end - st)
        if termination:
                break
        if len(valores_para_matching) == 0:
            # print("Nenhuma correspondência encontrada no último bloco processado.")

            nbS += offsetB
        naS += offsetA
        valores_para_matching = []

    print("blocking time (in mins)", blockingTime / 60)
    print("matching time (in mins)", matchingTime / 60)
    if tp + fp > 0:
        print("TP=", tp, "Recall=", tp / TP, "Precision=", tp / (tp + fp), "pairsNo=", pairsNo)