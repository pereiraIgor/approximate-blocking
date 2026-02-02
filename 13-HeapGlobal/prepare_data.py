"""
Script para pré-processar datasets e adicionar índices de correspondências
Executa UMA VEZ antes dos experimentos para eliminar overhead
"""
import pandas as pd
import json

def prepare_dblp_scholar():
    """Prepara datasets DBLP-Scholar com índices de correspondências"""
    print("Preparando DBLP-Scholar...")
    
    df1 = pd.read_csv("../00-datasets/DBLP.csv", sep=",", encoding="utf-8", keep_default_na=False)
    df2 = pd.read_csv("../00-datasets/Scholar.csv", sep=",", encoding="utf-8", keep_default_na=False)
    truth = pd.read_csv("../00-datasets/truth.csv", sep=",", encoding="utf-8", keep_default_na=False)
    
    # Criar índice reverso df2
    df2_id_to_index = {row['id']: idx for idx, row in df2.iterrows()}
    
    # Criar dicionário de verdade
    truthD = {}
    for i, r in truth.iterrows():
        idDBLP = r["idDBLP"]
        idScholar = r["idScholar"]
        if idDBLP in truthD:
            truthD[idDBLP].append(idScholar)
        else:
            truthD[idDBLP] = [idScholar]
    
    # Adicionar coluna com índices de correspondências (como string JSON)
    correspondencias_list = []
    for idx, row in df1.iterrows():
        idDBLP = row['id']
        correspondencias = []
        
        if idDBLP in truthD:
            for id_scholar in truthD[idDBLP]:
                if id_scholar in df2_id_to_index:
                    correspondencias.append(df2_id_to_index[id_scholar])
        
        # Salvar como string JSON (ex: "[10,25,30]")
        correspondencias_list.append(json.dumps(correspondencias))
    
    df1['correspondencias_indices'] = correspondencias_list
    
    # Adicionar flag de correspondência em df2
    df2['poss_correspondencia'] = df2['id'].apply(
        lambda x: True if x in truth["idScholar"].unique() else False
    )
    
    # Salvar datasets preparados
    df1.to_csv("../00-datasets/DBLP_prepared.csv", index=False, encoding="utf-8")
    df2.to_csv("../00-datasets/Scholar_prepared.csv", index=False, encoding="utf-8")
    
    total_corresp = sum(len(json.loads(c)) for c in correspondencias_list)
    print(f"✓ DBLP-Scholar preparado: {total_corresp} correspondências mapeadas")
    print(f"  - DBLP_prepared.csv")
    print(f"  - Scholar_prepared.csv")


def prepare_ncvoter():
    """Prepara datasets NCVoter com índices de correspondências"""
    print("\nPreparando NCVoter...")
    
    df1 = pd.read_csv("../00-datasets/ncvoter42.csv", sep=",", encoding="utf-8", keep_default_na=False)
    df2 = pd.read_csv("../00-datasets/perturbed_recordsNC_voter.csv", sep=",", encoding="utf-8", keep_default_na=False)
    truth = pd.read_csv("../00-datasets/ground_truthNC_voter.csv", sep=",", encoding="utf-8", keep_default_na=False)
    
    # Criar índice reverso df2
    df2_id_to_index = {row['id']: idx for idx, row in df2.iterrows()}
    
    # Criar dicionário de verdade
    truthD = {}
    for i, r in truth.iterrows():
        novoId = r["novoId"]
        antigoId = r["antigoId"]
        if antigoId in truthD:
            truthD[antigoId].append(novoId)
        else:
            truthD[antigoId] = [novoId]
    
    # Adicionar coluna com índices de correspondências
    correspondencias_list = []
    for idx, row in df1.iterrows():
        ncid = row['ncid']
        correspondencias = []
        
        if ncid in truthD:
            for id_scholar in truthD[ncid]:
                if id_scholar in df2_id_to_index:
                    correspondencias.append(df2_id_to_index[id_scholar])
        
        correspondencias_list.append(json.dumps(correspondencias))
    
    df1['correspondencias_indices'] = correspondencias_list
    
    # Adicionar flag de correspondência em df2
    df2['poss_correspondencia'] = df2['id'].apply(
        lambda x: True if x in truth["antigoId"].unique() else False
    )
    
    # Salvar datasets preparados
    df1.to_csv("../00-datasets/ncvoter42_prepared.csv", index=False, encoding="utf-8")
    df2.to_csv("../00-datasets/perturbed_recordsNC_voter_prepared.csv", index=False, encoding="utf-8")
    
    total_corresp = sum(len(json.loads(c)) for c in correspondencias_list)
    print(f"✓ NCVoter preparado: {total_corresp} correspondências mapeadas")
    print(f"  - ncvoter42_prepared.csv")
    print(f"  - perturbed_recordsNC_voter_prepared.csv")


def prepare_musicbrainz():
    """Prepara datasets MusicBrainz com índices de correspondências"""
    print("\nPreparando MusicBrainz...")
    
    df1 = pd.read_csv("musicbrainz-200-A01.csv.dapo", sep=",", encoding="utf-8", keep_default_na=False)
    df2 = pd.read_csv("music_brainz-simple-mutated.csv", sep=",", encoding="utf-8", keep_default_na=False)
    truth = pd.read_csv("ground_truth_music_brainz.csv", sep=",", encoding="utf-8", keep_default_na=False)
    
    # Criar índice reverso df2
    df2_id_to_index = {row['TID2']: idx for idx, row in df2.iterrows()}
    
    # Criar dicionário de verdade
    truthD = {}
    for i, r in truth.iterrows():
        novoId = r["novoidmusic1"]
        antigoId = r["antigoidmusic2"]
        if antigoId in truthD:
            truthD[antigoId].append(novoId)
        else:
            truthD[antigoId] = [novoId]
    
    # Adicionar coluna com índices de correspondências
    correspondencias_list = []
    for idx, row in df1.iterrows():
        tid = row['TID']
        correspondencias = []
        
        if tid in truthD:
            for id_scholar in truthD[tid]:
                if id_scholar in df2_id_to_index:
                    correspondencias.append(df2_id_to_index[id_scholar])
        
        correspondencias_list.append(json.dumps(correspondencias))
    
    df1['correspondencias_indices'] = correspondencias_list
    
    # Adicionar flag de correspondência em df2
    df2['poss_correspondencia'] = df2['TID2'].apply(
        lambda x: True if x in truth["antigoidmusic2"].unique() else False
    )
    
    # Salvar datasets preparados
    df1.to_csv("musicbrainz-200-A01_prepared.csv", index=False, encoding="utf-8")
    df2.to_csv("music_brainz-simple-mutated_prepared.csv", index=False, encoding="utf-8")
    
    total_corresp = sum(len(json.loads(c)) for c in correspondencias_list)
    print(f"✓ MusicBrainz preparado: {total_corresp} correspondências mapeadas")
    print(f"  - musicbrainz-200-A01_prepared.csv")
    print(f"  - music_brainz-simple-mutated_prepared.csv")


if __name__ == "__main__":
    print("="*60)
    print("PRÉ-PROCESSAMENTO DE DATASETS")
    print("="*60)
    
    prepare_dblp_scholar()
    prepare_ncvoter()
    prepare_musicbrainz()
    
    print("\n" + "="*60)
    print("✓ TODOS OS DATASETS PREPARADOS!")
    print("="*60)
    print("\nAgora os scripts podem ler as correspondências diretamente")
    print("sem nenhum overhead durante o blocking.")
