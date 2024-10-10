import pandas as pd
import numpy as np
from scipy.stats import entropy
from lxml import etree

def calcular_entropia(coluna):
    valor_contagem = coluna.value_counts()
    probabilidade = valor_contagem / len(coluna)
    return entropy(probabilidade,base=2)

def calcular_entropia_condicional(df, atributo, alvo):
    grupos = df.groupby(atributo)[alvo].apply(lambda x: calcular_entropia(x))
    probabilidade_atributo = df[atributo].value_counts(normalize=True)
    entropia_condicional = sum(probabilidade_atributo * grupos)
    return entropia_condicional

def calcular_ganho_informacao(df, atributo, alvo):
    entropia_alvo = calcular_entropia(df[alvo])
    entropia_condicional = calcular_entropia_condicional(df, atributo, alvo)
    return entropia_alvo - entropia_condicional

def selecionar_melhores_atributos(df, alvo):
    ganhos_informacao = {}
    atributos = [col for col in df.columns if col != alvo]

    for atributo in atributos:
        ganho = calcular_ganho_informacao(df, atributo, alvo)
        ganhos_informacao[atributo] = ganho
    atributos_ordenados = sorted(ganhos_informacao.items(), key=lambda x: x[1], reverse=True)
    
    return atributos_ordenados

def selecionar_melhores_atributos_sem_alvo(df):
    ganhos_informacao = {}
    atributos = df.columns

    for atributo in atributos:
        ganho = calcular_entropia(df[atributo])
        ganhos_informacao[atributo] = ganho
    atributos_ordenados = sorted(ganhos_informacao.items(), key=lambda x: x[1], reverse=True)
    
    return atributos_ordenados

def experimentos_simples():
    dados = {
        'Nome': ['João', 'Maria', 'José', 'Ana', 'Carlos', 'Fernanda'],
        'Idade': [23, 23, 22, 21, 23, 22],
        'Cidade': ['SP', 'RJ', 'SP', 'MG', 'RJ', 'MG'],
        'Profissão': ['Engenheiro', 'Médico', 'Engenheiro', 'Advogado', 'Médico', 'Advogado'],
        'Classe': ['A', 'B', 'A', 'B', 'A', 'B']
    }
    
    df = pd.DataFrame(dados)

    melhores_atributos = selecionar_melhores_atributos(df, 'Classe')
    
    print("Melhores atributos ordenados pelo ganho de informação:")
    for atributo, ganho in melhores_atributos:
        print(f"Atributo: {atributo}, Ganho de Informação: {ganho:.4f}")


    melhores_atributos_sem_alvo = selecionar_melhores_atributos_sem_alvo(df)
    print("Melhores atributos ordenados pela entropia:")
    for atributo, ganho in melhores_atributos_sem_alvo:
        print(f"Atributo: {atributo}, Entropia: {ganho:.4f}")

# Função para ler e filtrar o XML com XPath
def ler_xml_com_xpath(file_path, xpath_expression):
    # Parse o arquivo XML
    tree = etree.parse(file_path)
    
    # Aplique a expressão XPath
    records = tree.xpath(xpath_expression)
    
    # Extrair os dados dos registros e criar uma lista de dicionários
    dados = []
    for record in records:
        dados.append({element.tag: element.text for element in record})
    
    # Converter a lista de dicionários para um DataFrame
    df = pd.DataFrame(dados)
    return df

if __name__ == "__main__":
    # df1 = pd.read_csv("experimento_base/DBLP.csv", sep=",", encoding="utf-8", keep_default_na=False)
    # df2 = pd.read_csv("experimento_base/Scholar.csv", sep=",", encoding="utf-8", keep_default_na=False)

    # melhores_atributos = selecionar_melhores_atributos_sem_alvo(df2)
    
    # print("Melhores atributos ordenados pelo ganho de informação:")
    # for atributo, ganho in melhores_atributos:
    #     print(f"Atributo: {atributo}, Ganho de Informação: {ganho:.4f}")
    # Lendo uma parcela do arquivo XML
    # file_path = '/home/igor/Downloads/dblp.xml'
    # xpath_expression = ".//record[position() <= 100]"
    # df3_xml_parcela = ler_xml_com_xpath(file_path, xpath_expression)
    # print(df3_xml_parcela)
    # print("Primeiros 100 registros do arquivo XML:")

    df_txt = pd.read_csv("ncvoter42.txt", sep="\t", encoding="utf-8", encoding_errors='ignore', keep_default_na=False, on_bad_lines='skip')
    melhores_atributos_txt = selecionar_melhores_atributos_sem_alvo(df_txt)

    print("Melhores atributos ordenados pelo ganho de informação:")

    for atributo, ganho in melhores_atributos_txt:
        print(f"Atributo: {atributo}, Ganho de Informação: {ganho:.4f}")

    