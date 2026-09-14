# Comparação Estatística com Baseline

Este documento explica como usar os dois scripts para coletar baseline e comparar estatisticamente os experimentos.

## 📋 Fluxo de Execução

### Passo 1: Coletar Baseline (Experimento 01)

Execute o script de baseline para coletar as métricas de referência:

```bash
# No terminal, na pasta do projeto
bash script-baseline.sh
```

ou em background (recomendado para datasets grandes):

```bash
nohup bash script-baseline.sh > baseline-log.txt 2>&1 &
```

**O que acontece:**
- Executa o Experimento-01 (baseline) para todos os 3 datasets
- Cada dataset é executado com 10 réplicas
- Gera 3 arquivos CSV:
  - `resultados-exp-01-base-dblp.csv`
  - `resultados-exp-01-base-ncvoter.csv`
  - `resultados-exp-01-base-musicbrainz.csv`

**Tempo estimado:** 30-45 minutos (depende do tamanho dos datasets)

### Passo 2: Comparar com Baseline (Experimentos 02-04)

Após o baseline ser coletado, execute o script de comparação:

```bash
# No terminal, na pasta do projeto
bash script-comparacao.sh
```

ou em background:

```bash
nohup bash script-comparacao.sh > comparacao-log.txt 2>&1 &
```

**O que acontece:**
- Verifica se os 3 CSVs de baseline existem
- Executa os Experimentos 02, 03 e 04 para todos os datasets
- Compara cada experimento contra o baseline correspondente
- Gera 9 arquivos CSV de resultados:
  - `resultados-exp-02-descarte-tempo-{dblp,ncvoter,musicbrainz}.csv`
  - `resultados-exp-03-descarte-bloco-{dblp,ncvoter,musicbrainz}.csv`
  - `resultados-exp-04-descarte-global-{dblp,ncvoter,musicbrainz}.csv`

**Tempo estimado:** 45-60 minutos (depende do tamanho dos datasets)

## 📊 Entendendo os Resultados

### Tabela de Estatísticas Individuais

Cada experimento mostra:

```
Métrica         Média        Desvio Pad   IC Inferior  IC Superior  t-Student    p-valor
───────────────────────────────────────────────────────────────────────────────────────
blocking_s      3.186967     0.008950     3.180564     3.193369     1126.046124  1.74925e-24
matching_s      119.578471   2.093886     118.080595   121.076347   180.592580   2.48932e-17
...
```

**Interpretação:**
- **Média**: Valor médio da métrica entre as 10 réplicas
- **Desvio Pad**: Variabilidade dos resultados
- **IC Inferior/Superior**: Intervalo de confiança 95%
- **t-Student**: Valor do teste (quanto maior, mais diferente de 0)
- **p-valor**: Probabilidade de erro ao rejeitar H0:métrica=0

### Tabela de Comparação com Baseline

```
Métrica         Baseline      Atual         Diferença     t-Student     p-valor
───────────────────────────────────────────────────────────────────────────────
blocking_s      3.114614      3.197002      0.082388      1.051909      0.480063  ✗
matching_s      116.143947    117.937780    1.793832      0.593329      0.614411  ✗
tp              4980.000000   4974.000000   -6.000000     -1.664101     0.255876  ✗
...
```

**Interpretação:**
- **Baseline**: Média do Experimento-01 (referência)
- **Atual**: Média do Experimento em teste (02, 03 ou 04)
- **Diferença**: Atual - Baseline (positivo = pior, negativo = melhor)
- **t-Student**: Teste t independente (Welch's t-test)
- **p-valor**: Significância estatística
  - ✓ = **p < 0.05** → Diferença é SIGNIFICATIVA
  - ✗ = **p ≥ 0.05** → Diferença NÃO é significativa (pode ser coincidência)

## 🎯 Exemplo de Interpretação

Suponha esses resultados para `blocking_s`:

```
DBLP Dataset:
Baseline:    3.11 segundos
Exp-02:      3.19 segundos (+0.08s, p=0.48 ✗)
Exp-03:      2.95 segundos (-0.16s, p=0.002 ✓)
Exp-04:      2.88 segundos (-0.23s, p<0.001 ✓)
```

**Conclusão:**
- Exp-02 é MAIS LENTO (não significativo - pode ser variação)
- Exp-03 é MAIS RÁPIDO (significativo - melhora real)
- Exp-04 é MUITO MAIS RÁPIDO (altamente significativo - grande melhora)

## 📁 Arquivos Gerados

```
resultados/
├── resultados-exp-01-base-dblp.csv           (baseline)
├── resultados-exp-01-base-ncvoter.csv        (baseline)
├── resultados-exp-01-base-musicbrainz.csv    (baseline)
├── resultados-exp-02-descarte-tempo-*.csv    (comparação)
├── resultados-exp-03-descarte-bloco-*.csv    (comparação)
├── resultados-exp-04-descarte-global-*.csv   (comparação)
├── baseline-coleta.txt                       (log detalhado)
└── comparacao-resultados.txt                 (log detalhado)
```

## 🔄 Fluxo Recomendado

```bash
# Terminal 1: Coletar baseline
bash script-baseline.sh

# Aguardar conclusão (~30-45 min)

# Terminal 2 (quando baseline terminar): Comparar
bash script-comparacao.sh

# Aguardar conclusão (~45-60 min)

# Analisar resultados
tail -n 500 comparacao-resultados.txt
```

## ⚠️ Notas Importantes

1. **Ordem:** Sempre execute o baseline PRIMEIRO
2. **Datasets:** Os arquivos de baseline são específicos por dataset
3. **Seeds:** Use a mesma seed (padrão: 20260907) para comparação válida
4. **Réplicas:** 10 réplicas (padrão) é recomendado para validade estatística
5. **Tempo:** MusicBrainz é o mais lento; DBLP e NCVoter são mais rápidos

## 💾 Salvando Resultados

Para análise posterior, você pode:

1. **Copiar os CSVs:**
```bash
mkdir -p analise-$(date +%Y%m%d)
cp resultados-exp-*.csv analise-$(date +%Y%m%d)/
cp *-coleta.txt analise-$(date +%Y%m%d)/
```

2. **Analisar com Python:**
```python
import pandas as pd

# Carregar resultados
baseline = pd.read_csv('resultados-exp-01-base-dblp.csv')
exp02 = pd.read_csv('resultados-exp-02-descarte-tempo-dblp.csv')

# Comparar
print(baseline['blocking_s'].mean())
print(exp02['blocking_s'].mean())
```

## 🆘 Troubleshooting

**Erro: "Arquivo de baseline não encontrado"**
- Execute `bash script-baseline.sh` primeiro

**Erro: "ModuleNotFoundError: pandas"**
- Ative o virtual environment: `source .venv/bin/activate`

**Script muito lento com MusicBrainz**
- Normal! MusicBrainz é o maior dataset (~200K registros)
- Use `-r 5` para reduzir réplicas em testes rápidos

**Falha de memória**
- Reduzir número de réplicas com `-r` ou usar dataset menor
