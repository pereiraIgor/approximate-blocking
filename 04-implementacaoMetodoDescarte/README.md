# Experimento de Métodos de Descarte

## Descrição
Este experimento teve como objetivo criar e avaliar diferentes métodos de descarte de elementos em um sistema baseado em blocos de inserção. Foram desenvolvidos três estratégias distintas para realizar o descarte, cada uma com regras específicas para a remoção de elementos.

## Métodos de Descarte Implementados

### 1. Descarte Simples ao Atingir o Limiar do Bloco
Neste método, um bloco tem um limite de elementos que pode armazenar. Quando esse limiar é atingido, os 50 primeiros elementos inseridos no bloco são descartados para liberar espaço para novas inserções.

**Problema Identificado:** O método funciona bem para situações simples, mas utiliza um índice global de descarte. Isso pode causar problemas quando diferentes blocos crescem de maneira desigual, já que o índice de descarte de um bloco pode afetar outro bloco independente, sem considerar a individualidade de cada um.

### 2. Descarte Global Após 50 Inserções
Neste método, a cada 50 inserções realizadas em qualquer bloco, um processo de descarte é acionado para todos os blocos existentes.

**Problema Identificado:** Esse método acaba eliminando informações de blocos pequenos que não representam um problema de ocupação, o que pode levar à perda desnecessária de dados.

### 3. Descarte Baseado em Tempo
Neste método, além do limite de elementos por bloco, cada entidade inserida possui um tempo de inserção associado. Quando um bloco atinge seu limite, todas as entidades inseridas antes de um determinado limiar de tempo são removidas, garantindo que apenas os elementos mais recentes sejam mantidos.

**Vantagens e Desempenho:** Este método se mostrou eficaz ao permitir uma melhor gestão do espaço sem afetar blocos pequenos desnecessariamente. A estrutura armazenada inclui pares de valores como `('journals/sigmod/EisenbergM02', 0)` e `('journals/sigmod/CruzKLSWY02', 43)`, onde os números representam o instante de inserção. 

### Comparação de Performance

| Método | Tempo de Bloqueio (min) | Tempo de Matching (min) | TP | Recall | Precisão | Pares Processados |
|--------|-----------------|----------------|----|--------|----------|----------------|
| Base   | 0.1727         | 3.0012         | 4978 | 0.9309  | 0.6839   | 70160         |
| Com Descarte  | 0.0910         | 4.0058         | 4956 | 0.9268  | 0.6751   | 69077         |

Embora o método baseado em tempo tenha melhor desempenho na fase de bloqueio, ele apresentou um overhead na etapa de matching, que demorou cerca de 1 minuto a mais em relação ao tempo base. Esse impacto pode estar relacionado à estrutura de dados utilizada, que requer um unpacking adicional ao lidar com tuplas ao invés de strings, aumentando o custo computacional.

## Conclusões e Próximos Passos
Os testes indicaram que o método baseado em tempo apresenta vantagens na organização e eficiência do descarte, porém há um impacto de performance que precisa ser otimizado. 

Próximos passos incluem:
- Ajustar a estrutura de armazenamento para reduzir o overhead de matching.
- Investigar maneiras de otimizar o acesso e descarte das entidades baseadas em tempo.
- Testar outras abordagens híbridas para maximizar a precisão e minimizar a latência.


