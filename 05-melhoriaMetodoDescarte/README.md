# Experimento de Métodos de Descarte

## Descrição

Este experimento teve como objetivo modificar a estrutura dos blocos utilizados no método de descarte baseado no tempo. Observou-se que a mudança aumentou o tempo de matching em aproximadamente 1 minuto, sem impactar negativamente as métricas. O principal motivo desse experimento foi o tempo de descompactação proveniente da tupla de valores usada no método de descarte. Mais detalhes sobre o método podem ser encontrados na [documentação](https://github.com/pereiraIgor/approximate-blocking/blob/master/04-implementacaoMetodoDescarte/README.md).

## Estruturas Utilizadas

### 1. Array de Arrays

Nesta abordagem, em vez de utilizarmos uma tupla de valores `('journals/sigmod/EisenbergM02', 0)`, onde o primeiro elemento representa o ID e o segundo o tempo de inserção, foi adotado um array de duas posições `['journals/sigmod/EisenbergM02', 0]`, contendo os mesmos valores.

O objetivo era verificar se essa modificação traria alguma melhoria no tempo de matching. No entanto, os testes indicaram um aumento médio de **1,9 minutos** em relação ao tempo base (**4,5 minutos vs. 2,6 minutos** respectivamente).

### 2. Concatenação de Strings

Nesta estrutura, em vez de armazenarmos uma tupla de valores `('journals/sigmod/EisenbergM02', 0)`, utilizamos uma única string, concatenando o ID com o tempo de inserção, resultando em algo como `00002journals/sigmod/Liu02`.

O objetivo era avaliar possíveis ganhos de desempenho. Contudo, os testes mostraram um aumento médio de **3,7 minutos** em relação ao tempo base (**6,3 minutos vs. 2,6 minutos** respectivamente).

### 3. Estrutura Copiada

Nesta abordagem, criamos uma segunda estrutura para armazenar exclusivamente os tempos de inserção.

A estrutura original possui o seguinte formato:
```json
[{'-2131080486': ['019zSr3Lx4EJ', 'iWNLOYCQX-YJ', 'KureLsKbUXYJ', 'T2fm7Wb1ak4J'], ...}]
```
Já a estrutura adicional armazena os tempos correspondentes:
```json
[{'-2131080486': [0, 12, 31, 412], ...}]
```
Ambas as estruturas possuem o mesmo tamanho e seus índices são equivalentes. Qualquer operação realizada na estrutura original é replicada na estrutura copiada.

Os testes indicaram que essa abordagem **mantém o tempo de matching igual ao do método base**, sem prejudicar as métricas.

## Conclusões e Próximos Passos

Os experimentos mostraram que a estrutura copiada mantém o tempo de matching inalterado em relação ao método base, sem impactar as métricas. No entanto, ainda não foram analisados os efeitos dessa abordagem no consumo de memória.

### Próximos Passos
- Implementar uma estratégia de remoção ou ajuste do tamanho dos blocos com base no tempo do último acesso, garantindo maior alocação de memória para os blocos mais utilizados.
- Avaliar o impacto das mudanças no uso de memória.
- Explorar outras possíveis otimizações na estrutura de dados.

---

Esse estudo contribui para o aprimoramento do método de descarte baseado no tempo, proporcionando insights valiosos para otimizações futuras.

