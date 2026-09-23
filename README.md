# Teste de requisições aos modelos Huawei MaaS

Script em Python para executar requisições concorrentes à API de chat OpenAI-compatible do Huawei MaaS. As chamadas usam streaming e geram um resumo no terminal e um relatório HTML com métricas de latência, throughput, tokens e confiabilidade.

## Requisitos

- Python 3.9 ou superior.
- Acesso à API Huawei MaaS e uma chave válida para os modelos configurados.
- Dependência `requests`.

Instale a dependência:

```bash
python3 -m pip install requests
```

Configure a chave da API no ambiente. Não a coloque no código nem a compartilhe:

```bash
export API_KEY="sua-chave-aqui"
```

No PowerShell:

```powershell
$env:API_KEY = "sua-chave-aqui"
```

## Executar o teste

Na pasta do projeto, execute:

```bash
python3 maas_parallel_test.py
```

Por padrão, o script envia 10 chamadas para cada modelo em `MODELS` (`glm-5.2` e `glm-5.3`), usando até 20 workers. Um limitador compartilhado mantém o envio em até 4 requisições por segundo; respostas HTTP 429 são repetidas com espera exponencial, até o máximo configurado. As chamadas dos modelos concorrem no mesmo teste, portanto os resultados refletem essa carga compartilhada.

Para adaptar o teste, altere no início de `maas_parallel_test.py`:

- `MODELS`: nomes dos modelos disponíveis para sua conta/região.
- `CALLS_PER_MODEL`: quantidade de chamadas por modelo.
- `RATE_LIMIT_RPS`: limite de envio por segundo; ajuste de acordo com a quota autorizada do endpoint.
- `MAX_RETRIES` e `TIMEOUT`: política de repetição e tempo limite.
- `PROMPT`: solicitação enviada ao modelo. Para medir geração, use um prompt que produza uma resposta com quantidade razoável de tokens.

O limitador local ajuda a respeitar uma taxa de envio, mas não garante ausência de 429: quotas podem ser compartilhadas, variar por modelo ou ter janelas diferentes. Não aumente a taxa sem confirmar os limites da sua conta.

## Resultado no terminal

Durante a execução, cada chamada concluída imprime modelo, identificador, status HTTP, latência E2E, TTFT e, quando houver, tentativas repetidas. Ao final, há um resumo por modelo. Exemplo de uma execução:

```text
Executando 20 chamadas em paralelo (10 por modelo, modelos: glm-5.2, glm-5.3)...

[glm-5.3] chamada 03 | OK  | HTTP 200 | E2E: 7.50s | TTFT: 1728ms | resposta: ...
...

RESUMO

Modelo: glm-5.2
  Sucessos:        10/10 (error rate: 0.0%)
  Retries totais:  2
  TTFT médio:      5568ms (P50: 5695ms, P90: 6686ms)
  TPOT médio:      21ms
  Output tok/s:    47.7
  E2E média:       20.52s (P50: 18.20s, P90: 25.91s, P99: 33.65s)
  QPS atingido:    0.28 | TPM atingido: 12855
  Tokens médios:   in 41 / out 715 (raciocínio: 593)

Modelo: glm-5.3
  Sucessos:        10/10 (error rate: 0.0%)
  Retries totais:  1
  TTFT médio:      1950ms (P50: 1858ms, P90: 2155ms)
  TPOT médio:      18ms
  Output tok/s:    63.4
  E2E média:       9.46s (P50: 8.87s, P90: 13.76s, P99: 13.79s)
  QPS atingido:    0.28 | TPM atingido: 8132
  Tokens médios:   in 41 / out 437 (raciocínio: 281)

Tempo total de execução (todas em paralelo): 35.27s
Relatório HTML gerado em: .../maas_report.html
```

Esses valores são de uma execução específica e servem apenas para ilustrar o formato. Latência, quantidade de tokens, retries e throughput variam com o prompt, carga, quota, região e estado do serviço.

## Relatório HTML

Ao terminar, o script grava `maas_report.html` na mesma pasta do arquivo Python. Abra-o em um navegador. O relatório apresenta cards de resumo, tabelas de métricas, gráfico de latência por chamada e detalhes individuais.

### Resumo da execução

![Cards com chamadas bem-sucedidas, error rate, tempo total e TTFT médio](image/report_cards.png)

### Latência e geração

![Tabela de TTFT, TPOT, tokens por segundo e percentis de latência](image/report_metrics.png)

### Capacidade, confiabilidade e tokens

![Tabela de QPS, TPM, sucessos, erros e contagem de tokens](image/report_capacity.png)

### Latência por chamada

![Gráfico comparando latência E2E e TTFT por chamada](image/report_chart.png)

### Detalhes das chamadas

![Tabela com status e métricas individuais de cada requisição](image/report_details.png)

## Como interpretar as métricas

| Métrica | Significado neste teste |
|---|---|
| **TTFT** | Tempo entre o envio da requisição e o primeiro fragmento de token recebido. Para modelos de raciocínio, inclui o primeiro `reasoning_content` ou `content`, o que ocorrer primeiro. |
| **TPOT** | Estimativa do tempo médio por token de saída, calculada a partir da duração após o TTFT e da contagem de tokens de completion informada pelo serviço. |
| **Output tokens/sec** | Tokens de completion divididos pelo tempo de geração após o TTFT. Inclui tokens de raciocínio quando estes fazem parte de `completion_tokens`. |
| **E2E Latency** | Tempo total medido para a requisição, do envio ao fim do stream. O tempo na fila do limitador local fica fora desta medição. |
| **P50/P90/P99** | Percentis da latência E2E das chamadas bem-sucedidas. P90, por exemplo, indica o valor abaixo do qual ficaram 90% das chamadas observadas. |
| **QPS atingido** | Chamadas bem-sucedidas por segundo, calculadas usando o tempo total do teste. É throughput observado, não a quota máxima do serviço. |
| **TPM atingido** | Tokens de entrada e completion por minuto durante o teste. Também é throughput observado. |
| **Error rate** | Percentual de chamadas que terminaram sem sucesso, após as tentativas configuradas. |
| **Tokens/request** | Contagens de entrada, saída e raciocínio reportadas pela API, quando disponíveis. Tokens de raciocínio são uma parte dos tokens de saída; não devem ser somados novamente ao total. |

O relatório mostra a média de QPS/TPM por modelo usando a duração total da execução compartilhada. Como os modelos são testados simultaneamente, esses valores não representam uma medição isolada nem o limite máximo de cada modelo.

## Uso por outras pessoas

1. Obtenha acesso ao Huawei MaaS e confirme que sua conta pode invocar os modelos desejados.
2. Disponibilize Python 3.9+ e instale `requests`.
3. Defina `API_KEY` no ambiente da máquina que executará o teste.
4. Copie/clone este projeto e ajuste modelos, quantidade de chamadas, prompt e taxa de envio para a quota da sua conta.
5. Execute `python3 maas_parallel_test.py` e abra o `maas_report.html` gerado.

Cada usuário deve usar sua própria chave e respeitar as políticas, quotas e custos da conta. Para comparar modelos de forma mais controlada, mantenha o mesmo prompt e configuração, repita o teste em condições equivalentes e avalie várias execuções; uma amostra pequena pode ser afetada por variação temporária do serviço.