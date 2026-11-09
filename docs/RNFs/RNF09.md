# **RNF09 \- Configurabilidade**

| Campo | Descrição |
| :---- | :---- |
| Identificador | RNF09 |
| Nome | Configurabilidade |
| Categoria | Manutenibilidade |
| Descrição | Os parâmetros operacionais devem ser ajustáveis sem alteração de código. |

**Metas:**

* Todo parâmetro que um RF chama de configurável é lido de uma variável de ambiente e aparece num `.env.example`, com o valor padrão do RF.
* Mudar um parâmetro exige só recriar o container (`docker compose up -d`), sem *rebuild* nem mudança de código.
* Valor ausente usa o padrão. Valor inválido faz o componente falhar na partida, com uma mensagem que cita a variável, como o RF02 já exige para o `TARGET_ADDRESS`.

**Justificativa:** com essas metas, a configurabilidade passa a ser conferida com uma lista, contra os RFs. Hoje o código lê 27 variáveis de ambiente, e o único `.env.example` (o do coletor) documenta 12. As do coordenador, como `MARGEM_HISTERESE`, `LIMIAR_CONEXAO_FRACA` e `PRAZO_LIBERACAO`, não aparecem em nenhum exemplo. O *keepalive* MQTT também está fixo no código (10 s), embora o RF11 o vincule ao intervalo do sinal de vida.
