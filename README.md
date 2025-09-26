# Code Challenge: Data Engineer

## Contexto

Somos uma empresa que está transformando dados em insights para melhorar a experiência do cliente e otimizar operações. Estamos construindo uma plataforma robusta que integra dados de diversas fontes e permite a análise eficiente e confiável desses dados. Sua tarefa é implementar um pipeline de ETL que processe dados e os torne acessíveis para consulta, análise e visualização.

---

## Fontes de Dados

Você receberá dois conjuntos de dados **aprimorados** com estruturas mais realistas e cenários complexos:

- **Transações**: Representa operações entre a empresa e seus clientes, incluindo valores transacionados, condições, metadados ricos e dados de qualidade variada.
- **Pagamentos**: Representa os pagamentos realizados por clientes, incluindo cenários complexos como pagamentos parciais, antecipados, atrasados e múltiplos por transação.

### 📊 **Dados Aprimorados (Versão 2.0)**

Os dados foram significativamente melhorados para tornar o desafio mais realista:

- [transactions.json](transactions.json) - **500 transações** com estrutura rica
- [payments.json](payments.json) - **602 pagamentos** com cenários complexos
- [data_structure_example.json](data_structure_example.json) - Documentação da estrutura

### 🎯 **Melhorias Implementadas**

#### **1. Dados Mais Realistas**
- ✅ IDs únicos não sequenciais (UUIDs)
- ✅ Valores variados e distribuição realista (70% baixo/médio, 20% alto, 10% muito alto)
- ✅ Múltiplas moedas (BRL 85%, USD 10%, EUR 5%)
- ✅ Datas de vencimento variáveis (15-90 dias)
- ✅ Metadados ricos (categoria, canal, desconto, parcelamento)

#### **2. Cenários de Inadimplência Complexos**
- ✅ **Sem pagamento** (~10% das transações)
- ✅ **Pagamentos parciais múltiplos** (~15% com 2-5 parcelas)
- ✅ **Pagamentos atrasados** (~20% com atrasos de 1-120 dias)
- ✅ **Pagamentos antecipados** (~8%)
- ✅ **Pagamentos a maior** (~2%)
- ✅ **Múltiplas transações por cliente** (~30% dos clientes)

#### **3. Estrutura de Dados Rica**
```json
{
  "transaction_id": "txn_a1b2c3d4e5f6",
  "customer_id": "cust_xyz789ab",
  "amount": 1250.50,
  "currency": "BRL",
  "transaction_date": "2024-01-15",
  "due_date": "2024-02-15",
  "payment_terms": "30_days",
  "status": "pending",
  "metadata": {
    "product_category": "electronics",
    "sales_rep": "rep_001",
    "channel": "online",
    "discount_applied": 10,
    "installments": 3
  }
}
```

#### **4. Desafios de Qualidade de Dados**
- ✅ ~3% dos registros têm problemas intencionais
- ✅ Valores nulos, tipos incorretos, datas inválidas
- ✅ Testa robustez do pipeline ETL

---

## Definições

- **Transação**: Uma operação comercial que gera obrigações financeiras, como uma compra ou contrato de serviço.
- **Pagamento**: Um registro de que uma obrigação financeira foi quitada.

---

## Desafio ETL

Como uma empresa orientada por dados, é crucial termos uma infraestrutura que permita o armazenamento, processamento e análise dos dados de maneira eficiente e escalável.

Sua tarefa é construir um pipeline de ETL (Extract, Transform, Load) que:

1. **Extraia** os dados dos arquivos fornecidos, tratando-os de forma adequada;
2. **Transforme** os dados aplicando regras de negócio, agregações e tratamento adequado (limpeza, conversões e enriquecimento);
3. **Carregue** os dados em um formato estruturado, armazenado de forma que possa ser consultado via SQL.

A solução deve ser escalável e preparada para lidar com um aumento significativo no volume de dados no futuro.

### Requisitos

- Implementar o pipeline de ETL.
- Os dados processados devem ser armazenados de forma que possam ser consultados via SQL (por exemplo, através de tabelas em um data lake ou banco de dados compatível).
- A solução deve tratar os dados de forma segura, garantindo que não haja perda de informações.
- A implementação deve contemplar o armazenamento de resultados utilizando arquivos Parquet, particionados por um critério que identifique a inadimplência (por exemplo, `inadimplente = true` ou `false`).

### Diferencial

- Implementar o pipeline de ETL utilizando **Apache Spark** (Scala ou Python).
- Emule uma solução de **streaming** utilizando os arquivos fornecidos como fonte de dados.

---

## Governança e Privacidade

### Desafio

Além de processar e armazenar os dados, é fundamental que a solução implemente políticas de governança e privacidade:

- **Governança**:  
  - Descreva e, se possível, implemente uma solução de metadados que controle quem pode acessar quais dados.  
  - Explique como você faria o gerenciamento de permissões e o controle de acesso aos dados processados.

- **Privacidade**:  
  - Mostre como garantir que os dados sensíveis sejam protegidos, incluindo abordagens para anonimização e/ou criptografia dos dados.
  - Caso opte por implementar, integre essa solução ao pipeline ETL.

### Diferencial

- Implemente no pipeline uma etapa voltada para a governança e privacidade, acompanhada de uma documentação que explique as escolhas e estratégias.

---

## Regras de Negócio

### Contexto

Queremos analisar o comportamento dos clientes em relação a pagamentos atrasados. Para isso, estabelecemos dois critérios para classificar um cliente como inadimplente:

- **Critério 1**: O cliente atrasou um pagamento em mais de 30 dias, após um período de 3 meses.
- **Critério 2**: O cliente teve algum atraso superior a 15 dias em um período de 6 meses.

### Sua Tarefa

- Implemente uma lógica que avalie os clientes com base nesses critérios para um período de 6 meses.  
- Forneça os seguintes resultados:
  - A porcentagem de clientes inadimplentes.
  - A lista de clientes considerados inadimplentes, detalhando os respectivos pagamentos que violaram os critérios.
- Armazene os resultados como arquivos Parquet, particionados por um campo que indique se o cliente é inadimplente (`inadimplente = true` ou `false`).

### 🎯 **Novos Desafios com Dados Aprimorados**

Os dados melhorados introduzem cenários mais complexos que seu pipeline deve tratar:

#### **Cenários de Pagamento Complexos**
- **Pagamentos Parciais**: Como tratar transações com múltiplos pagamentos parciais?
- **Pagamentos Atrasados Variados**: Atrasos de 1 a 120 dias - como calcular inadimplência?
- **Múltiplas Transações por Cliente**: Como agregar o comportamento de pagamento?
- **Pagamentos Antecipados**: Devem ser considerados na análise de risco?

#### **Qualidade de Dados**
- **Dados Inconsistentes**: ~3% dos registros têm problemas intencionais
- **Valores Nulos**: Como tratar transações sem valor?
- **Datas Inválidas**: Como validar e corrigir datas?
- **Tipos Incorretos**: Como converter dados com tipos errados?

#### **Análises Adicionais Possíveis**
- **Por Método de Pagamento**: PIX vs Cartão vs Boleto - qual tem maior inadimplência?
- **Por Categoria de Produto**: Electronics vs Clothing - padrões diferentes?
- **Por Canal de Venda**: Online vs Loja física - comportamentos distintos?
- **Por Valor da Transação**: Transações altas vs baixas - correlação com inadimplência?

### Requisitos

- A solução deve ser confiável e preparada para escalabilidade em volumes maiores de dados.

### Diferencial

- Implemente o pipeline de ETL utilizando **Apache Spark** (Scala ou Python).

---

## Critérios de Avaliação

- **Eficiência**:  
  - Avaliaremos o tempo de execução do pipeline e o uso de recursos, principalmente em operações distribuídas com Apache Spark.
  
- **Clareza do Código**:  
  - O código deve ser claro, com nomeação adequada de variáveis, funções e módulos.
  
- **Documentação**:  
  - A presença de um README.md que detalhe as instruções de configuração, instalação, execução e explicação das decisões de design.  
  - Uso de docstrings (seguindo o PEP 257) e comentários esclarecedores sobre as escolhas técnicas.

- **Escalabilidade**:  
  - A solução deve ser capaz de lidar com um aumento no volume de dados e demonstrar boas práticas para operações distribuídas.

- **Testes**:  
  - A inclusão de testes unitários (ou de integração) para validar a funcionalidade do pipeline e as regras de negócio será considerada um diferencial.

---

## Entrega

- **Repositório Git**:  
  - A solução deve ser entregue através de um repositório Git. Faça uma cópia do repositório e envie do link do seu repositório com a solução.
  
- **Prazo**:  
  - O prazo para a entrega da solução é de 7 dias a partir do recebimento do desafio.
  
- **Instruções de Execução**:  
  - Inclua um arquivo `README.md` atualizado com instruções claras sobre como configurar o ambiente, instalar as dependências e executar a solução (incluindo exemplos de comandos, como `spark-submit spark_pipeline.py` e instruções de agendamento ou execução automatizada se implementadas).
