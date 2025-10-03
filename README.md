# SaudeBliss - Data Engineer Code Challenge

## Como executar
Este projeto usa PySpark para processar os arquivos `transactions.json` e `payments.json`, aplicar as regras de inadimplência (3m > 30 dias, 6m > 15 dias), e gerar datasets particionados por cliente inadimplente.

### Requisitos
- Python 3.8+
- Java 11+
- Apache Spark 3.3+ (modo local)
- PySpark instalado (`pip install pyspark`)

### Execução
```bash
spark-submit \
  spark_pipeline_refactor.py \
  --transactions ./transactions.json \
  --payments ./payments.json \
  --outdir ./output \
  --mode overwrite \
  --enriched_partitions processing_date \
  --mart_partitions processing_date,delinquent \
  --repartition 8
