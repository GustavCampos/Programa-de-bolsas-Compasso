# Desafio

1. [Processamento para Camada Trusted](Desafios/)

# Exercícios

1. [Geração e Massa de Dados](Exercícios/Ex1/Ex1.ipynb)
2. [Apache Spark](Exercícios/Ex2/Ex2.ipynb)
1. [Consumo API TMDB](Exercícios/Ex3/tmdb_test.py)

# Evidências

* Execução do Script [tmdb_test.py](Exercícios/Ex3/tmdb_test.py) criado para o exercício 3.

![local_env_vars](Evidências/ex3_execution.png)

* Logs de execução do AWS Glue Job ```createTrustedDataLocal```:

![job_local_log](Evidências/job_local_log.png)

* Logs de execução do AWS Glue Job ```createTrustedDataTMDB```:

![job_tmdb_1](Evidências/job_tmdb_1.png)
![job_tmdb_2](Evidências/job_tmdb_2.png)
![job_tmdb_3](Evidências/job_tmdb_3.png)

* Logs de execução do AWS Glue Crawler ```createTrustedLocalDataCrawler```:

![crawler_local_log](Evidências/crawler_local_log.png)

* Logs de execução do AWS Glue Crawler ```createTrustedTMDBDataCrawler```:

![crawler_tmdb_log](Evidências/crawler_tmdb_log.png)
