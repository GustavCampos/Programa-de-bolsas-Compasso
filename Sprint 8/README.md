# Desafio

1. [Processamento para Camada Trusted](Desafios/)

# Exercícios

1. [Geração e Massa de Dados](Exercícios/Ex1/Ex1.ipynb)
2. [Apache Spark](Exercícios/Ex2/Ex2.ipynb)
1. [Consumo API TMDB](Exercícios/Ex3/tmdb_test.py)

# Evidências

* Arquivo txt criado no exercício 1 para ser usado no exercício 2 dentro do container Spark.

![ex1_output](Evidências/ex1_output.png)


* Execução das etapas do exercício 2 dentro do container Spark.

![ex2_1](Evidências/ex2_1.png)
![ex2_2](Evidências/ex2_2.png)
![ex2_3](Evidências/ex2_3.png)
![ex2_4](Evidências/ex2_4.png)
![ex2_5](Evidências/ex2_5.png)
![ex2_6](Evidências/ex2_6.png)
![ex2_7](Evidências/ex2_7.png)
![ex2_8](Evidências/ex2_8.png)
![ex2_9](Evidências/ex2_9.png)


* Execução do Script [tmdb_test.py](Exercícios/Ex3/tmdb_test.py) criado para o exercício 3.

![ex3_execution](Evidências/ex3_execution.png)

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

* Estrutura de diretórios do S3 para os arquivos temporários de teste durante desenvolvimento dos Jobs do Glue.

![temp_tree](Evidências/temp_tree.png)

* Estrutura de diretórios do S3 para os arquivos da camada ***Raw***.

![raw_tree](Evidências/raw_tree.png)

* Estrutura de diretórios do S3 para os arquivos criados para a camada ***Trusted*** pelos Jobs do Glue.

![trusted_tree](Evidências/trusted_tree.png)

