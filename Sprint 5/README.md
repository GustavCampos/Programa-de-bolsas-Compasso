# Desafio

1. [Consultando buckets AWS com S3Select e Boto3](Desafios/)

# Evidências

* Arquivo JSON [canais-de-programacao-de-programadoras-ativos-credenciados.json](Desafios/data/canais-de-programacao-de-programadoras-ativos-credenciados.json) retirado da base de dados pública do governo utilizado para o desafio

![raw_dataset.png](Evidências/raw_dataset.png)

* Arquivo CSV [canais-credenciados.csv](Desafios/data/canais-credenciados.csv), arquivo resultante do processamento dos dados em [data_processing.py](Desafios/data-processing/data_processing.py).

![processed_dataset.png](Evidências/processed_dataset.png)

* Arquivo ENV [aws_parameters.env](Desafios/data/aws_parameters.env) que contêm variáveis importantes para o acesso da AWS. Utilizado por [data_upload.py](Desafios/data-upload/data_upload.py) e [data_query.py](Desafios/data-query/data_query.py)

![aws_parameters.png](Evidências/aws_parameters.png)

* Arquivo JSON [query-result.json](Desafios/data/query-result.json), arquivo gerado a partir do resultado da consulta realizada em [data_query.py](Desafios/data-query/data_query.py).

![data_query_result.png](Evidências/data_query_result.png)

* Script [data_processing.py](Desafios/data-processing/data_processing.py) utilizado para o desafio, ele processa e normaliza os dados do JSON referenciado acima e cria um CSV com os dados processados.

![data_processing_script.png](Evidências/data_processing_script.png)

* Arquivo [Dockerfile](Desafios/data-processing/Dockerfile) em [data-processing](Desafios/data-processing/), serve para criar uma imagem docker com o ambiente necessario para rodar o script descrito acima.

![dockerfile_data_processing.png](Evidências/dockerfile_data_processing.png)

* Script [data_upload.py](Desafios/data-upload/data_upload.py) utilizado para o desafio, ele utiliza utiliza os paramêtros de [aws_parameters.env](Desafios/data/aws_parameters.env) para acessar a AWS. Ele também verifica se [canais-credenciados.csv](Desafios/data/canais-credenciados.csv) já foi enviado para nuvem ou faz o seu upload em caso negativo.

![data_upload_script.png](Evidências/data_upload_script.png)

* Arquivo [Dockerfile](Desafios/data-upload/Dockerfile) em [data-upload](Desafios/data-upload/), serve para criar uma imagem docker com o ambiente necessario para rodar o script descrito acima.

![dockerfile_data_upload.png](Evidências/dockerfile_data_upload.png)

* Script [data_query.py](Desafios/data-query/data_query.py) utilizado para o desafio, ele utiliza utiliza os paramêtros de [aws_parameters.env](Desafios/data/aws_parameters.env) para acessar a AWS. Ele também utiliza o script SQL [s3select_query.sql](Desafios/data-query/s3select_query.sql) para buscar os dados na AWS.

![data_query_script.png](Evidências/data_query_script.png)

* Arquivo [Dockerfile](Desafios/data-query/Dockerfile) em [data-query](Desafios/data-query/), serve para criar uma imagem docker com o ambiente necessario para rodar o script descrito acima.

![dockerfile_data_upload.png](Evidências/dockerfile_data_query.png)

* Script [s3select_query.sql](Desafios/data-query/s3select_query.sql) com uma query S3Select utilizada por [data_query.py](Desafios/data-query/data_query.py).

![data_query_sql.png](Evidências/data_query_sql.png)

* Arquivo [docker-compose.yml](Desafios/docker-compose.yml) utilizado configurar a criação e execução dos containeres utilizados no desafio.

![docker_compose.png](Evidências/docker_compose.png)

* Log da criação dos conteineres criados para rodar os scripts descritos acima utilizando o [docker-compose.yml](Desafios/docker-compose.yml).

![image_build.png](Evidências/image_build.png)

* Log da execução do conteiner criado para rodar o script [data_processing.py](Desafios/data-processing/data_processing.py).

![data_processing_log.png](Evidências/data_processing_log.png)

* Log da execução do conteiner criado para rodar o script [data_upload.py](Desafios/data-upload/data_upload.py).

Log com bucket já criado e upload do arquivo já feito.
![data_upload_log_1.png](Evidências/data_upload_log_1.png)

Log com bucket não criado e arquivo não enviado.
![data_upload_log_2.png](Evidências/data_upload_log_2.png)

* Log da execução do conteiner criado para rodar o script [data_query.py](Desafios/data-query/data_query.py).

![data_query_log.png](Evidências/data_query_log.png)

# Certificados

- Badge Cloud Quest: Cloud Practioner

![Badge Cloud Practioner](Certificados/Badge%20Cloud%20Practitioner.png)

- Certificado AWS Certified Cloud Practioner - [Versão PDF](Certificados/AWS%20Certified%20Cloud%20Practioner.pdf)

![AWS Certified Cloud Practioner](Certificados/AWS%20Certified%20Cloud%20Practioner.png)