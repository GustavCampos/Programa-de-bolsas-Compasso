# Etapas

---

## Contextualização do Desafio Final:

* Como a Squad 1 deve abordar filmes/séries de comédia ou animação, tenho como idéia inicial verificar se o gênero de animação possuía algum tipo de estigma e caso verdadeiro, como ele evoluiu ao longo do tempo.
* Pretendo verificar quais genêros que mais aparecem em conjunto com animação em cada década e verificar se os filmes mais populares dos atores/dubladores que aparecem nestes filmes são animações.

## Ingestão de Dados:

* Script principal: [lambda_function.py](Local/lambda_function.py)
* Arquivo de configuração: [config.json](Local/config.json)
* Script de definição de constantes: [constants.py](Local/constants.py)
* Script de funções utilitárias: [utils.py](Local/utils.py)
* Script de funções para requisição da API: [request_functions.py](Local/request_functions.py)
* Script de funções para o S3: [s3_functions.py](Local/s3_functions.py)
* Script para teste local: [local_test.py](Local/local_test.py)
---

# Passos para reexecução do desafio

Como primeiro passo devemos baixar os arquivos CSV que serão enviados para o Bucket e deixá-los na pasta [Data](data/) dentro de desafios.

![desafio_folder.png](../Evidências/desafio_folder.png)

Em sequência, devemos garantir que estamos acessando a pasta de Desafio desta sprint.

```bash
cd "<path_repositorio>/Sprint 6/Desafios/"
```

Na pasta [data](data/) é necessário inserir os dados necessários para o acesso a AWS no arquivo [aws_parameters.json](data/aws_parameters.json)

```json
"AWSAccessKeyId":  # Chave de acesso AWS,
"AWSSecretAccessKey": # Chave de acesso secreta,
"AWSSessionToken": # Token da sessão utilizada,
"AWSRegion": # Região utilizada,
"AWSBucketName": # Nome do bucket que será acessado
```
Basta agora apenas utilizar o docker compose para gerar as imagens e containeres que serão utilizadas neste desafio.

```bash
docker compose up
```

Devemos ter o seguinte output ao listar as imagens e containeres criados:

1. Irá aparecer o log de criação dos containeres
![image_build.png](../Evidências/image_build.png)


3. Será criado uma sessão com o s3 da amazon e será verificado a existencia/acesso dos bucket e objeto descritos em [aws_parameters.json](data/aws_parameters.json)
![no_bucket_compose.png](../Evidências/no_bucket_compose.png)

Podemos excluir os containeres criados após a execução completa deles com o comando:
```bash
docker compose down
```