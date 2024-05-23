# TODO: Executar comandos no diretório que este arquivo se encontra!!!

# Gerar imagem a partir da Dockerfile
docker build -t imagem_etapa1 .

# Executando container a partir da imagem criada_______________________________

# Container de execução única (se auto remove após execução)
docker run --rm imagem_etapa1

# Container persistente (utilizado apenas na primeira execução)
docker run --name container_etapa1 imagem_etapa1

# Executando container persistente novamente
docker start -i container_etapa1