# TODO: Executar comandos no diretório que este arquivo se encontra!!!

# Gerar imagem a partir da Dockerfile
docker build -t mascarar-dados .

# Executar container a partir da imagem
docker run --rm imagem_etapa1 [args]
# Exemplo com argumentos
docker run --rm imagem_etapa1 teste string