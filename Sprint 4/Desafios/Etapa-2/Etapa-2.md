### É possível reutilizar containers?

> A resposta curta é: **Sim!**

Vamos dar uma olhada nos comandos descritos em [comandos.sh](../Etapa-1/comandos.sh) para a etapa 1 como exemplo:

```bash
#...
# Container de execução única (se auto remove após execução)
docker run --rm imagem_etapa1

# Container persistente (utilizado apenas na primeira execução)
docker run --name container_etapa1 imagem_etapa1

# Executando container persistente novamente
docker start -i container_etapa1
```

Como mostrado, a menos que um container tenha sido criado com a tag de autoremove ```--rm```, o container fica salvo no ambiente docker e pode ser reiniciado com o comando ```start```, bastando saber apenas seu nome ou id, que pode ser conferido usando o comando ```container ls -a```