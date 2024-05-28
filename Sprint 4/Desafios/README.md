# Etapas

---

## Etapa 1:

* Script python utilizado: [carguru.py](./Etapa-1/carguru.py)
* Dockerfile criada para executar script: [Dockerfile](./Etapa-1/Dockerfile)

## Etapa 2:

* Markdown respondendo a pergunta apresentada: [Etapa-2.md](./Etapa-2/Etapa-2.md)

## Etapa 3:

* Script python utilizado: [script.py](./Etapa-3/script.py)
* Dockerfile criada para executar script: [Dockerfile](./Etapa-3/Dockerfile)

---

# Passos para Execução

Antes de tudo devemos garantir que estamos acessando a pasta de Desafio desta sprint.

```bash
cd "./Sprint 4/Desafios/"
```

Basta agora apenas utilizar o docker compose para gerar ambas as imagens que serão utilizadas neste desafio.

```bash
docker compose up -d
```

Devemos ter o seguinte output ao listar as imagens e containeres criados.

![imagens_containeres_criados.png](../Evidências/imagens_containeres_criados.png)

Agora podemos utiilizar os scripts de cada container:

1. O container da primeira etapa está parado e sua função é unicamente retornar uma string ao ser executado e parar novamente. Podemos roda-lo da seguinte maneira:
```bash
docker start -i carguru-container
# -> Você deve dirigir um Peugeot 208
```

2. O container da terceira etapa fica rodando de plano de fundo e devemos enviar strings de hash para ele a partir do comando exec:

```bash
docker exec mascarar-dados-container python script.py [<args>]

#Ex:
docker exec mascarar-dados-container python script.py gustavcampos \#SenhaParaAlgoSecreto
# -> 456ba286aba355e488d120d19ce5e549f5aaaec8
# -> bc0c31a8f30cbe0a739ebf1c0378903abf4d0a65
```

Por ultimo podemos finalizar o container em execução e excluir os containeres criados.
```bash
docker compose down
```
> Importante destacar que as imagens construídas persistem no local para evitar a necessidade de rebuild ao rodar o *compose* novamente.