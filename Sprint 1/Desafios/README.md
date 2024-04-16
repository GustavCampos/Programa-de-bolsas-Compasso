
# Instruções

Neste arquivo você irá apresentar suas entregas referentes ao desafio final. 
O desafio está presente em cada sprint ao longo do estágio. Utilize o diretório "Desafio" para organizar seus artefatos e este README.md para fazer referência aos arquivos de código-fonte e demais entregáveis solicitados.


# Etapas

## 1. Setup inicial de arquivos

Comandos a serem utilizados nessa seção se encontram em [comandos.txt](Etapa-1/comandos.txt)

Dentro de uma pasta de preferência adicione a pasta [ecommerce](Etapa-1/ecommerce/) e seus script dentro ([processamento_de_vendas.sh](Etapa-1/ecommerce/processamento_de_vendas.sh) e [consolidador_de_processamento_de_vendas.sh](Etapa-1/ecommerce/consolidador_de_processamento_de_vendas.sh) )

Também adicione [dados_de_vendas-dia_1.csv](dados_de_vendas-dia_1.csv) a pasta com o nome "dados_de_vendas.csv"

![Estrutura Inicial](../Evidências/Estrutura_inicial.png)

Conceda permissão de execução para ambos os scripts
![Concedendo permissao](../Evidências/Concendendo_permissao_scripts.png)

Dentro de um editor de texto altere a variável "path" em ambos os scripts para o local de execução desejado (pasta ecommerce)

![path para os scripts](../Evidências/get_path.png)

![path processamento_de_vendas.sh](../Evidências/path-processamento_vendas.png)

![path consolidador_de_processamento_de_vendas.sh](../Evidências/path-consolidador_vendas.png)


## 2. Agendando execução do script
Instale o Cron utilizando o gerenciador de pacotes

![instalando cron](../Evidências/Install_cron.png)

Acesse o arquivo crontab e agenda a execucao dos script para 15:27 de seg-sex (comando em [comandos.txt](Etapa-2/comandos.txt))

![arquivo agendamento crontab](../Evidências/change_crontab.png)

## 3. Gerar novos relatorio para 
[Chat com geraçao dos novos csv](https://chat.openai.com/share/95544468-c001-474f-be19-ff94b385cd9b)




