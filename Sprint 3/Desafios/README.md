
# Etapas

* [Etapa 1 - Ambiente](requirements.txt)
	Etapa desenvolvida no [jupyter notebook](Desafio.ipynb) e em [requirements.txt](requirements.txt).

* [Etapa 2 - Desenvolvimento](Desafio.ipynb)
	Etapa desenvolvida no [jupyter notebook](Desafio.ipynb).

# Passos Para Reexecução do Código

Todos os comandos a sere executados neste passo a passo se encontram em [comandos.txt](./comandos.txt)

## 1. Setup inicial de arquivos

Dentro de uma pasta de preferência crie uma pasta chamada "ecommerce" e adicione os scripts [processamento_de_vendas.sh](ecommerce/processamento_de_vendas.sh) e [consolidador_de_processamento_de_vendas.sh](ecommerce/consolidador_de_processamento_de_vendas.sh).

Também adicione [dados_de_vendas-dia_1.csv](dados_de_vendas-dia_1.csv) a pasta trocando o nome do arquivo para "dados_de_vendas.csv"

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

Acesse o arquivo crontab e agende a execucao dos script para 15:27 de seg-sex

![arquivo agendamento crontab](../Evidências/crontab_edit.png)

## 3. Execução

Com todos os passo de setup e agendamento prontos o projeto deve se parecer com isso:
![Estrutura inicial](../Evidências/Estrutura_inicial_c_permissao.png)

A única necessidade restante é a alteração do arquivo "ecommerce/dados_de_vendas.csv" após cada execução do script. Arquivos se encontram na pasta [Desafios](.) ([dia 2](dados_de_vendas-dia_2.csv), [dia 3](dados_de_vendas-dia_3.csv)).

_Estrutura  do projeto após primeira execução_

![Estrutura apos primeira execucao](../Evidências/Primeira_execucao.png)

_Estrutura  do projeto após segunda execução_

![Estrutura apos primeira execucao](../Evidências/Segunda%20_execucao.png)

_Diferença nos relatórios gerados_

![Diferenca relatorios gerados](../Evidências/Diferenca_relatorio.png)

Após todas as execuções automáticas do [processamento_de_ vendas.sh](ecommerce/processamento_de_vendas.sh) podemos finalmente executar o [consolidamento_de_processamento_de_vendas.sh](ecommerce/consolidador_de_processamento_de_vendas.sh)

_Execução do Consolidador de Vendas e Estrutura final do Projeto_

![Consolidador de vendas](../Evidências/consolidador_de_vendas.png)
