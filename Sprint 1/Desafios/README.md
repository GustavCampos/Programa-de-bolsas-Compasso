
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
Comandos a serem utilizados nessa seção se encontram em [comandos.txt](Etapa-2/comandos.txt)

Instale o Cron utilizando o gerenciador de pacotes

![instalando cron](../Evidências/Install_cron.png)

Acesse o arquivo crontab e agende a execucao dos script para 15:27 de seg-sex

![arquivo agendamento crontab](../Evidências/change_crontab.png)

## 3. Execução

Com todos os passo de setup e agendamento prontos o projeto deve se parecer com isso:

_Estrutura do projeto antes de qualquer execução_
![Estrutura inicial](../Evidências/Estrutura_inicial_c_permissao.png)

A única necessidade restante é a alteração do arquivo "ecommerce/dados_de_vendas.csv" após cada execução do script. Arquivos se encontram na pasta [Desafios](.) ([dia 2](dados_de_vendas-dia_2.csv), [dia 3](dados_de_vendas-dia_3.csv)).

_Estrutura  do projeto após primeira execução_
![Estrutura apos primeira execucao](../Evidências/Primeira_execucao.png)

_Estrutura  do projeto após segunda execução_
![Estrutura apos primeira execucao](../Evidências/Segunda%20_execucao.png)

_Diferença nos relatórios gerados_
![Diferenca relatorios gerados](../Evidências/Diferenca_relatorio.png)

Após todas as execuções automáticas do [processamento_de_ vendas.sh](Etapa-1/ecommerce/processamento_de_vendas.sh) podemos finalmente executar o [consolidamento_de_processamento_de_vendas.sh](Etapa-1/ecommerce/consolidador_de_processamento_de_vendas.sh)

_Execução do Consolidador de Vendas e Estrutura final do projeto_
![Consolidador de vendas](../Evidências/consolidador_de_vendas.png)


#joaskdjaosjd
[Chat com geraçao dos novos csv](https://chat.openai.com/share/95544468-c001-474f-be19-ff94b385cd9b)




