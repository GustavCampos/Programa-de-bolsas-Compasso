#!/bin/bash

#Variaveis iniciais ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#Local de execucao do script (pasta ecommerce)
run_path="/home/gustavo/Desafio-S1/ecommerce"

#Data atual formatada (yyyy/mm/dd)
dateF=$(date '+%Y/%m/%d')

#Data atual "crua" (yyyymmdd)
dateR=$(date '+%Y%m%d')

#Horário atua formatado (HH:MM)
timeF=$(date '+%H:%M')

#Nome csv original utilizado (ecommerce/dados_de_vendas.csv) 
og_file="dados_de_vendas.csv"

#Nome do csv com a data no final (dados-<yyyymmdd>.csv)
dated_file="dados-$dateR.csv"

#Nome do csv na pasta backup (backup-dados-<yymmdd>.csv)
bk_file="backup-$dated_file"

#Nome do arquivo de relatorio gerado ao final (relatorio-<yyyymmdd>.txt)
relatorio_file="relatorio-$dateR.txt"

#Sed regex para transformar "mmddyyyy" em "dd/mm/yyyy"
sedRegex="s#\(..\)\(..\)\(....\)#\2/\1/\3#"

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#Acessando local de execucao (pasta ecommerce)
cd $run_path

#Criando diretorio vendas caso nao exista
mkdir -p vendas

#Copiando dados do csv original (dados_de_vendas.csv) para o dir vendas
cp $og_file vendas/

#Acessando diretorio vendas
cd vendas

#Criando diretorio backup caso nao exista
mkdir -p backup

#Copiando csv de vendas para o diretorio backup com o nome 
#"dados-<yyyymmdd>.csv"
cp $og_file backup/$dated_file

#Acessando diretorio backup
cd backup

#Renomeando dados do backup de "dados-<yyyymmdd>.csv" 
#para "backup-dados-<yyyymmdd>.csv"
mv $dated_file $bk_file

#Criando relatorio_____________________________________________________________

#Criar arquivo relatorio-<yyyymmdd>.txt
touch $relatorio_file

#Adicionando data e hora de execucao do script a primeira linha do relatorio
echo "Relatorio gerado em:             $dateF $timeF" >> $relatorio_file

#Adicionando linha em branco ao relatorio
echo -en '\n' >> $relatorio_file 

#Organizando datas por ordem crescente-----------------------------------------

#Cria uma lista apenas com o campo data dos registros 
#-F ',': Indica <,> como separador para cada variavel da linha
#'NR > 1': Ignora  a primeira linha do csv (header)
#'{print $5}': Registra apenas a 5 variavel (data)
orderedDates=$(awk -F ',' 'NR > 1 {print $5}' $bk_file)

#Cria uma lista com as datas em formato americano em ordem crescente
#echo "$orderedDates": reutiliza lista criada na linha acima
#-F '/': indica </> como separador para cada variavel da linha
#'{print $2$1$3}': Registra "mmddyyyy" na linha
#sort -n: ordena as datas de forma crescente
orderedDates=$(echo "$orderedDates" | awk -F '/' '{print $2$1$3}' | sort -n)

#------------------------------------------------------------------------------

#Pega a primeira linha das datas ordenadas (head -1) e converte novamente
#para formato "dd/mm/yyyy" (sed $sedRegex)
fr_date=$(echo "$orderedDates" | head -1 | sed $sedRegex)

#Pega linha das datas ordenadas (tail -1) e converte novamente
#para formato "dd/mm/yyyy" (sed $sedRegex)
lr_date=$(echo "$orderedDates" | tail -1 | sed $sedRegex)

#Adicionando data da primeira venda realizada ao relatorio
echo "Data do primeiro registro:       $fr_date" >> $relatorio_file

#Adicionando data da ultima venda realizada ao relatorio
echo "Data do ultimo registro:         $lr_date" >> $relatorio_file

#Cria um conjunto com os nomes dos produtos vendidos
#-F ',': Indica <,> como separador para cada variavel da linha
#'NR > 1': Ignora  a primeira linha do csv (header)
#'{print $2}': Registra apenas a 2 variavel (produto)
#sort: Ordena a lista de produtos em ordem alfabetica crescente
#uniq: Remove registros adjacentes repetidos (necessario ordenar 
#alfabeticamente para execucao correta do comando)
productSet=$(awk -F ',' 'NR > 1 {print $2}' $bk_file | sort | uniq)

#Conta quantidade de linhas da conjunto criado acima
total_quantity=$(echo "$productSet" | wc -l)

#Adiciona a quantidade de itens diferentes vendidos ao relatorio
echo "Total itens diferentes vendidos: $total_quantity" >> $relatorio_file

#Adicionando linha em branco ao relatorio
echo -en '\n' >> $relatorio_file 

#Adicionando as 10 primeiras linhas de backup-dados-<yyyymmdd>.csv 
#ao relatorio.txt
head -10 $bk_file >> $relatorio_file

#______________________________________________________________________________

#Compactando arquivo backup-dados-<yyyymmdd>.csv
zip -r backup-dados-$dateR.zip $bk_file

#Removendo arquivo backup-dados-<yyyymmdd>.csv
rm $bk_file

#Acessando a pasta acima de backup (vendas)
cd ..

#Removendo dados-<yyyymmdd>.csv da pasta vendas
rm $og_file
