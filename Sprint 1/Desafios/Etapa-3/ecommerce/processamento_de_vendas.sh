#!/bin/bash

#Variaveis iniciais ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#Local de execucao do script (pasta ecommerce)
run_path="/home/gustavo/Desafio-S1/ecommerce"

dateF=$(date '+%Y/%m/%d')
dateR=$(date '+%Y%m%d')
timeF=$(date '+%H:%M')

og_file="dados_de_vendas.csv"
dated_file="dados-$dateR.csv"
bk_file="backup-$dated_file"
relatorio_file="relatorio-$dateR.txt"

#Regra para substituir "mmddyyyy" por "dd/mm/yyyy"
sedRegex="s#\(..\)\(..\)\(....\)#\2/\1/\3#"

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#Acessando local de execucao
cd $run_path

#Criando dir vendas caso nao exista
mkdir -p vendas

#Copiando dados de vendas
cp $og_file vendas/

#Criando dir backup caso nao exista
cd vendas
mkdir -p backup

#Copiando dados para o backup
cp $og_file backup/$dated_file

#Renomeando dados do backup
cd backup
mv $dated_file $bk_file

#Criando relatorio____________________________________________________________
touch $relatorio_file

echo "Relatorio gerado em:             $dateF $timeF" >> $relatorio_file

echo -en '\n' >> $relatorio_file #linha em branco

#Cria uma lista apenas com o campo data
orderedDates=$(awk -F ',' 'NR > 1 {print $5}' $bk_file)
#Cria uma lista com as datas no formato "mmddyyyy" e ordena elas
orderedDates=$(echo "$orderedDates" | awk -F '/' '{print $2$1$3}' | sort -n)

fr_date=$(echo "$orderedDates" | head -1 | sed $sedRegex)
lr_date=$(echo "$orderedDates" | tail -1 | sed $sedRegex)

echo "Data do primeiro registro:       $fr_date" >> $relatorio_file
echo "Data do ultimo registro:         $lr_date" >> $relatorio_file

#Mesma coisa que o comando acima só que para o nome dos produtos, e por fim
#remove duplicadas adjacentes na lista
productSet=$(awk -F ',' 'NR > 1 {print $2}' $bk_file | sort | uniq)

#Conta quantidade de linhas da lista totalmente tratada
total_quantity=$(echo "$productSet" | wc -l)

echo "Total itens diferentes vendidos: $total_quantity" >> $relatorio_file

echo -en '\n' >> $relatorio_file #linha em branco

head -10 $bk_file >> $relatorio_file
#______________________________________________________________________________

#Compactando arquivo csv de backup
zip -r backup-dados-$dateR.zip $bk_file

#Removendo arquivo csv
rm $bk_file

#Removendo dados da pasta vendas
cd .. #vendas
rm $og_file
