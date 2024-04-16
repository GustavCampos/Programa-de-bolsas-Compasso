#!/bin/bash

#Variaveis _____________________________________________
#Path de execucao do script
path="/home/gustavo/Desafio-S1"

#Arquivo relatorio final
relatorio_file="relatorio_final.txt"
#_______________________________________________________

#Acessando pasta de execucao
cd $path/vendas/backup

for file in relatorio-*
do
	cat $file >> $relatorio_file
	echo -en '\n' >> $relatorio_file #linha em branco
	echo "----------------------------------------" >> $relatorio_file
done
