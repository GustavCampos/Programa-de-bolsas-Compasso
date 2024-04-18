#!/bin/bash

#Variaveis _____________________________________________
#Path de execucao do script (pasta ecommerce)
path="/home/gustavo/Desafio-S1/ecommerce"

#Arquivo relatorio final
relatorio_file="relatorio_final.txt"
#_______________________________________________________

#Acessando pasta de execucao
cd $path/vendas/backup

#Itera por todos os relatorios com data
for file in relatorio-*
do
	#Adiciona conteúdo do relatorio ao relatorio final
	cat $file >> $relatorio_file
	echo -en '\n' >> $relatorio_file #linha em branco
	#Printar um separador
	echo "----------------------------------------" >> $relatorio_file
done
