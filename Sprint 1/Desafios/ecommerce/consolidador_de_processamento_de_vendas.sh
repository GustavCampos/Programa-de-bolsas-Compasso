#!/bin/bash

#Variaveis _____________________________________________
#Path de execucao do script (pasta ecommerce)
path="/home/gustavo/Desafio-S1/ecommerce"

#Nome do arquivo de relatorio final
relatorio_file="relatorio_final.txt"
#_______________________________________________________

#Acessando pasta backup para ler relatorios
cd $path/vendas/backup

#Itera por todos os relatorios com data (relatorio-<mmddyyyy>.txt)
for file in relatorio-*
do
	#Adiciona conteúdo do relatorio em $file ao relatorio final
	cat $file >> $relatorio_file

	#linha em branco
	echo -en '\n' >> $relatorio_file 
	
	#Printa uma linha de "-" como separador entre relatorios
	echo "----------------------------------------" >> $relatorio_file
done
