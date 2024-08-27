--Ex 8
--O código e o nome do vendedor com maior número de vendas (contagem)
SELECT 
	vdd.cdvdd,
	vdd.nmvdd
FROM tbvendas v
LEFT JOIN tbvendedor vdd
	ON vdd.cdvdd = v.cdvdd
GROUP BY v.cdvdd
HAVING LOWER(v.status) = 'concluído'
ORDER BY count(v.cdven) DESC
LIMIT 1;

--Ex 9
--O código e nome do produto mais vendido entre as datas de 2014-02-03 até 2018-02-02'
WITH filtered_vendas AS (
	SELECT *  FROM tbvendas
	WHERE 
		LOWER(status) = 'concluído' AND
		dtven BETWEEN '2014-02-03' AND '2018-02-02'
)
SELECT
	p.cdpro,
	t.nmpro
FROM filtered_vendas t
RIGHT JOIN tbestoqueproduto p
GROUP BY t.nmpro, p.cdpro
ORDER BY count(t.cdven) DESC
LIMIT 1;

--Ex 10
--Calcule a comissão de todos os vendedores, considerando todas as vendas armazenadas na base de dados
SELECT 
	vdd.nmvdd AS vendedor,
	ROUND(SUM(t.qtd * t.vrunt), 2) AS valor_total_vendas,
	ROUND(SUM((t.qtd * t.vrunt) * (CAST(vdd.perccomissao AS float)/100)), 2) AS comissao
FROM tbvendas t
LEFT JOIN tbvendedor vdd
	ON vdd.cdvdd = t.cdvdd 
WHERE LOWER(t.status) = 'concluído'
GROUP BY t.cdvdd
ORDER BY comissao desc; 

--Ex 11
--O código e nome cliente com maior gasto na loja'
SELECT
	cdcli,
	nmcli,
	SUM(qtd * vrunt) AS gasto
FROM tbvendas t
GROUP BY cdcli
ORDER BY gasto DESC
LIMIT 1;

--Ex 12
--Código, nome 'e data de nascimento dos dependentes do vendedor com menor valor total bruto em vendas (não sendo zero)
--cddep, nmdep, dtnasc e valor_total_vendas
WITH menor_vendedor AS (
	SELECT 
		vdd.cdvdd,
		ROUND(SUM(t.qtd * t.vrunt), 2) AS valor_total_vendas
	FROM tbvendas t
	LEFT JOIN tbvendedor vdd
		ON vdd.cdvdd = t.cdvdd
	WHERE LOWER(t.status) = 'concluído'
	GROUP BY t.cdvdd
	ORDER BY valor_total_vendas
	LIMIT 1
)
SELECT 
	cddep,
	nmdep,
	dtnasc,
	valor_total_vendas
FROM tbdependente t
INNER JOIN menor_vendedor mv
	ON mv.cdvdd = t.cdvdd;

--Ex 13
--Listar os 10 produtos menos vendidos pelos canais de E-Commerce ou Matriz'
SELECT
	cdpro,
	nmcanalvendas,
	nmpro,
	SUM(qtd) quantidade_vendas
FROM tbvendas t 
WHERE 
	LOWER(status) = 'concluído' AND
	LOWER(nmcanalvendas) IN ('ecommerce', 'matriz')
GROUP BY nmcanalvendas, cdpro
ORDER BY quantidade_vendas;

--Ex 14
--O gasto médio por estado da federação'
SELECT
	estado,
	ROUND(AVG(vrunt * qtd), 2) AS gastomedio
FROM tbvendas t 
WHERE LOWER(status) = 'concluído'
GROUP BY pais, estado
ORDER BY gastomedio DESC;

--Ex 15
--Os códigos das vendas identificadas como deletadas'
SELECT cdven FROM tbvendas t
WHERE deletado IS TRUE
ORDER BY cdven;

--Ex 16
--A quantidade média vendida de cada produto agrupado por estado da federação'
SELECT
	estado,
	nmpro,
	ROUND(AVG(qtd), 4) AS quantidade_media
FROM tbvendas t 
WHERE LOWER(status) = 'concluído'
GROUP BY pais, estado, cdpro
ORDER BY estado, nmpro;
