-- EXERCÍCIOS ########################################################################

-- (Exercício 1) Identifique quais as marcas de veículo mais visitada na tabela sales.funnel
SELECT 
	p.brand,
	count(*) AS qtt
FROM sales.funnel f
LEFT JOIN sales.products p
	ON f.product_id = p.product_id
GROUP BY p.brand
ORDER BY qtt DESC;

-- (Exercício 2) Identifique quais as lojas de veículo mais visitadas na tabela sales.funnel
SELECT 
	s.store_name,
	count(*) AS visitas
FROM sales.funnel f 
LEFT JOIN sales.stores s 
	ON f.store_id = s.store_id
GROUP BY s.store_name
ORDER BY visitas DESC;

-- (Exercício 3) Identifique quantos clientes moram em cada tamanho de cidade (o porte da cidade
-- consta na coluna "size" da tabela temp_tables.regions)
SELECT 
	r."size",
	count(*) AS qtt
FROM sales.customers c 
LEFT JOIN temp_tables.regions r 
	ON lower(c.city) = lower(r.city )
	AND lower(c.state) = lower(r.state)
GROUP BY r."size"
ORDER BY qtt DESC;


