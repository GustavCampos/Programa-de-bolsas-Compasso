--Query 1
WITH leads AS (
	--Vendas por mes
	SELECT
		date_trunc('month', visit_page_date)::date AS visit_month,
		count(*) qtt
	FROM sales.funnel f
	GROUP BY visit_month
	ORDER BY visit_month
), 
payments AS (
	--Pagamentos por mes
	SELECT 
		date_trunc('month', f.visit_page_date)::date AS payment_month,
		count(*) AS qtt,
		sum(p.price * (1 + f.discount)) AS receita
	FROM sales.funnel f
	LEFT JOIN sales.products p
		ON p.product_id  = f.product_id
	WHERE f.paid_date IS NOT NULL
	GROUP BY payment_month
	ORDER BY payment_month
)
SELECT 
	l.visit_month AS "mês",
	l.qtt AS "leads (#)",
	p.qtt AS "vendas (#)",
	p.receita/1000 AS "receita (k, R$)",
	(p.qtt::float/l.qtt::float) AS "conversão (%)",
	p.receita/p.qtt/1000 AS "ticket médio (k, R$)"
FROM leads l
LEFT JOIN payments p
	ON l.visit_month = p.payment_month;


--Query 2
SELECT
	'Brasil' AS "país",
	c.state AS estado,
	count(*) AS "vendas (#)"
FROM sales.funnel f 
LEFT JOIN sales.customers c
	ON f.customer_id = c.customer_id
GROUP BY c.state
ORDER BY "vendas (#)" DESC;
	

--Query 3
SELECT 
	p.brand AS marca,
	count(*) AS "vendas (#)"
FROM sales.funnel f
LEFT JOIN sales.products p
	ON p.product_id = f.product_id
WHERE paid_date BETWEEN'2021-08-01' AND '2021-08-31'
GROUP BY marca
ORDER BY "vendas (#)" DESC;

--Query 4
SELECT 
	s.store_name AS loja,
	count(*) AS "vendas (#)"
FROM sales.funnel f 
LEFT JOIN sales.stores s 
	ON f.store_id = s.store_id
GROUP BY s.store_name
ORDER BY "vendas (#)" DESC;

--Query 5
SELECT 
	EXTRACT('dow' FROM visit_page_date) AS dia_da_semana,
	CASE 
		WHEN EXTRACT('dow' FROM visit_page_date) = 0 THEN 'domingo'
		WHEN EXTRACT('dow' FROM visit_page_date) = 1 THEN 'segunda'
		WHEN EXTRACT('dow' FROM visit_page_date) = 2 THEN 'terça'
		WHEN EXTRACT('dow' FROM visit_page_date) = 3 THEN 'quarta'
		WHEN EXTRACT('dow' FROM visit_page_date) = 4 THEN 'quinta'
		WHEN EXTRACT('dow' FROM visit_page_date) = 5 THEN 'sexta'
		WHEN EXTRACT('dow' FROM visit_page_date) = 6 THEN 'sábado'
		ELSE NULL
	END AS "dia da semana",
	count(*) AS "vendas (#)"
FROM sales.funnel f
GROUP BY dia_da_semana
ORDER BY "vendas (#)" DESC;
