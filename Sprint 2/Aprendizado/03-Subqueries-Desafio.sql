-- EXERCÍCIOS ########################################################################

-- (Exercício 1) Crie uma coluna calculada com o número de visitas realizadas por cada
-- cliente da tabela sales.customers
WITH visitas AS (
	SELECT 
		customer_id,
		count(*) AS num_visitas
	FROM sales.funnel f
	GROUP BY customer_id
)
SELECT *
FROM sales.customers c
LEFT JOIN visitas USING(customer_id);
