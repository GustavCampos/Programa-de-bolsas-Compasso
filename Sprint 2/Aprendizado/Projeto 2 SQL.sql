--Query 1
SELECT 
	CASE 
		WHEN ig.gender = 'male' THEN 'homem'
		WHEN ig.gender = 'female' THEN 'mulher'
	END AS "gênero",
	count(*) AS "leads (#)"
FROM sales.funnel f 
LEFT JOIN sales.customers c
	ON c.customer_id = f.customer_id 
LEFT JOIN temp_tables.ibge_genders ig
	ON lower(ig.first_name) = lower(c.first_name)
GROUP BY ig.gender
ORDER BY "leads (#)" DESC;

--Query 2
SELECT 
	c.professional_status,
	(count(*) * 100) / (SELECT count(*) FROM sales.funnel f)::float AS "leads(%)"
FROM sales.funnel f
LEFT JOIN sales.customers c
	ON c.customer_id  = f.customer_id
GROUP BY professional_status;

--Query 3
SELECT
	CASE
		WHEN (current_date - c.birth_date)/365 < 20 THEN '0-20'
		WHEN (current_date - c.birth_date)/365 < 40 THEN '20-40'
		WHEN (current_date - c.birth_date)/365 < 60 THEN '40-60'
		WHEN (current_date - c.birth_date)/365 < 80 THEN '60-80'
		ELSE '80+' 
	END AS "faixa etária",
	(count(*) * 100) / (SELECT count(*) FROM sales.funnel f)::float AS "leads (%)"
FROM sales.funnel f
LEFT JOIN sales.customers c
	ON f.customer_id  = c.customer_id
GROUP BY "faixa etária"
ORDER BY "leads (%)" DESC;