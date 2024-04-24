-- EXERCÍCIOS ########################################################################

-- (Exercício 1) Conte quantos clientes da tabela sales.customers tem menos de 30 anos
SELECT count(*) AS "Clientes com menos de 30 anos"
FROM sales.customers c
WHERE ((current_date - birth_date) / 365) < 30;


-- (Exercício 2) Informe a idade do cliente mais velho e mais novo da tabela sales.customers
SELECT
	(current_date  - MIN(birth_date)) / 365 AS "Cliente mais velho",
	(current_date  - MAX(birth_date)) / 365 AS "Cliente mais novo"
FROM sales.customers c;


-- (Exercício 3) Selecione todas as informações do cliente mais rico da tabela sales.customers
-- (possívelmente a resposta contém mais de um cliente)
SELECT * FROM sales.customers c 
WHERE income = (SELECT max(income) FROM sales.customers c2);


-- (Exercício 4) Conte quantos veículos de cada marca tem registrado na tabela sales.products
-- Ordene o resultado pelo nome da marca
SELECT brand, count(*) AS qtt FROM sales.products p
GROUP BY brand 
ORDER BY brand;


-- (Exercício 5) Conte quantos veículos existem registrados na tabela sales.products
-- por marca e ano do modelo. Ordene pela nome da marca e pelo ano do veículo
SELECT brand, model_year, count(*) AS qtt 
FROM sales.products p
GROUP BY brand, model_year
ORDER BY brand, model_year;


-- (Exercício 6) Conte quantos veículos de cada marca tem registrado na tabela sales.products
-- e mostre apenas as marcas que contém mais de 10 veículos registrados
SELECT brand, count(*) AS qtt
FROM sales.products p
GROUP BY brand
HAVING count(*) > 10;

