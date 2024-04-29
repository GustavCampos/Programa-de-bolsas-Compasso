--Excluindo fato e dimensoes existentes para teste_____________________________
DROP VIEW IF EXISTS dim_cliente;
DROP VIEW IF EXISTS dim_vendedor;
DROP VIEW IF EXISTS dim_carro;
DROP VIEW IF EXISTS dim_data;
DROP VIEW IF EXISTS fato_locacao;

--Criando as views para modelo dimensional_____________________________________

--Criando dimensao de cliente
CREATE VIEW dim_cliente AS
SELECT 
	c.id,
	c.nome,
	ci.nome AS cidade,
	e.nome AS estado,
	p.nome AS pais
FROM cliente c
LEFT JOIN cidade ci
	ON c.cidade = ci.id
LEFT JOIN estado e
	ON ci.estado = e.id
LEFT JOIN pais p
	ON e.pais = p.id;

--Criando dimensao de vendedor
CREATE VIEW dim_vendedor AS
SELECT 
	v.id,
	v.nome,
	v.sexo,
	e.nome AS estado,
	p.nome AS pais
FROM vendedor v
LEFT JOIN estado e
	ON v.estado = e.id
LEFT JOIN pais p
	ON e.pais = p.id;

--Criando dimensao de carro
CREATE VIEW dim_carro AS
SELECT 
	c.id,
	c.chaci,
	co.tipo AS combustivel,
	mc.nome AS modelo,
	m.nome AS marca,
	c.ano
FROM carro c
LEFT JOIN modelo_carro mc 
	ON c.modelo = mc.id
LEFT JOIN marca m
	ON mc.marca = m.id
LEFT JOIN combustivel co
	ON c.combustivel = co.id;

--Criando dimensao de data
CREATE VIEW dim_data AS
WITH todas_datas AS (
	SELECT DISTINCT
		DATETIME(CONCAT(data_retirada, ' ', hora_retirada)) AS data
	FROM locacao l
	UNION
	SELECT DISTINCT
		DATETIME(CONCAT(data_entrega, ' ', hora_entrega)) AS data
	FROM locacao l
)
SELECT
	data AS data_completa,
	CAST(STRFTIME('%Y', data) AS INT) AS ano,
	CAST(FLOOR((STRFTIME('%m', DATE(data)) + 2)/3) AS INT) AS trimestre,
	CAST(STRFTIME('%m', data) AS INT) AS mes,
	CAST(STRFTIME('%d', data) AS INT) AS dia,
	CAST(STRFTIME('%H', data) AS INT) AS hora,
	CAST(STRFTIME('%M', data) AS INT) AS minuto
FROM todas_datas;

--Criando fato locacao
CREATE VIEW fato_locacao AS
SELECT 
	id,
	DATETIME(CONCAT(data_retirada, ' ', hora_retirada)) AS data_retirada,
	DATETIME(CONCAT(data_entrega, ' ', hora_entrega)) AS data_entrega,
	cliente AS id_cliente,
	carro AS id_carro,
	vendedor AS id_vendedor,
	kilometragem_entrada,
	quantidade_diarias,
	valor_diaria
FROM locacao l; 