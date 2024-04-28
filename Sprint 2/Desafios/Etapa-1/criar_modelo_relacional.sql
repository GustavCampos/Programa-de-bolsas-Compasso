--Excluindo tabelas existentes para teste
DROP TABLE IF EXISTS pais;
DROP TABLE IF EXISTS estado;
DROP TABLE IF EXISTS cidade;
DROP TABLE IF EXISTS combustivel;
DROP TABLE IF EXISTS marca;
DROP TABLE IF EXISTS modelo_carro;
DROP TABLE IF EXISTS carro;
DROP TABLE IF EXISTS cliente;
DROP TABLE IF EXISTS vendedor;
DROP TABLE IF EXISTS locacao;

--Tabelas de endereco
CREATE TABLE pais (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	nome VARCHAR(200) NOT NULL
);

CREATE TABLE estado (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	nome VARCHAR(200) NOT NULL,
	pais INT NOT NULL,
	FOREIGN KEY (pais) REFERENCES pais(id)
);

CREATE TABLE cidade (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	nome VARCHAR(200) NOT NULL,
	estado INT NOT NULL,
	FOREIGN KEY (estado) REFERENCES estado(id)
);

--Tabelas para os carros
CREATE TABLE combustivel (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	tipo VARCHAR(200) NOT NULL
);

CREATE TABLE marca (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	nome VARCHAR(200)
);

CREATE TABLE modelo_carro (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	nome VARCHAR(200) NOT NULL,
	marca INT NOT NULL,
	FOREIGN KEY (marca) REFERENCES marca(id)
);

CREATE TABLE carro (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	chaci VARCHAR(100) NOT NULL,
	combustivel INT NOT NULL,
	modelo INT NOT NULL,
	ano INT NOT NULL,
	FOREIGN KEY (combustivel) REFERENCES combustivel(id),
	FOREIGN KEY (modelo) REFERENCES modelo_carro(id)
);

--Cliente
CREATE TABLE cliente (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	nome VARCHAR(200) NOT NULL,
	cidade INT,
	FOREIGN KEY (cidade) REFERENCES cidade(id)
);

--Vendedor
CREATE TABLE vendedor (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	nome VARCHAR(200) NOT NULL, 
	sexo SMALLINT,
	estado INT NOT NULL,
	FOREIGN KEY (estado) REFERENCES estado(id)
);

--Nova tabela de locacao
CREATE TABLE locacao (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	cliente INT NOT NULL,
	carro INT NOT NULL,
	kilometragem_entrada INT NOT NULL,
	data_retirada DATE NOT NULL,
	hora_retirada TIME NOT NULL,
	quantidade_diarias INT DEFAULT 1,
	valor_diaria DECIMAL DEFAULT 0,
	data_entrega DATE,
	hora_entrega TIME,
	vendedor INT NOT NULL,
	FOREIGN KEY (cliente) REFERENCES cliente(id),
	FOREIGN KEY (carro) REFERENCES carro(id),
	FOREIGN KEY (vendedor) REFERENCES vendedor(id)
);

--Inserindo dados na banco de dados novo________________________________

--Adicionar paises
INSERT INTO pais (nome)
SELECT DISTINCT paisCliente FROM tb_locacao tl;

--Adicionar estados
INSERT INTO estado (nome, pais)
SELECT DISTINCT estadoCliente, p.id FROM tb_locacao tl
LEFT JOIN pais p
	ON LOWER(tl.paisCliente) = LOWER(p.nome);

--Adicionar cidades
INSERT INTO cidade (nome, estado)
WITH referencia_e_p AS (
	SELECT 
		e.*,
		p.nome AS nome_p
	FROM estado e 
	LEFT JOIN pais p
		ON e.pais = p.id
)
SELECT DISTINCT cidadeCliente, ep.id FROM tb_locacao tl
LEFT JOIN referencia_e_p AS ep
	ON LOWER(tl.paisCliente) = LOWER(ep.nome_p)
	AND LOWER(tl.estadoCliente) = LOWER(ep.nome);

--Adicionar clientes
INSERT INTO cliente (id, nome, cidade)
WITH referencia_cep AS (
	SELECT 
		c.id,
		c.nome,
		e.nome AS nome_e,
		p.nome AS nome_p
	FROM cidade c 
	LEFT JOIN estado e 
		ON e.id = c.estado
	LEFT JOIN pais p
		ON e.pais = p.id
)
SELECT DISTINCT
	idCliente,
	nomeCliente,
	cep.id
FROM tb_locacao tl
LEFT JOIN referencia_cep cep
	ON LOWER(tl.paisCliente) = LOWER(cep.nome_p)
	AND LOWER(tl.estadoCliente) = LOWER(cep.nome_e)
	AND LOWER(tl.cidadeCliente) = LOWER(cep.nome);

--Adicionar marcas
INSERT INTO marca (nome)
SELECT DISTINCT marcaCarro  FROM tb_locacao tl;

--Adicionar modelos de carro
INSERT INTO modelo_carro (nome, marca)
SELECT DISTINCT modeloCarro, m.id FROM tb_locacao tl
LEFT JOIN marca m
	ON LOWER(m.nome) = LOWER(tl.marcaCarro);

--Adicionar combustivel
INSERT INTO combustivel (id, tipo)
SELECT DISTINCT idcombustivel, tipoCombustivel  FROM tb_locacao tl;

--Adicionar carros
INSERT INTO carro (id, chaci, ano, modelo, combustivel)
WITH referencia_m_m AS (
	SELECT 
		mc.id,
		mc.nome AS nome_mo,
		m.nome AS nome_ma
	FROM modelo_carro mc
	LEFT JOIN marca m
		ON m.id = mc.marca
)
SELECT DISTINCT 
	idCarro, 
	classiCarro,
	anoCarro,
	mm.id AS id_modelo,
	idcombustivel
FROM tb_locacao tl
LEFT JOIN referencia_m_m mm
	ON LOWER(mm.nome_ma) = LOWER(tl.marcaCarro)
	AND LOWER(mm.nome_mo) = LOWER(tl.modeloCarro);

--Adicionar vendedor
INSERT INTO vendedor (id, nome, sexo, estado)
SELECT DISTINCT
	idVendedor, 
	nomeVendedor, 
	sexoVendedor, 
	e.id
FROM tb_locacao tl
LEFT JOIN estado e
	ON LOWER(tl.estadoVendedor) = LOWER(e.nome);

--Adicionar registros de locacao
INSERT INTO locacao (
	id, cliente, carro, kilometragem_entrada, quantidade_diarias, valor_diaria, vendedor, 
	data_retirada, hora_retirada, data_entrega, hora_entrega)
SELECT 
	idLocacao, idCliente, idCarro, kmCarro, qtdDiaria, vlrDiaria, idVendedor,
	--Corrige o formato da data para o sqlite
	DATE(
		CONCAT(
			SUBSTR(dataLocacao, 1, 4), '-', 
			SUBSTR(dataLocacao, 5, 2), '-', 
			SUBSTR(dataLocacao, 7)))
	AS data_retirada,
	--Corrige o formato da hora para o sqlite
	CASE 
		WHEN horaLocacao LIKE '_:__' THEN TIME(CONCAT('0', horaLocacao))
		ELSE TIME(horaLocacao)
	END AS hora_retirada,
	--Corrige o formato da data para o sqlite
	DATE(
		CONCAT(
			SUBSTR(dataEntrega , 1, 4), '-', 
			SUBSTR(dataEntrega, 5, 2), '-', 
			SUBSTR(dataEntrega, 7)))
	AS data_entrega,
	--Corrige o formato da hora para o sqlite
	CASE 
		WHEN horaEntrega LIKE '_:__' THEN TIME(CONCAT('0', horaEntrega))
		ELSE TIME(horaEntrega)
	END AS hora_entrega
FROM tb_locacao tl;

--Validando transferencia de dados_____________________________________
--Criando table inicial a partir do modelo novo
WITH vendedor_estado AS (
	SELECT 
		v.id,
		v.nome,
		v.sexo,
		e.nome AS estado
	FROM vendedor v
	LEFT JOIN estado e
		ON e.id = v.estado
), full_query AS (
	SELECT 
		l.id AS idLocacao,
		c.id AS idCliente,
		c.nome AS nomeCliente,
		c2.nome AS cidadeCliente,
		e.nome AS estadoCliente,
		p.nome AS paisCliente,
		l.carro AS idCarro,
		l.kilometragem_entrada AS kmCarro,
		c3.chaci AS classiCarro,
		m.nome AS marcaCarro,
		mc.nome AS modeloCarro,
		c3.ano AS anoCarro,
		c3.combustivel AS idCombustivel,
		c4.tipo AS tipoCombustivel,
		l.data_retirada AS dataLocacao,
		l.hora_retirada AS horaLocacao,
		l.quantidade_diarias AS qtdDiaria,
		l.valor_diaria AS vlrDiaria,
		l.data_entrega AS dataEntrega,
		l.hora_entrega AS horaEntrega,
		l.vendedor AS idVendedor,
		ve.nome AS nomeVendedor,
		ve.sexo AS sexoVendedor,
		ve.estado AS estadoVendedor 
	FROM locacao l
	LEFT JOIN cliente c
		ON c.id = l.cliente
	LEFT JOIN cidade c2
		ON c2.id = c.cidade
	LEFT JOIN estado e
		ON c2.estado  = e.id
	LEFT JOIN pais p
		ON p.id = e.pais
	LEFT JOIN carro c3
		ON c3.id = l.carro
	LEFT JOIN modelo_carro mc
		ON c3.modelo = mc.id
	LEFT JOIN marca m
		ON mc.marca = m.id
	LEFT JOIN combustivel c4
		ON c4.id = c3.combustivel
	LEFT JOIN vendedor_estado ve
		ON ve.id = l.vendedor
), compare_query AS (
	SELECT
		idLocacao, idCliente, nomeCliente, cidadeCliente, estadoCliente,
		paisCliente, idCarro, kmCarro, classiCarro, marcaCarro, modeloCarro,
		anoCarro, idcombustivel, tipoCombustivel,
		--Corrige o formato da data para o sqlite
		DATE(
				CONCAT(
					SUBSTR(dataLocacao, 1, 4), '-', 
					SUBSTR(dataLocacao, 5, 2), '-', 
					SUBSTR(dataLocacao, 7)))
			AS dataLocacao,
			--Corrige o formato da hora para o sqlite
		CASE 
			WHEN horaLocacao LIKE '_:__' THEN TIME(CONCAT('0', horaLocacao))
			ELSE TIME(horaLocacao)
		END AS horaLocacao,
		qtdDiaria, 
		vlrDiaria,
		--Corrige o formato da data para o sqlite
		DATE(
			CONCAT(
				SUBSTR(dataEntrega , 1, 4), '-', 
				SUBSTR(dataEntrega, 5, 2), '-', 
				SUBSTR(dataEntrega, 7)))
		AS dataEntrega,
		--Corrige o formato da hora para o sqlite
		CASE 
			WHEN horaEntrega LIKE '_:__' THEN TIME(CONCAT('0', horaEntrega))
			ELSE TIME(horaEntrega)
		END AS horaEntrega,
		idVendedor, nomeVendedor, sexoVendedor, estadoVendedor
	FROM tb_locacao tl
)
SELECT COUNT(*) = 26 AS "Dados tranferidos com sucesso?"
FROM (SELECT * FROM compare_query INTERSECT SELECT * FROM full_query);
