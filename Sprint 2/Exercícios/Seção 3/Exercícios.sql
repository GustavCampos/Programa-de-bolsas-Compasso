--Ex 1
--Todos os livros publicados após 2014
SELECT * FROM livro
WHERE publicacao >= '2015-01-01' 
ORDER BY cod;

--Ex 2
--Os 10 livros mais caros
SELECT titulo, valor FROM livro
ORDER BY valor DESC
LIMIT 10;

--Ex 3
--As 5 editoras com mais livros na biblioteca
--quantidade, nome, estado e cidade
SELECT 
	count(*) AS quantidade,
	e.nome,
	en.estado,
	en.cidade 
FROM livro l 
LEFT JOIN editora e
	ON e.codeditora = l.editora
LEFT JOIN endereco en
	ON en.codendereco = e.endereco 
GROUP BY e.codeditora
ORDER BY quantidade DESC
LIMIT 5;

--Ex 4
--A quantidade de livros publicada por cada 'autor
--codautor, nascimento e quantidade
SELECT 
	a.nome,
	a.codautor,
	a.nascimento,
	count(l.cod) AS quantidade
FROM livro l
RIGHT JOIN autor a
	ON a.codautor = l.autor
GROUP BY codautor
ORDER BY a.nome;

--Ex 5
--O nome dos autores que publicaram livros através de editoras NÃO situadas na região sul do Brasil
SELECT DISTINCT a.nome FROM livro l 
LEFT JOIN autor a 
	ON a.codautor = l.autor 
LEFT JOIN editora e 
	ON e.codeditora = l.editora
LEFT JOIN endereco e2
	ON e2.codendereco = e.endereco
WHERE e2.estado NOT IN ('RIO GRANDE DO SUL', 'SANTA CATARINA', 'PARANÁ')
ORDER BY a.nome;


--Ex 6
--O autor com maior número de livros publicados
SELECT 
	a.codautor,
	a.nome,
	count(l.cod)AS quantidade_publicacoes
FROM livro l
LEFT JOIN autor a
	ON a.codautor = l.autor
GROUP BY autor
ORDER BY quantidade_publicacoes DESC
LIMIT 1;

--Ex 7
--O nome dos autores com nenhuma publicação
SELECT 
	a.nome
FROM livro l
RIGHT JOIN autor a
	ON a.codautor = l.autor
GROUP BY codautor
HAVING count(l.cod) = 0
ORDER BY a.nome;
