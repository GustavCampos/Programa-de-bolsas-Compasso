-- Query em uma única linha para facilitar o copiar e colar
SELECT 'Canais de TV credenciados no Brasil de conteúdo geral ou ofertados a partir de 2010' AS SOBRE, COUNT(*) AS TOTAL_DE_CANAIS, SUM( CASE (LOWER(DENSIDADE_CANAL) = 'alta definição') WHEN TRUE THEN 1 ELSE 0 END ) AS CANAIS_EM_HD, SUM( CASE (LOWER(PAIS_PROGRAMADORA) != 'brasil') WHEN TRUE THEN 1 ELSE 0 END ) AS CANAIS_ESTRANGEIROS, SUM( CASE (CLASSIFICACAO_PROGRAMADORA LIKE '%art.17%') WHEN TRUE THEN 1 ELSE 0 END ) AS CANAIS_ELEGIDOS_PELO_ARTIGO_17 FROM s3object AS s WHERE CNPJ_PROGRAMADORA <> '' AND DATE_DIFF(year, CAST(DATA_INICIO_OFERTA AS TIMESTAMP), UTCNOW()) <= DATE_DIFF(year, CAST('2010-01-01' AS TIMESTAMP), UTCNOW()) OR TIPO_CONTEUDO_CANAL LIKE '%geral%'

-- Query formatada para melhor visualização (utilizada no script)
SELECT
    'Canais de TV credenciados no Brasil de conteúdo geral ou ofertados a partir de 2010' AS SOBRE,
    COUNT(*) AS TOTAL_DE_CANAIS, 
    SUM(
        CASE (LOWER(DENSIDADE_CANAL) = 'alta definição')
            WHEN TRUE THEN 1 
            ELSE 0 
        END
    ) AS CANAIS_EM_HD,
    SUM(
        CASE (LOWER(PAIS_PROGRAMADORA) != 'brasil') 
            WHEN TRUE THEN 1 
            ELSE 0 
        END
    ) AS CANAIS_ESTRANGEIROS,
    SUM(
        CASE (CLASSIFICACAO_PROGRAMADORA LIKE '%art.17%') 
            WHEN TRUE THEN 1 
            ELSE 0 END
    ) AS CANAIS_ELEGIDOS_PELO_ARTIGO_17
FROM s3object AS s 
WHERE 
    CNPJ_PROGRAMADORA <> '' AND
    DATE_DIFF(year, CAST(DATA_INICIO_OFERTA AS TIMESTAMP), UTCNOW()) 
    <= DATE_DIFF(year, CAST('2010-01-01' AS TIMESTAMP), UTCNOW()) OR
    TIPO_CONTEUDO_CANAL LIKE '%geral%'