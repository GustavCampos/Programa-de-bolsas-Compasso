-- Getting the 3 most used names for every decade
WITH ranked_names AS (
    SELECT nome,
        ROUND(ano / 10) AS decada,
        SUM(total) AS total,
        ROW_NUMBER() OVER (
            PARTITION BY ROUND(ano / 10)
            ORDER BY SUM(total) DESC
        ) AS row_num
    FROM meubanco.nomes_csv
    WHERE ano >= 1950
    GROUP BY ROUND(ano / 10), nome
)
SELECT nome, total, decada
FROM ranked_names
WHERE row_num IN (1, 2, 3)
ORDER BY decada, total DESC;
