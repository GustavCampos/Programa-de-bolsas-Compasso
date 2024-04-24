## Basic Commands
DISTINCT: Used to return only distinct (different) values. If passed more than one column, every distinct combination is returned
```SQL
SELECT DISTINCT * FROM table_1;
SELECT DISTINCT column_a, column_b FROM table_2;
```

## Operators
||: Used to concatenate multiple columns in a single one
```SQL
SELECT first_name || second_name as complete_name 
FROM table_1;
```

ILIKE: Same as LIKE but ignoring case
```SQL
--This one
SELECT first_name 
FROM table_1;
WHERE first_name ILIKE 'ana%'

--Is same as
SELECT first_name 
FROM table_1;
WHERE 
    first_name LIKE 'ANA%' or
    first_name LIKE 'ana%';
```

## Aggregators
HAVING: Used as a WHERE clause for queries with aggregate functions
```SQL
-- Get total price for every category, filter total prices <= 100
SELECT 
    category, 
    SUM(price) AS "Total Price"
FROM products
GROUP BY category
HAVING SUM(price) > 100;
```

## Subqueries
WITH: Defines a temporary dataset, and the output of this dataset is available in several places within the main SQL Query.
```SQL
--Show product only if price is less than brand average price
WITH avg_per_brand AS (
    SELECT
        brand, 
        avg(price) AS avg_price  
    FROM products
    GROUP BY brand
)
SELECT * FROM products p
LEFT JOIN avg_per_brand apb USING(brand)
WHERE p.price < apb.avg_price;
```

## Data Processing
Conversion operator (**::**): Used to specify a data type.
```SQL
--Error, no explicit conversion
SELECT '2021-10-01' - '2021-02-01'; 

--Correct example
SELECT '2021-10-01'::date - '2021-02-01'::date; 
```

CAST: Converts a value (of any type) into a specified datatype.
```SQL
SELECT CAST(25.65 AS int); 
SELECT CAST('2017-08-25' AS datetime); 
```

COALESCE: Return fisrt non-null value from a list
```SQL
SELECT
    name,
    COALESCE(ID, 'Not legally registered'),
    birth_date
FROM customers;
```

#### String Manipulation
```SQL
--Upper case a string
SELECT UPPER('São Paulo') = 'SÃO PAULO';

--lower case a string
SELECT LOWER('São Paulo') = 'são paulo';

--Trim a string
SELECT TRIM('São Paulo        ') = 'São Paulo';

--Replace any part of a string
SELECT REPLACE('SAO PAULO', 'SAO', 'SÃO') = 'SÃO PAULO';
```

#### Date Manipulation
INTERVAL: Used to set intervals to add in a date
```SQL
SELECT (current_date + INTERVAL '10 weeks')::date;
SELECT (current_date + INTERVAL '10 months')::date;

--Turn into a datetime without date cast
SELECT (current_date + INTERVAL '10 hours');
```
DATE_TRUNC: Used to truncate datetime
 ```SQL
SELECT DATE_TRUNC('month', current_date)::date;
```

EXTRACT: Extract a part of a date.
```SQL
SELECT EXTRACT(MONTH FROM "2017-06-15");
```
DATEDIFF: Return the number of days between two date values.
```SQL
SELECT DATEDIFF("2017-06-25", "2017-06-15"); 
```