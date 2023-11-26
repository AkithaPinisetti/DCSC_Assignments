1. SELECT
    animal_type,
    COUNT(DISTINCT animal_id) AS number_of_animals
FROM
    dim_animal
GROUP BY
    animal_type

2. SELECT
    COUNT(*) AS number_of_animals_with_more_than_one_outcome
FROM (
    SELECT
        animal_id,
        COUNT(*) AS number_of_outcomes
    FROM
        fact_animal_outcomes
    GROUP BY
        animal_id
    HAVING
        COUNT(*) > 1
) AS animals_with_multiple_outcomes;

3. SELECT
    TO_CHAR(datetime, 'Month') AS calendar_month,
    COUNT(*) AS outcome_count
FROM
    dim_time
GROUP BY
    calendar_month
ORDER BY
    outcome_count DESC
LIMIT 5;

4.1. WITH age_categories AS (
    SELECT
        CASE
            WHEN animal_type = 'Cat' AND age_in_years < 1 THEN 'Kitten'
            WHEN animal_type = 'Cat' AND age_in_years >= 1 AND age_in_years <= 10 THEN 'Adult'
            WHEN animal_type = 'Cat' AND age_in_years > 10 THEN 'Senior'
        END AS category,
        outcome_type
    FROM dim_animal
    WHERE animal_type = 'Cat'
)

SELECT
    category,
    (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM age_categories WHERE outcome_type = 'Adopted')) AS percentage
FROM age_categories
WHERE outcome_type = 'Adopted'
GROUP BY category;

4.2. WITH CatAges AS (
    SELECT
        animal_id,
        CASE
            WHEN EXTRACT(YEAR FROM AGE(date_of_birth, CURRENT_DATE)) < 1 THEN 'Kitten'
            WHEN EXTRACT(YEAR FROM AGE(date_of_birth, CURRENT_DATE)) >= 1
                 AND EXTRACT(YEAR FROM AGE(date_of_birth, CURRENT_DATE)) <= 10 THEN 'Adult'
            WHEN EXTRACT(YEAR FROM AGE(date_of_birth, CURRENT_DATE)) > 10 THEN 'Senior'
            ELSE 'Unknown' -- Handle cases with missing or invalid data
        END AS age_category
    FROM
        dim_animal
    WHERE
        animal_type = 'Cat'
)

SELECT
    age_category,
    COUNT(*) AS count,
    (COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()) AS percentage
FROM
    CatAges
WHERE
    animal_id IN (
        SELECT
            animal_id
        FROM
            fact_animal_outcomes
        WHERE
            outcome_type = 'Adopted'
    )
GROUP BY
    age_category
ORDER BY
    age_category;

5.SELECT
    datetime,
    outcome_type,
    COUNT(*) OVER (PARTITION BY outcome_type ORDER BY datetime) AS cumulative_total
FROM
    dim_animal
ORDER BY
    datetime;
