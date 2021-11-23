CREATE OR REPLACE TABLE `twitter-project-328515.wiki.long_term_results` AS(
    WITH x AS(
        SELECT
            EXTRACT(year FROM Date) AS Year,
            SUM(Injury) AS Total_Injury,
            SUM(Death) AS Total_Death,
            COUNT(Date) AS Num_of_Shooting
        FROM `twitter-project-328515.wiki.wiki_19`
        GROUP BY EXTRACT(year FROM Date)
    ),

    y AS(
        SELECT
            EXTRACT(year FROM Date) AS Year,
            SUM(Injury) AS Total_Injury,
            SUM(Death) AS Total_Death,
            COUNT(Date) AS Num_of_Shooting
        FROM `twitter-project-328515.wiki.wiki_20`
        GROUP BY EXTRACT(year FROM Date)
    ),

    z AS(
        SELECT 
            * 
        FROM x
        UNION ALL
        SELECT 
            * 
        FROM y
        ORDER BY Year
    ),

    a AS (
        SELECT
            FLOOR(Year / 10) * 10 as Decade,
            * 
        FROM z
    ),

    b AS (
    SELECT
        a.Decade,
        SUM(a.Num_of_Shooting) AS Num_of_Shooting_per_Decade
    FROM a
    GROUP BY 1
    )

    SELECT
        b.Decade,
        b.Num_of_Shooting_per_Decade,
        ARRAY_AGG(STRUCT(
                a.Year,
                a.Num_of_Shooting,
                a.Total_Injury,
                a.Total_Death
                )ORDER BY a.Year)AS Yearly
    FROM b
    INNER JOIN a
    ON
        b.Decade = a.Decade
    GROUP BY 1,2
    ORDER BY b.Decade
)



