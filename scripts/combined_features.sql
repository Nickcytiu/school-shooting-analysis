CREATE OR REPLACE TABLE `twitter-project-328515.wiki.features_combined` AS(
    WITH X AS(
        SELECT
            Trim(Replace(State, 'DC', 'D.C.')) AS State,
            COUNT(*) AS Num_of_Shooting
        FROM `twitter-project-328515.wiki.wiki_19`
        GROUP BY 1
        UNION ALL
        SELECT
            Trim(Replace(State, 'DC', 'D.C.')) AS State,
            COUNT(*) AS Num_of_Shooting
        FROM `twitter-project-328515.wiki.wiki_20`
        GROUP BY 1
    ),

    a AS(
        SELECT 
            State,
            SUM(X.Num_of_Shooting) AS Num_of_Shooting
        FROM X
        GROUP BY 1),

    b AS(
        SELECT
            Trim(Replace(State, 'DC', 'D.C.')) AS State,
            Date,
            City,
            School_Name,
            Institution_Type,
            Injury,
            Death,
            RANK() OVER (ORDER BY Death DESC) Death_rank,
            RANK() OVER (ORDER BY Injury DESC) Injury_rank,
            RANK() OVER (PARTITION BY State ORDER BY Death DESC) State_Death_rank,
            RANK() OVER (PARTITION BY State ORDER BY Injury DESC) State_Injury_rank
        FROM `twitter-project-328515.wiki.wiki_19`
        UNION ALL
        SELECT
            Trim(Replace(State, 'DC', 'D.C.')) AS State,
            Date,
            City,
            School_Name,
            Institution_Type,
            Injury,
            Death,
            RANK() OVER (ORDER BY Death DESC) Death_rank,
            RANK() OVER (ORDER BY Injury DESC) Injury_rank,
            RANK() OVER (PARTITION BY State ORDER BY Death DESC) State_Death_rank,
            RANK() OVER (PARTITION BY State ORDER BY Injury DESC) State_Injury_rank
        FROM `twitter-project-328515.wiki.wiki_20`
    ),
    c AS (
        SELECT
            a.State,
            a.Num_of_Shooting,
            Y.Year_2018 AS Household_Median_Income,
            Z.Pop2018 AS Population,
            a.Num_of_Shooting/Z.Pop2018 AS Num_of_Shooting_per_Capita,
            ARRAY_AGG(STRUCT(
                b.Date,
                b.City,
                b.School_Name,
                b.Institution_Type,
                b.Injury,
                b.State_Injury_rank,
                b.Injury_rank,
                b.Death,
                b.State_Death_rank,
                b.Death_rank)ORDER BY b.Date ASC) AS event
        FROM b
        JOIN a
        ON
            a.State  = b.State
        JOIN `twitter-project-328515.wiki.Household_income_state` Y
        ON
            Y.State = b.State
        JOIN `twitter-project-328515.wiki.Population_state` Z
        ON
            Z.State = b.State
        GROUP BY 1,2,3,4
    )
    SELECT
        *
    FROM c 
    ORDER BY
        c.Num_of_Shooting 
)
