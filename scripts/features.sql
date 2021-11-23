CREATE OR REPLACE TABLE `twitter-project-328515.wiki.feature_19` AS(
    WITH a AS(
        SELECT 
            DISTINCT State,
            COUNT(*) AS Num_of_Shooting
        FROM `twitter-project-328515.wiki.wiki_19`
        GROUP BY State
    ),

    b AS(
        SELECT
            *,
        RANK() OVER (ORDER BY Death DESC) Death_rank,
        RANK() OVER (ORDER BY Injury DESC) Injury_rank,
        RANK() OVER (PARTITION BY State ORDER BY Death DESC) State_Death_rank,
        RANK() OVER (PARTITION BY State ORDER BY Injury DESC) State_Injury_rank
    FROM 
        `twitter-project-328515.wiki.wiki_19`
    ),
    c AS (
        SELECT
            a.State,
            a.Num_of_Shooting,
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
        LEFT JOIN a
        ON
            a.State  = b.State
        GROUP BY 1,2
    )
    SELECT
        *
    FROM c
    ORDER BY
        Num_of_Shooting
);

-- 2000 Table
DROP TABLE IF EXISTS `twitter-project-328515.wiki.feature_20`;
CREATE TABLE `twitter-project-328515.wiki.feature_20` AS(
    WITH a AS(
        SELECT 
            DISTINCT State,
            COUNT(*) AS Num_of_Shooting
        FROM `twitter-project-328515.wiki.wiki_20`
        GROUP BY State
    ),

    b AS(
        SELECT
            *,
        RANK() OVER (ORDER BY Death DESC) Death_rank,
        RANK() OVER (ORDER BY Injury DESC) Injury_rank,
        RANK() OVER (PARTITION BY State ORDER BY Death DESC) State_Death_rank,
        RANK() OVER (PARTITION BY State ORDER BY Injury DESC) State_Injury_rank
    FROM 
        `twitter-project-328515.wiki.wiki_20`
    ),
    c AS (
        SELECT
            a.State,
            a.Num_of_Shooting,
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
        LEFT JOIN a
        ON
            a.State  = b.State
        GROUP BY 1,2
    )
    SELECT
        *
    FROM c
    ORDER BY
        Num_of_Shooting
)