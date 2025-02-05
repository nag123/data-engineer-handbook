SELECT * FROM public.actor_films
ORDER BY actorid ASC, filmid ASC LIMIT 10


CREATE TYPE films AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

CREATE TYPE quality_class AS
    ENUM ('star', 'good', 'average', 'bad');
CREATE TYPE is_active AS ENUM ('true', 'false');

select max(year) from actor_films; --2021

CREATE TABLE actors (
     actor TEXT,
     actorid TEXT,
     films films[],
     quality_class quality_class,
     is_active BOOLEAN,
     current_year INTEGER,
     PRIMARY KEY (actor, current_year)
);

alter table public.actors
    owner to postgres;
	
select * from actors;

--drop table actors;


INSERT INTO actors
WITH yesterday AS (
    SELECT * FROM actors
    WHERE current_year = 2009

), today AS (
     SELECT * FROM actor_films
    WHERE year = 2010
)

SELECT
    COALESCE(t.actor, y.actor) AS actor,
    COALESCE(t.actorid, y.actorid) AS actorid,
    CASE WHEN y.films IS NULL
	    THEN ARRAY[ROW(
	    t.film,
	    t.votes,
	    t.rating,
	    t.filmid
        )::films]
    WHEN t.year IS NOT NULL THEN y.films || ARRAY[ROW(
        t.film,
	    t.votes,
	    t.rating,
	    t.filmid
        )::films]
    ELSE y.films
    END as films,
    CASE
        WHEN t.year IS NOT NULL THEN
            CASE
                WHEN t.rating > 8 THEN 'star'
                WHEN t.rating > 7 AND t.rating <= 8 THEN 'good'
                WHEN t.rating > 6 AND t.rating <= 7 THEN 'average'
                ELSE 'bad'
            END::quality_class
        ELSE y.quality_class
    END as quality_class,
    CASE WHEN t.year IS NOT NULL THEN TRUE
        ELSE FALSE
    END as is_active,

    COALESCE(t.year, y.current_year + 1) as current_year

    FROM today t FULL OUTER JOIN yesterday y
        ON t.actor = y.actor
ON CONFLICT (actor, current_year) DO NOTHING;


SELECT *
FROM actors
WHERE is_active = 'false' AND current_year = 2010;

CREATE TABLE actors_history_scd (
    actorid TEXT NOT NULL,
    actor TEXT NOT NULL,
    quality_class TEXT NOT NULL,
    is_active BOOLEAN NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    PRIMARY KEY (actorid, start_date)
);

INSERT INTO actors_history_scd (
    actor,
    actorid,
    quality_class,
    is_active,
    start_date,
    end_date
)
SELECT
    actor,
    actorid,
    quality_class,
    is_active,
    '1998-01-01'::DATE AS start_date,
    '9999-12-31'::DATE AS end_date
FROM actors;

select * from actors where actorid = 'nm0123092'; 

SELECT
    actorid,
    COUNT(*)
FROM actors
GROUP BY actorid
HAVING COUNT(*) > 1;


select * from actors_history_scd;
