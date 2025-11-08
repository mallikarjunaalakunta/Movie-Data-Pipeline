
-- 1)Which  Movie has Highest Rating
SELECT 
    m.title,
    ROUND(AVG(r.rating), 2) AS avg_rating
FROM movie m
JOIN rating r ON m.movie_id = r.movie_id
GROUP BY m.title
ORDER BY avg_rating DESC
LIMIT 1;

-- 2)Top 5 movie genres with the highest average rating

SELECT
    g.genre_name,
    ROUND(AVG(r.rating), 2) AS avg_rating
FROM rating r
JOIN movie_genre mg ON r.movie_id = mg.movie_id
JOIN genre g ON mg.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY avg_rating DESC
LIMIT 5;

--3)Who is the director with the most movies?

SELECT 
    director,
    COUNT(*) AS total_movies
FROM movie
WHERE director IS NOT NULL AND director <> ''
GROUP BY director
ORDER BY total_movies DESC
LIMIT 1;

-- 4)Average rating of movies released each year

SELECT
    m.release_year,
    ROUND(AVG(r.rating), 2) AS avg_rating
FROM movie m
JOIN rating r ON m.movie_id = r.movie_id
WHERE m.release_year IS NOT NULL
GROUP BY m.release_year
ORDER BY m.release_year;

