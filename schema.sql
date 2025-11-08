DROP TABLE IF EXISTS rating CASCADE;
DROP TABLE IF EXISTS movie_genre CASCADE;
DROP TABLE IF EXISTS genre CASCADE;
DROP TABLE IF EXISTS movie CASCADE;
DROP TABLE IF EXISTS user_t CASCADE;

-- USER TABLE
CREATE TABLE user_t (
    user_id     SERIAL PRIMARY KEY,
    username    VARCHAR(200)
);

-- MoVIE TABLE

CREATE TABLE movie (
    movie_id     INTEGER PRIMARY KEY,
    title        VARCHAR(300) NOT NULL,
    release_year INT,
    director     VARCHAR(300),
    plot         TEXT,
    box_office   VARCHAR(50),
    decade       VARCHAR(10)
);

-- GENRE TABLE

CREATE TABLE genre (
    genre_id     SERIAL PRIMARY KEY,
    genre_name   VARCHAR(100) UNIQUE NOT NULL
);


-- MOVIE_GENRE TABLE (FOR M:N relation)
CREATE TABLE movie_genre (
    movie_id  INTEGER NOT NULL,
    genre_id  INTEGER NOT NULL,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genre(genre_id) ON DELETE CASCADE
);


-- RATING TABLE
CREATE TABLE rating (
    rating_id   SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL,
    movie_id    INTEGER NOT NULL,
    rating      DECIMAL(3,1) CHECK (rating >= 0 AND rating <= 5),
    rating_ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (user_id)  REFERENCES user_t(user_id)  ON DELETE CASCADE,
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX idx_rating_user ON rating(user_id);
CREATE INDEX idx_rating_movie ON rating(movie_id);
CREATE INDEX idx_moviegenre_movie ON movie_genre(movie_id);
CREATE INDEX idx_moviegenre_genre ON movie_genre(genre_id);

ALTER TABLE rating
ADD CONSTRAINT rating_unique UNIQUE (user_id, movie_id, rating_ts);