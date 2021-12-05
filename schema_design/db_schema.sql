-- Создание отдельной схемы для контента:
CREATE SCHEMA IF NOT EXISTS content;

-- Фильмы
CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    certificate TEXT,
    file_path TEXT,
    rating FLOAT,
    type TEXT not null,
    created_at timestamp,
    updated_at timestamp
);

-- Жанры кинопроизведений: 
CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created_at timestamp,
    updated_at timestamp
);

-- Разбиение связи много-ко-многим (Жанры и Фильмы):
CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid PRIMARY KEY,
    filmwork_id uuid NOT NULL REFERENCES content.film_work(id) ON DELETE CASCADE,
    genre_id uuid NOT NULL REFERENCES content.genre(id) ON DELETE CASCADE,
    created_at timestamp
);

-- Актеры:
CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY,
    full_name TEXT NOT NULL,
    birth_date DATE,
    created_at timestamp,
    updated_at timestamp
);

-- Разбиение связи много-ко-многим (Актеры и Фильмы):
CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid PRIMARY KEY,
    filmwork_id uuid NOT NULL REFERENCES content.film_work(id) ON DELETE CASCADE,
    person_id uuid NOT NULL REFERENCES content.person(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    created_at timestamp
);

CREATE UNIQUE INDEX IF NOT EXISTS film_work_genre ON content.genre_film_work (filmwork_id, genre_id);
CREATE UNIQUE INDEX IF NOT EXISTS film_work_person_role ON content.person_film_work (filmwork_id, person_id, role);