import sqlite3
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor


@dataclass(frozen=True)
class FilmWork:
    __slots__ = ['id', 'title', 'description', 'creation_date', 'certificate', 'file_path', 'rating', 'type',
                 'created_at', 'updated_at']

    id: uuid
    title: str
    description: str
    creation_date: str
    certificate: str
    file_path: str
    rating: float
    type: str
    created_at: datetime
    updated_at: datetime

    def get_values(self) -> tuple:
        return (self.id, self.title, self.description, self.creation_date, self.certificate, self.file_path,
                self.rating, self.type, self.creation_date, self.updated_at)

    @staticmethod
    def get_fields_name() -> str:
        return "id, title, description, creation_date, certificate, file_path, rating, type, created_at, updated_at"

    @staticmethod
    def get_args() -> str:
        return "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s"


@dataclass(frozen=True)
class Genre:
    __slots__ = ['id', 'name', 'description', 'created_at',  'updated_at']

    id: uuid
    name: str
    description: str
    created_at: datetime
    updated_at: datetime

    def get_values(self) -> tuple:
        return (self.id, self.name, self.description, self.created_at, self.updated_at)

    @staticmethod
    def get_fields_name() -> str:
        return "id, name, description, created_at, updated_at"

    @staticmethod
    def get_args() -> str:
        return "%s, %s, %s, %s, %s"


@dataclass(frozen=True)
class GenreFilmWork:
    __slots__ = ['id', 'film_work', 'genre', 'created_at']

    id: uuid
    film_work: FilmWork
    genre: Genre
    created_at: datetime

    def get_values(self) -> tuple:
        return (self.id, self.film_work, self.genre, self.created_at)

    @staticmethod
    def get_fields_name() -> str:
        return "id, film_work, genre, created_at"

    @staticmethod
    def get_args() -> str:
        return "%s, %s, %s, %s"


@dataclass(frozen=True)
class Person:
    __slots__ = ['id', 'full_name', 'birth_date', 'created_at', 'updated_at']

    id: uuid
    full_name: str
    birth_date: str
    created_at: datetime
    updated_at: datetime

    def get_values(self) -> tuple:
        return (self.id, self.full_name, self.birth_date, self.created_at, self.updated_at)

    @staticmethod
    def get_fields_name() -> str:
        return "id, full_name, birth_date, created_at, updated_at"

    @staticmethod
    def get_args() -> str:
        return "%s, %s, %s, %s, %s"


@dataclass(frozen=True)
class PersonFilmWork:
    __slots__ = ['id', 'film_work', 'person', 'role', 'created_at']

    id: uuid
    film_work: FilmWork
    person: Person
    role: str
    created_at: datetime

    def get_values(self) -> tuple:
        return (self.id, self.film_work, self.person, self.role, self.created_at)

    @staticmethod
    def get_fields_name() -> str:
        return "id, film_work, person, role, created_at"

    @staticmethod
    def get_args() -> str:
        return "%s, %s, %s, %s, %s"


class PostgresSaver:
    def __init__(self, connection: _connection):
        self.__connection = connection

    def save_all_data(self, data: dict):
        batch: int = 100
        cursor: DictCursor = self.__connection.cursor()

        for table_name in data:
            values = []
            i = 1

            inserted = 0
            n = len(data[table_name])

            for entry in data[table_name]:
                values.append(entry.get_values())
                inserted += 1
                if i is batch or inserted == n:
                    fields: str = entry.get_fields_name()
                    args: str = entry.get_args()
                    self.__multi_inserting(cursor, table_name, fields, values, args)
                    i = 0
                    values = []
                i += 1

    def __multi_inserting(self, cursor: DictCursor, table_name: str, fields_name: str, data: list, args: str):
        values = ','.join(cursor.mogrify(f"({args})", item).decode() for item in data)
        query = f"""INSERT INTO content.{table_name} ({fields_name}) VALUES {values} ON CONFLICT (id) DO NOTHING;"""

        try:
            cursor.execute(query)
            self.__connection.commit()
        except Exception as e:
            print(f"Ошибка записи в PostgreSQL:  {e}")


class SQLiteLoader:
    def __init__(self, connection: sqlite3.Connection):
        self.__connection = connection

    def load_movies(self) -> dict:
        executor = self.__connection.cursor()

        try:
            data = {
                'film_work': self.__get_data_from_table(executor, 'film_work', FilmWork),
                'genre': self.__get_data_from_table(executor, 'genre', Genre),
                'genre_film_work': self.__get_data_from_table(executor, 'genre_film_work', GenreFilmWork),
                'person': self.__get_data_from_table(executor, 'person', Person),
                'person_film_work': self.__get_data_from_table(executor, 'person_film_work', PersonFilmWork)
            }

            return data
        except Exception as e:
            sys.exit(f"Ошибка чтения из БД Sqlite3: {e}")



    @staticmethod
    def __get_data_from_table(executor: sqlite3.Cursor, table_name: str, data_class: dataclass) -> list:
        data = []
        for row in executor.execute(f"SELECT * FROM {table_name}"):
            data.append(data_class(*row))

        return data


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)

    data: dict = sqlite_loader.load_movies()
    postgres_saver.save_all_data(data)


if __name__ == '__main__':
    dsl = {
        'dbname': 'movies_database',
        'user': 'postgres',
        'password': 1234,
        'host': 'localhost',
        'port': 5432
    }

    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
