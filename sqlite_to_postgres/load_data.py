import sqlite3
import sys
from dataclasses import dataclass

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from models import FilmWork, Genre, GenreFilmWork, Person, PersonFilmWork
from config import logging, env


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
            logging.exception(f"Ошибка записи в PostgreSQL: {e}")


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
            logging.exception(f"Ошибка чтения из БД Sqlite3:: {e}")
            sys.exit()

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
            'dbname': env('DB_NAME'),
            'user': env('DB_USER'),
            'password': env('DB_PASSWORD'),
            'host': env('DB_HOST'),
            'port': env('DB_PORT')
        }

    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
