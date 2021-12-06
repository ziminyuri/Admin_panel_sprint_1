import sqlite3
import sys
from dataclasses import dataclass

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor, execute_batch

from models import FilmWork, Genre, GenreFilmWork, Person, PersonFilmWork
from config import logging, env


class PostgresSaver:
    def __init__(self, connection: _connection):
        self.__connection = connection

    def save_all_data(self, table, model, data: list):
        cursor: DictCursor = self.__connection.cursor()

        values = []
        for entry in data:
            values.append(entry.get_values())

        fields: str = model.get_fields_name()
        args: str = model.get_args()

        command = f"""INSERT INTO content.{table} ({fields}) VALUES ({args}) ON CONFLICT (id) DO NOTHING;"""
        try:
            execute_batch(cursor, command, values)
            self.__connection.commit()
        except Exception as e:
            logging.exception(f"Ошибка записи в PostgreSQL: {e}")


class SQLiteLoader:
    def __init__(self, connection: sqlite3.Connection):
        self.__connection = connection
        self.__cursor = self.__connection.cursor()

    def getCursor(self):
        return self.__cursor

    def setCursor(self, value):
        self.__cursor = value

    def get_count(self, table) -> int:
        cursor = self.getCursor()
        return cursor.execute(f"SELECT Count() FROM {table}").fetchone()[0]

    def load_data(self, table, model, batch) -> list:
        executor = self.getCursor()

        try:
            data = []
            for row in executor.execute(f"SELECT * FROM {table}").fetchmany(batch):
                data.append(model(*row))

            self.setCursor(executor)
            return data

        except Exception as e:
            logging.exception(f"Ошибка чтения из БД Sqlite3:: {e}")
            self.__connection.close()
            sys.exit()


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)

    batch = 100
    tables_models = [
        ['film_work', FilmWork],
        ['genre', Genre],
        ['genre_film_work', GenreFilmWork],
        ['person', Person],
        ['person_film_work', PersonFilmWork]
    ]

    for table in tables_models:
        count = sqlite_loader.get_count(table[0])
        i = 0
        while i < count:
            data = sqlite_loader.load_data(table[0], table[1], batch)
            postgres_saver.save_all_data(table[0], table[1], data)
            i += batch

    # data: dict = sqlite_loader.load_movies()
    # postgres_saver.save_all_data(data)


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

    sqlite_conn.close()
