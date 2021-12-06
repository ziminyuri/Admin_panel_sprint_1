import uuid
from dataclasses import dataclass
from datetime import datetime


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

    @classmethod
    def get_fields_name(cls) -> str:
        return "id, title, description, creation_date, certificate, file_path, rating, type, created_at, updated_at"

    @classmethod
    def get_args(cls) -> str:
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

    @classmethod
    def get_fields_name(cls) -> str:
        return "id, name, description, created_at, updated_at"

    @classmethod
    def get_args(cls) -> str:
        return "%s, %s, %s, %s, %s"


@dataclass(frozen=True)
class GenreFilmWork:
    __slots__ = ['id', 'filmwork_id', 'genre_id', 'created_at']

    id: uuid
    filmwork_id: FilmWork
    genre_id: Genre
    created_at: datetime

    def get_values(self) -> tuple:
        return (self.id, self.filmwork_id, self.genre_id, self.created_at)

    @classmethod
    def get_fields_name(cls) -> str:
        return "id, filmwork_id, genre_id, created_at"

    @classmethod
    def get_args(cls) -> str:
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

    @classmethod
    def get_fields_name(cls) -> str:
        return "id, full_name, birth_date, created_at, updated_at"

    @classmethod
    def get_args(cls) -> str:
        return "%s, %s, %s, %s, %s"


@dataclass(frozen=True)
class PersonFilmWork:
    __slots__ = ['id', 'filmwork_id', 'person_id', 'role', 'created_at']

    id: uuid
    filmwork_id: FilmWork
    person_id: Person
    role: str
    created_at: datetime

    def get_values(self) -> tuple:
        return (self.id, self.filmwork_id, self.person_id, self.role, self.created_at)

    @classmethod
    def get_fields_name(cls) -> str:
        return "id, filmwork_id, person_id, role, created_at"

    @classmethod
    def get_args(cls) -> str:
        return "%s, %s, %s, %s, %s"