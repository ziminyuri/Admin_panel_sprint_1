import uuid

from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _


class TimeStampedModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Genre(TimeStampedModel):
    """ Жанр """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    name = models.CharField(_('title'), max_length=255)
    description = models.TextField(_('description'), blank=True)

    def __str__(self):
        return f'{self.name}'

    class Meta:
        verbose_name = _('genre')
        verbose_name_plural = _('genres')
        db_table = "content\".\"genre"


class FilmWorkGenre(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    filmwork = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    genre = models.ForeignKey('Genre', on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        indexes = [
            models.Index(fields=['filmwork', 'genre']),
        ]


class FilmWork(TimeStampedModel):
    """ Фильм """

    class FilmWorkType(models.TextChoices):
        MOVIE = 'movie', _('movie')
        TV_SHOW = 'tv_show', _('TV Show')

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    title = models.CharField(_('title'), max_length=255)
    description = models.TextField(_('description'), blank=True)
    creation_date = models.DateField(_('creation date'), blank=True)
    certificate = models.TextField(_('certificate'), blank=True)
    file_path = models.FileField(_('file'), upload_to='film_works/', blank=True)

    rating = models.FloatField(
        _('rating'),
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        blank=True
    )

    type = models.CharField(
        _('type'),
        max_length=20,
        choices=FilmWorkType.choices,
        default=FilmWorkType.MOVIE
    )

    genres = models.ManyToManyField(Genre, through='FilmworkGenre')

    def __str__(self):
        return f'{self.title}'

    class Meta:
        verbose_name = _('filmwork')
        verbose_name_plural = _('filmworks')
        db_table = "content\".\"film_work"


class Person(TimeStampedModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    full_name = models.CharField(_('full_name'), max_length=255)
    birth_date = models.DateField(_('birth date'), blank=True)

    def __str__(self):
        return f'{self.full_name}'

    class Meta:
        verbose_name = _('person')
        verbose_name_plural = _('persons')
        db_table = 'content\".\"person'


class FilmWorkPerson(models.Model):
    class Role(models.TextChoices):
        DIRECTOR = 'director', _('Director')
        WRITER = 'writer', _('Writer')
        ACTOR = 'actor', _('Actor')

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    filmwork = models.ForeignKey(FilmWork, on_delete=models.CASCADE)
    person = models.ForeignKey(Person, on_delete=models.CASCADE)
    role = models.CharField(
        _('role'),
        choices=Role.choices,
        default=Role.ACTOR,
        max_length=255
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content\".\"person_film_work'
        indexes = [
            models.Index(fields=['filmwork', 'person', 'role']),
        ]
