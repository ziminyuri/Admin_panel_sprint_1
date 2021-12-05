from django.contrib import admin

from movies.models import (FilmWork, FilmWorkGenre, FilmWorkPerson, Genre,
                           Person)


class FilmWorkPersonInline(admin.TabularInline):
    model = FilmWorkPerson
    extra = 0


class FilmworkGenreInline(admin.TabularInline):
    model = FilmWorkGenre
    extra = 0


@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    list_display = ('title', 'creation_date', 'rating', 'created_at', 'updated_at', 'type')
    list_filter = ('type',)
    search_fields = ('title', 'description', 'id')
    fields = ('title', 'type', 'description', 'creation_date', 'certificate', 'file_path', 'rating')

    inlines = [
        FilmWorkPersonInline,
        FilmworkGenreInline,
    ]


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name', 'description', 'created_at', 'updated_at')
    search_fields = ('id', 'name', 'description')
    fields = ('name', 'description')


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name', 'birth_date', 'created_at', 'updated_at')
    search_fields = ('id', 'full_name', 'birth_date')
    fields = ('full_name', 'birth_date')
