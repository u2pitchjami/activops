from __future__ import annotations

from typing import Any, TypedDict

# --- Trakt raw payloads (subset utile) ---------------------------------------


class Ids(TypedDict, total=False):
    trakt: int | None
    imdb: str | None
    tmdb: int | None
    tvdb: int | None


class Movie(TypedDict, total=False):
    title: str | None
    year: int | None
    ids: Ids


class Show(TypedDict, total=False):
    title: str | None
    year: int | None
    ids: Ids


class Episode(TypedDict, total=False):
    season: int | None
    number: int | None
    title: str | None
    ids: Ids


class HistoryMovieEntry(TypedDict, total=False):
    watched_at: str | None
    movie: Movie
    rating: int | None


class HistoryShowEntry(TypedDict, total=False):
    watched_at: str | None
    show: Show
    episode: Episode
    rating: int | None


class RatingMovieEntry(TypedDict, total=False):
    rated_at: str | None
    rating: int | None
    movie: Movie


class RatingEpisodeEntry(TypedDict, total=False):
    rated_at: str | None
    rating: int | None
    show: Show
    episode: Episode


class WatchlistItem(TypedDict, total=False):
    type: str  # "movie" | "show"
    listed_at: str | None
    movie: Movie
    show: Show


JsonObj = dict[str, Any]
