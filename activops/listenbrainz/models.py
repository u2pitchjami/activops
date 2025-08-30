from __future__ import annotations

from typing import Any, TypedDict


# --- RAW ListenBrainz (déjà proposé) -----------------------------------------
class Mbids(TypedDict, total=False):
    artist_mbids: list[str]
    release_mbid: str | None
    recording_mbid: str | None


class AdditionalInfo(TypedDict, total=False):
    music_service_name: str | None
    submission_client: str | None
    submission_client_version: str | None
    duration_ms: int | None
    recording_msid: str | None


class TrackMetadata(TypedDict, total=False):
    track_name: str | None
    artist_name: str | None
    release_name: str | None
    mbid_mapping: Mbids | None | None
    recording_msid: str | None
    additional_info: AdditionalInfo


class ListenEntry(TypedDict, total=False):
    inserted_at: float
    listened_at: int
    track_metadata: TrackMetadata


class Payload(TypedDict, total=False):
    listens: list[ListenEntry]


class PayloadEnvelope(TypedDict, total=False):
    payload: Payload


# --- Ligne DB (listenbrainz_tracks) ------------------------------------------
class ScrobbleRow(TypedDict, total=False):
    track_id: int
    recording_msid: str | None
    artist: str | None
    artist_mbid: str | None
    title: str | None
    album: str | None
    album_mbid: str | None
    track_mbid: str | None
    service: str | None
    client: str | None
    played_at: str | None  # TIMESTAMP -> str/iso ou datetime selon driver
    last_updated: str | None  # idem
    theme: str | None
    listened_at: int | None
    scrobble_type: str | None
    _normalized: str | None


# --- Configs JSON ------------------------------------------------------------
class PodcastRule(TypedDict, total=False):
    theme: str | None
    switch_title_artist: bool
    force_album: bool
    set_album_if_missing: bool
    album: str | None
    service: str | None
    _original_title: str | None


PodcastConfig = dict[str, PodcastRule]  # clé=title lower()


class VideoRule(TypedDict, total=False):
    service: str | None
    artist: list[str]  # liste d’artistes à matcher
    _original_artist: str | None


VideoConfig = dict[str, VideoRule]  # clé=artist lower()


class VideoArtistIndexEntry(TypedDict, total=False):
    service: str | None
    theme: str | None
    scrobble_type: str
    _original_artist: str | None


VideoArtistIndex = dict[str, VideoArtistIndexEntry]
JsonObj = dict[str, Any]
