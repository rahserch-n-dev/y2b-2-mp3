import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import yt_dlp
from youtube_transcript_api import NoTranscriptFound, TranscriptsDisabled, YouTubeTranscriptApi

DATA_ROOT = Path("data")
DEFAULT_LANGUAGES = ["en", "en-US", "en-GB"]
MANIFEST_NAME = "manifest.json"


class YDLLogger:
    def debug(self, message: str) -> None:
        logging.debug(message)

    def warning(self, message: str) -> None:
        logging.warning(message)

    def error(self, message: str) -> None:
        logging.error(message)


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="YouTube ingestion utility for audio and transcript datasets."
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging output.",
    )

    subparsers = parser.add_subparsers(dest="command")

    playlist_parser = subparsers.add_parser(
        "ingest-playlist",
        help="Download audio and transcripts for every video in a playlist.",
    )
    playlist_parser.add_argument(
        "--playlist-url",
        required=True,
        help="YouTube playlist URL to ingest.",
    )
    playlist_parser.add_argument(
        "--languages",
        nargs="+",
        default=DEFAULT_LANGUAGES,
        help="Language codes to request for transcripts, ordered by priority.",
    )
    playlist_parser.add_argument(
        "--max-videos",
        type=int,
        default=None,
        help="Optional limit on the number of videos to process.",
    )
    playlist_parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Reuse existing audio files when present.",
    )
    playlist_parser.add_argument(
        "--skip-audio",
        action="store_true",
        help="Do not download audio assets.",
    )
    playlist_parser.add_argument(
        "--skip-transcripts",
        action="store_true",
        help="Do not fetch transcripts.",
    )
    playlist_parser.add_argument(
        "--output-root",
        type=Path,
        default=DATA_ROOT,
        help="Directory where playlist assets and metadata will be stored.",
    )

    video_parser = subparsers.add_parser(
        "download-video",
        help="Download a single YouTube video as an MP3 file.",
    )
    video_parser.add_argument(
        "--url",
        required=True,
        help="YouTube video URL to download.",
    )
    video_parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("mp3_downloads"),
        help="Directory for saved MP3 files.",
    )
    video_parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip download if the MP3 already exists.",
    )

    return parser


def extract_playlist_id(playlist_url: str) -> Optional[str]:
    parsed = urlparse(playlist_url)
    query = parse_qs(parsed.query)
    if "list" in query and query["list"]:
        return query["list"][0]
    if "list=" in playlist_url:
        return playlist_url.split("list=")[-1].split("&")[0]
    return None


def ensure_directories(paths: List[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Dict) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2)
        handle.write("\n")


def relative_to_base(path: Optional[Path], base: Path) -> Optional[str]:
    if path is None:
        return None
    try:
        return str(path.relative_to(base))
    except ValueError:
        return str(path.resolve())


def get_playlist_entries(playlist_url: str) -> Tuple[Dict, List[Dict]]:
    options = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": "in_playlist",
        "logger": YDLLogger(),
    }
    with yt_dlp.YoutubeDL(options) as downloader:
        info = downloader.extract_info(playlist_url, download=False)
    entries = info.get("entries") or []
    for index, entry in enumerate(entries, start=1):
        entry.setdefault("playlist_index", index)
    return info, entries


def get_video_info(video_url: str) -> Dict:
    options = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "logger": YDLLogger(),
    }
    with yt_dlp.YoutubeDL(options) as downloader:
        return downloader.extract_info(video_url, download=False)


def download_audio(video_url: str, output_dir: Path) -> Dict:
    options = {
        "format": "bestaudio/best",
        "outtmpl": str(output_dir / "%(id)s.%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "logger": YDLLogger(),
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }
        ],
    }
    with yt_dlp.YoutubeDL(options) as downloader:
        return downloader.extract_info(video_url, download=True)


def fetch_transcript(video_id: str, languages: List[str]) -> Tuple[Optional[List[Dict]], Optional[str]]:
    try:
        transcripts = YouTubeTranscriptApi.list_transcripts(video_id)
    except (TranscriptsDisabled, NoTranscriptFound) as error:
        logging.warning("Transcript listing not available for %s: %s", video_id, error)
        return None, None
    except Exception as error:
        logging.warning("Unexpected transcript listing error for %s: %s", video_id, error)
        return None, None

    ordered_languages: List[str] = []
    for code in languages:
        if code not in ordered_languages:
            ordered_languages.append(code)
    for fallback in ["en", "en-US", "en-GB"]:
        if fallback not in ordered_languages:
            ordered_languages.append(fallback)

    try:
        transcript_obj = transcripts.find_transcript(ordered_languages)
        transcript_data = transcript_obj.fetch()
        return transcript_data, transcript_obj.language_code
    except NoTranscriptFound:
        logging.debug("Manual transcript not found for %s.", video_id)

    try:
        generated_obj = transcripts.find_generated_transcript(ordered_languages)
        transcript_data = generated_obj.fetch()
        return transcript_data, generated_obj.language_code
    except NoTranscriptFound:
        logging.debug("Auto transcript not found for %s.", video_id)

    for transcript_obj in transcripts:
        if not transcript_obj.is_translatable:
            continue
        for target_language in ordered_languages:
            try:
                translated = transcript_obj.translate(target_language)
                transcript_data = translated.fetch()
                return transcript_data, target_language
            except (NoTranscriptFound, ValueError):
                continue
            except Exception as error:
                logging.debug(
                    "Transcript translation failed for %s to %s: %s",
                    video_id,
                    target_language,
                    error,
                )
    logging.warning("No transcript could be retrieved for %s.", video_id)
    return None, None


def save_transcript(transcript_dir: Path, video_id: str, language: Optional[str], segments: List[Dict]) -> Path:
    transcript_path = transcript_dir / f"{video_id}.json"
    payload = {
        "video_id": video_id,
        "language": language,
        "segments": segments,
    }
    write_json(transcript_path, payload)
    return transcript_path


def ingest_playlist_command(args: argparse.Namespace) -> int:
    playlist_id = extract_playlist_id(args.playlist_url)
    if not playlist_id:
        logging.error("Unable to determine playlist id from %s.", args.playlist_url)
        return 1

    playlist_root = args.output_root.resolve() / playlist_id
    audio_dir = playlist_root / "audio"
    transcripts_dir = playlist_root / "transcripts"
    metadata_dir = playlist_root / "metadata"
    ensure_directories([playlist_root, audio_dir, transcripts_dir, metadata_dir])

    manifest_path = playlist_root / MANIFEST_NAME
    if manifest_path.exists():
        manifest = read_json(manifest_path)
    else:
        manifest = {"videos": {}}

    manifest["playlist_url"] = args.playlist_url
    manifest["playlist_id"] = playlist_id
    manifest["language_preferences"] = args.languages

    playlist_info, entries = get_playlist_entries(args.playlist_url)
    if not entries:
        logging.error("No videos were found in the playlist.")
        return 1

    if args.max_videos is not None and args.max_videos > 0:
        entries = entries[: args.max_videos]

    summary = {
        "processed": 0,
        "audio_downloaded": 0,
        "audio_skipped": 0,
        "transcripts_saved": 0,
        "errors": 0,
    }

    total_videos = len(entries)
    logging.info(
        "Starting ingestion for playlist '%s' with %d videos.",
        playlist_info.get("title", playlist_id),
        total_videos,
    )

    for index, entry in enumerate(entries, start=1):
        summary["processed"] += 1
        entry_id = entry.get("id")
        entry_url = entry.get("url")
        if entry_url and entry_url.startswith("http"):
            video_url = entry_url
        else:
            if not entry_id:
                logging.error("Skipping entry %d: missing video id.", index)
                summary["errors"] += 1
                continue
            video_url = f"https://www.youtube.com/watch?v={entry_id}"

        logging.info("Processing video %d of %d: %s", index, total_videos, video_url)

        try:
            video_info = get_video_info(video_url)
        except Exception as error:
            logging.error("Metadata retrieval failed for %s: %s", video_url, error)
            summary["errors"] += 1
            continue

        video_id = video_info.get("id")
        if not video_id:
            logging.error("Video metadata missing id for %s.", video_url)
            summary["errors"] += 1
            continue

        audio_path = audio_dir / f"{video_id}.mp3"
        download_audio_now = not args.skip_audio
        if args.skip_existing and audio_path.exists():
            download_audio_now = False
            logging.info("Audio already exists for %s; skipping download.", video_id)

        if download_audio_now:
            try:
                download_audio(video_url, audio_dir)
                if audio_path.exists():
                    logging.info("Audio saved to %s.", audio_path)
                    summary["audio_downloaded"] += 1
                else:
                    logging.error("Audio file missing after download for %s.", video_id)
                    summary["errors"] += 1
                    continue
            except Exception as error:
                logging.error("Audio download failed for %s: %s", video_id, error)
                summary["errors"] += 1
                continue
        else:
            summary["audio_skipped"] += 1

        transcript_path: Optional[Path] = None
        transcript_language: Optional[str] = None
        if not args.skip_transcripts:
            segments, transcript_language = fetch_transcript(video_id, args.languages)
            if segments:
                transcript_path = save_transcript(transcripts_dir, video_id, transcript_language, segments)
                summary["transcripts_saved"] += 1
                logging.info("Transcript saved to %s.", transcript_path)
            else:
                logging.warning("Transcript unavailable for %s.", video_id)

        metadata_path = metadata_dir / f"{video_id}.json"
        metadata_payload = {
            "video_id": video_id,
            "title": video_info.get("title"),
            "description": video_info.get("description"),
            "channel": video_info.get("uploader"),
            "uploader_id": video_info.get("uploader_id"),
            "channel_id": video_info.get("channel_id"),
            "duration": video_info.get("duration"),
            "view_count": video_info.get("view_count"),
            "like_count": video_info.get("like_count"),
            "webpage_url": video_info.get("webpage_url") or video_url,
            "thumbnail": video_info.get("thumbnail"),
            "playlist_index": entry.get("playlist_index"),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "audio_path": relative_to_base(audio_path if audio_path.exists() else None, playlist_root),
            "transcript_path": relative_to_base(transcript_path, playlist_root),
            "transcript_language": transcript_language,
        }
        write_json(metadata_path, metadata_payload)

        manifest["videos"][video_id] = {
            "title": video_info.get("title"),
            "webpage_url": video_info.get("webpage_url") or video_url,
            "audio_path": relative_to_base(audio_path if audio_path.exists() else None, playlist_root),
            "transcript_path": relative_to_base(transcript_path, playlist_root),
            "transcript_language": transcript_language,
            "metadata_path": relative_to_base(metadata_path, playlist_root),
            "duration": video_info.get("duration"),
            "channel": video_info.get("uploader"),
            "playlist_index": entry.get("playlist_index"),
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

    manifest["updated_at"] = datetime.now(timezone.utc).isoformat()
    write_json(manifest_path, manifest)

    logging.info(
        "Ingestion complete. Processed=%d downloaded=%d transcripts=%d skipped_audio=%d errors=%d",
        summary["processed"],
        summary["audio_downloaded"],
        summary["transcripts_saved"],
        summary["audio_skipped"],
        summary["errors"],
    )
    return 0 if summary["errors"] == 0 else 1


def download_video_command(args: argparse.Namespace) -> int:
    output_dir = args.output_dir.resolve()
    ensure_directories([output_dir])

    try:
        video_info = get_video_info(args.url)
    except Exception as error:
        logging.error("Failed to retrieve metadata: %s", error)
        return 1

    video_id = video_info.get("id")
    if not video_id:
        logging.error("Video metadata missing id.")
        return 1

    audio_path = output_dir / f"{video_id}.mp3"
    if args.skip_existing and audio_path.exists():
        logging.info("Audio already exists at %s; skipping download.", audio_path)
        return 0

    try:
        download_audio(args.url, output_dir)
    except Exception as error:
        logging.error("Download failed: %s", error)
        return 1

    if audio_path.exists():
        logging.info("Audio saved to %s.", audio_path)
        return 0

    logging.error("Download reported success but audio file is missing.")
    return 1


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    configure_logging(verbose=getattr(args, "verbose", False))

    if not args.command:
        parser.print_help()
        return 1

    if args.command == "ingest-playlist":
        return ingest_playlist_command(args)
    if args.command == "download-video":
        return download_video_command(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
