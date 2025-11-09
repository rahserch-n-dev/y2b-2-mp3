# YouTube Playlist Ingestion

End to end utility for building audio and transcript datasets from YouTube playlists. Designed to support downstream machine learning workflows where you need aligned audio, text, and metadata bundles.

## Features

- Playlist level ingestion with optional video limits for quick iteration.
- Audio download as MP3 using `yt-dlp` and `ffmpeg`.
- Transcript retrieval via `youtube-transcript-api` with manual, auto, and translated fallbacks.
- Structured metadata and manifest files that record file locations and source context.
- Reusable dataset layout keyed by playlist id with relative paths for portability.

## Getting Started

Install dependencies:

```bash
pip install -r requirements.txt
```

Run playlist ingestion (verbose logging recommended for the first run):

```bash
python main.py ingest-playlist \
  --playlist-url "https://youtube.com/playlist?list=PL..." \
  --verbose
```

Optional flags:

- `--languages <codes>`: transcript language priority, default is `en en-US en-GB`.
- `--max-videos <n>`: stop after `n` videos.
- `--skip-existing`: reuse previously downloaded audio.
- `--skip-audio`: create transcript and metadata only.
- `--skip-transcripts`: download audio without transcript retrieval.
- `--output-root <path>`: change the dataset root directory (defaults to `data/`).

Single video download remains available:

```bash
python main.py download-video --url "https://youtu.be/..." --verbose
```

## Output Layout

```
data/
  <playlist_id>/
    audio/            # MP3 files named <video_id>.mp3
    transcripts/      # JSON transcripts with segments and language metadata
    metadata/         # Per video metadata snapshots
    manifest.json     # Playlist level summary of processed assets
```

All JSON content is written with ASCII-safe escapes so it can be moved across environments without encoding surprises.

## Next Steps

- Feed `manifest.json` into your feature extraction or training pipeline.
- Store derived features or annotations beside the existing structure by extending metadata entries.
- Automate ingestion on a schedule with your preferred orchestrator (Airflow, Prefect, Dagster, etc.).
