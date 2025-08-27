import os
import subprocess
import re
import logging
from pathlib import Path

# --- Configuration ---
DOWNLOAD_FOLDER = "mp3_downloads"  # Folder to save downloaded MP3s
DEFAULT_CLIP_LENGTH_SECONDS = 60  # Default length for trimming

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- Helper Functions ---
def sanitize_filename(name):
    """
    Sanitizes a string to be a valid filename.
    Removes illegal characters and replaces spaces.
    """
    if not name:
        name = "untitled_audio"
    # Remove most non-alphanumeric characters (except hyphens, underscores, periods)
    name = re.sub(r'[^\w\-\.]', '_', name)
    # Replace multiple underscores/hyphens with a single one
    name = re.sub(r'__+', '_', name)
    name = re.sub(r'--+', '-', name)
    # Remove leading/trailing underscores/hyphens
    name = name.strip('_-')
    return name if name else "untitled_audio"

def ensure_download_folder():
    """Ensures the download folder exists."""
    Path(DOWNLOAD_FOLDER).mkdir(parents=True, exist_ok=True)

def run_ytdlp_command(command):
    """
    Runs a yt-dlp command using subprocess and captures its output.
    Returns the last line of stdout (expected to be the filepath) or None on error.
    """
    try:
        logging.info(f"Executing yt-dlp command: {' '.join(command)}")
        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,  # Don't raise an exception for non-zero exit codes immediately
            encoding='utf-8'
        )

        if process.returncode != 0:
            logging.error(f"yt-dlp error (code {process.returncode}):")
            if process.stdout:
                logging.error(f"yt-dlp stdout:\n{process.stdout.strip()}")
            if process.stderr:
                logging.error(f"yt-dlp stderr:\n{process.stderr.strip()}")
            return None

        # Assuming the last non-empty line of stdout is the filepath
        output_lines = [line for line in process.stdout.strip().split('\n') if line.strip()]
        if output_lines:
            filepath = output_lines[-1].strip()
            # Verify the path isn't pointing to the template itself due to an error
            if "%(title)s" not in filepath and "%(ext)s" not in filepath:
                 # Basic check that the file reported by yt-dlp actually exists
                if Path(filepath).exists() and Path(filepath).is_file():
                    logging.info(f"yt-dlp successfully processed: {filepath}")
                    return filepath
                else:
                    logging.error(f"yt-dlp reported filepath '{filepath}' but it was not found or is not a file.")
                    logging.error("This might happen if the video title leads to an invalid/empty filename "
                                  "or if there were permission issues.")
                    if process.stderr: # Log stderr again if file not found, it might contain clues
                        logging.error(f"yt-dlp stderr (for missing file context):\n{process.stderr.strip()}")
                    return None
            else:
                logging.error(f"yt-dlp output filepath seems to be an unresolved template: {filepath}")
                return None
        else:
            logging.error("yt-dlp did not return a filepath in stdout.")
            if process.stderr:
                logging.error(f"yt-dlp stderr:\n{process.stderr.strip()}")
            return None

    except FileNotFoundError:
        logging.error("yt-dlp command not found. Please ensure yt-dlp is installed and in your system's PATH.")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while running yt-dlp: {e}")
        return None

# --- Core Functions ---
def download_mp3_with_ytdlp(video_url):
    """
    Downloads audio from a video URL as MP3 using yt-dlp.
    Saves to DOWNLOAD_FOLDER with a filename based on the video title.
    Returns the full path to the downloaded MP3 file or None if an error occurs.
    """
    ensure_download_folder()

    # Output template for yt-dlp. It will create the filename based on video metadata.
    # Using a temporary placeholder for the title initially if needed, but yt-dlp's own templating is better.
    # The --print option will give the *actual* final path.
    # We use a generic output template that yt-dlp will fill.
    # %(title)s will be sanitized by yt-dlp.
    # --restrict-filenames can be added for stricter sanitization if needed.
    output_template = os.path.join(DOWNLOAD_FOLDER, "%(title)s.%(ext)s")

    # Consider adding --restrict-filenames if you encounter issues with special characters in titles
    # For extremely problematic titles, yt-dlp might use the ID. The --print option handles this.
    # Using 'bestaudio/best' and letting ffmpeg handle post-processing for format ensures better source quality.
    cmd = [
        "yt-dlp",
        "-x",  # --extract-audio
        "--audio-format", "mp3",
        "--audio-quality", "0",  # 0 is best quality for variable bitrate (VBR)
        #"--get-filename", # Alternative: get filename first, then download. But --print is good for post-dl confirm.
        "--print", "filepath", # More direct way to get the final path
        # Or for older yt-dlp that might not support just "filepath":
        # "--print", "%(filepath)q", # q for quoted, safer for paths with spaces
        "-o", output_template,
        "--ffmpeg-location", "/path/to/your/ffmpeg", # Optional: Explicitly specify if not in PATH
        # "--verbose", # Uncomment for more detailed yt-dlp output during debugging
        video_url
    ]
    # Remove --ffmpeg-location if FFmpeg is reliably in your PATH
    # For example, if FFmpeg is in PATH, remove the line:
    # cmd.pop(cmd.index("--ffmpeg-location") + 1)
    # cmd.pop(cmd.index("--ffmpeg-location"))
    # For now, assuming it might be needed by some users. If FFmpeg is in PATH, yt-dlp usually finds it.
    # A common setup is to have ffmpeg in PATH, so let's remove the explicit path for now.
    # If issues, user can add it back or ensure ffmpeg is in PATH.
    cmd = [item for item in cmd if not item.startswith("--ffmpeg-location")]


    downloaded_path = run_ytdlp_command(cmd)

    if downloaded_path and Path(downloaded_path).exists():
        return downloaded_path
    else:
        logging.error(f"Failed to download or locate MP3 for URL: {video_url}")
        return None

def trim_and_tag_audio(mp3_path, new_title=None, new_artist=None, clip_length_seconds=None):
    """
    Loads an MP3, optionally trims it, and optionally re-tags it.
    (This is a placeholder for your pydub logic)
    """
    if not mp3_path or not Path(mp3_path).exists():
        logging.error(f"trim_and_tag: Invalid or non-existent MP3 path: {mp3_path}")
        return None

    try:
        from pydub import AudioSegment
        from pydub.utils import mediainfo
    except ImportError:
        logging.error("pydub library is not installed. Please install it with 'pip install pydub'.")
        return None

    logging.info(f"Attempting to process audio file: {mp3_path}")

    try:
        audio = AudioSegment.from_file(mp3_path, format="mp3")
        logging.info(f"Successfully loaded '{mp3_path}' with pydub.")

        # --- 1. Trimming (Example) ---
        if clip_length_seconds is not None and clip_length_seconds > 0:
            duration_ms = len(audio)
            clip_length_ms = clip_length_seconds * 1000
            if clip_length_ms < duration_ms:
                audio = audio[:clip_length_ms]
                logging.info(f"Trimmed audio to {clip_length_seconds} seconds.")
            else:
                logging.info(f"Clip length ({clip_length_seconds}s) is longer than or equal to audio duration. No trimming performed.")

        # --- 2. Tagging (Example) ---
        # pydub uses ffmpeg for exporting with tags.
        # The 'tags' parameter in export is a dictionary.
        tags_to_apply = {}
        current_tags = mediainfo(mp3_path).get('TAG', {})

        if new_title:
            tags_to_apply['title'] = new_title
        elif 'title' in current_tags: # Keep original if not overwriting
            tags_to_apply['title'] = current_tags['title']

        if new_artist:
            tags_to_apply['artist'] = new_artist
        elif 'artist' in current_tags: # Keep original if not overwriting
            tags_to_apply['artist'] = current_tags['artist']
        
        # Add other tags as needed, e.g., album
        # tags_to_apply['album'] = "My Album"


        # --- 3. Exporting (Overwriting original in this example) ---
        # You might want to save to a new file instead of overwriting.
        # For example: processed_filename = Path(mp3_path).stem + "_processed.mp3"
        # processed_path = Path(DOWNLOAD_FOLDER) / processed_filename
        
        export_path = mp3_path # Overwriting
        
        logging.info(f"Exporting processed audio to: {export_path} with tags: {tags_to_apply}")
        audio.export(export_path, format="mp3", tags=tags_to_apply)
        logging.info("Successfully trimmed and/or tagged audio.")
        return export_path

    except FileNotFoundError:
        logging.error(f"pydub: File not found at {mp3_path}. This should not happen if path was pre-validated.")
        return None
    except Exception as e: # Catch pydub specific errors like CouldntDecodeError or generic ones
        logging.error(f"pydub: Error processing audio file '{mp3_path}': {e}")
        logging.error("Ensure FFmpeg is installed and in your PATH, and the file is a valid MP3.")
        return None

# --- Main Execution ---
if __name__ == "__main__":
    print("🎵 YouTube to MP3 Downloader & Processor 🎵")
    print("--------------------------------------------")

    video_url = input("Enter YouTube video URL: ").strip()

    if not video_url:
        logging.error("No URL provided. Exiting.")
        exit()

    logging.info(f"Attempting to download audio from: {video_url}")
    downloaded_mp3_file = download_mp3_with_ytdlp(video_url)

    if downloaded_mp3_file:
        logging.info(f"Successfully downloaded MP3 to: {downloaded_mp3_file}")

        # Example: Ask if user wants to trim and tag
        choice = input("Do you want to trim and/or re-tag the MP3? (yes/no): ").strip().lower()
        if choice == 'yes':
            try:
                original_filename = Path(downloaded_mp3_file).stem
                default_title = original_filename.replace('_', ' ') # Basic title from filename

                custom_title = input(f"Enter new title (or press Enter to keep/use '{default_title}'): ").strip()
                if not custom_title:
                    custom_title = default_title # Use a sensible default if empty

                custom_artist = input("Enter new artist (or press Enter to keep original/leave blank): ").strip()
                
                clip_len_str = input(f"Enter clip length in seconds (e.g., {DEFAULT_CLIP_LENGTH_SECONDS}, or 0 for no trim): ").strip()
                clip_len = DEFAULT_CLIP_LENGTH_SECONDS
                if clip_len_str:
                    try:
                        clip_len_val = int(clip_len_str)
                        if clip_len_val == 0:
                            clip_len = None # No trimming
                            logging.info("No trimming will be performed.")
                        elif clip_len_val < 0:
                            logging.warning("Clip length cannot be negative. Using default/no trim.")
                            clip_len = None if DEFAULT_CLIP_LENGTH_SECONDS == 0 else DEFAULT_CLIP_LENGTH_SECONDS
                        else:
                            clip_len = clip_len_val
                    except ValueError:
                        logging.warning(f"Invalid clip length. Using default {DEFAULT_CLIP_LENGTH_SECONDS}s or no trim.")
                        clip_len = None if DEFAULT_CLIP_LENGTH_SECONDS == 0 else DEFAULT_CLIP_LENGTH_SECONDS
                else: # User pressed enter
                    clip_len = None if DEFAULT_CLIP_LENGTH_SECONDS == 0 else DEFAULT_CLIP_LENGTH_SECONDS


                processed_file = trim_and_tag_audio(
                    downloaded_mp3_file,
                    new_title=custom_title if custom_title else None,
                    new_artist=custom_artist if custom_artist else None,
                    clip_length_seconds=clip_len
                )
                if processed_file:
                    logging.info(f"Processed file saved at: {processed_file}")
                else:
                    logging.error("Failed to process the MP3 file.")
            except Exception as e:
                logging.error(f"Error during trim/tag input or call: {e}")
        else:
            logging.info("Skipping trim and tag.")
    else:
        logging.error(f"Could not download MP3 from {video_url}.")

    logging.info("Script finished.")