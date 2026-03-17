"""
uploader.py — Uploads finished videos to YouTube via Data API v3
First run will open a browser for OAuth. Token is saved for future runs.
"""

import os
import pickle
import config
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
TOKEN_FILE = "token.pickle"


def _get_authenticated_service():
    credentials = None

    # Load saved token if it exists
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "rb") as token:
            credentials = pickle.load(token)

    # Refresh or re-authenticate if needed
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            secrets_file = os.getenv("YOUTUBE_CLIENT_SECRETS_FILE", "client_secrets.json")
            if not os.path.exists(secrets_file):
                raise FileNotFoundError(
                    f"YouTube client secrets file not found: '{secrets_file}'\n"
                    "Download it from Google Cloud Console → APIs & Services → Credentials."
                )
            flow = InstalledAppFlow.from_client_secrets_file(secrets_file, SCOPES)
            credentials = flow.run_local_server(port=0)

        # Save token for next run
        with open(TOKEN_FILE, "wb") as token:
            pickle.dump(credentials, token)

    return build("youtube", "v3", credentials=credentials)


def upload_to_youtube(video_path, post):
    """
    Uploads video_path to YouTube with metadata from post.
    Returns the video URL on success, None on failure.
    """
    try:
        youtube = _get_authenticated_service()

        title = config.YOUTUBE_TITLE_TEMPLATE.format(title=post["title"][:80])
        description = config.YOUTUBE_DESCRIPTION_TEMPLATE.format(
            title=post["title"],
            subreddit=post["subreddit"],
            score=f"{post['score']:,}",
        )

        body = {
            "snippet": {
                "title": title[:100],  # YouTube title limit
                "description": description,
                "tags": config.YOUTUBE_TAGS,
                "categoryId": config.YOUTUBE_CATEGORY_ID,
            },
            "status": {
                "privacyStatus": config.YOUTUBE_PRIVACY,
                "selfDeclaredMadeForKids": False,
            },
        }

        media = MediaFileUpload(
            video_path,
            mimetype="video/mp4",
            resumable=True,
            chunksize=1024 * 1024 * 10,  # 10MB chunks
        )

        request = youtube.videos().insert(
            part="snippet,status",
            body=body,
            media_body=media,
        )

        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                progress = int(status.progress() * 100)
                print(f"  Upload progress: {progress}%")

        video_id = response["id"]
        return f"https://www.youtube.com/watch?v={video_id}"

    except FileNotFoundError as e:
        print(f"  ⚠️  {e}")
        return None
    except Exception as e:
        print(f"  ⚠️  YouTube upload failed: {e}")
        return None
