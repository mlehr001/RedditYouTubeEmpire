import os
from dotenv import load_dotenv

load_dotenv()

# ─── Reddit Settings ──────────────────────────────────────────────────────────

# Subreddits to scrape (add or remove as you like)
SUBREDDITS = [
    "AmItheAsshole",
    "tifu",
    "confessions",
    "relationships",
    "TrueOffMyChest",
]

# Only pull posts with this score or higher
MIN_POST_SCORE = 5000

# How many posts to check per run
POST_LIMIT = 10

# Time filter: "day", "week", "month", "year", "all"
TIME_FILTER = "week"

# Skip posts that have already been used (stored in used_posts.txt)
SKIP_USED_POSTS = True

# ─── Script Settings ──────────────────────────────────────────────────────────

# Max words in the final TTS script (keep videos under ~10 min)
MAX_SCRIPT_WORDS = 1200

# Add an intro line before reading the post
INTRO_TEMPLATE = "Today's story comes from Reddit's {subreddit} community."

# Add an outro to drive engagement
OUTRO = (
    "What do you think? Drop a comment below. "
    "Subscribe for more Reddit stories every day."
)

# ─── TTS Settings ─────────────────────────────────────────────────────────────

TTS_ENGINE = os.getenv("TTS_ENGINE", "gtts")  # "gtts", "elevenlabs", "openai"
TTS_LANGUAGE = "en"                            # for gTTS
TTS_SPEED = False                              # gTTS slow mode

ELEVENLABS_VOICE_ID = os.getenv("ELEVENLABS_VOICE_ID", "21m00Tcm4TlvDq8ikWAM")
OPENAI_TTS_VOICE = "onyx"                      # alloy, echo, fable, onyx, nova, shimmer

# ─── Video Settings ───────────────────────────────────────────────────────────

VIDEO_WIDTH = 1920
VIDEO_HEIGHT = 1080
VIDEO_FPS = 30

# Pexels search terms for background footage
BROLL_SEARCH_TERMS = [
    "minecraft parkour",
    "subway surfers",
    "satisfying video",
    "city timelapse",
    "nature relaxing",
]

# How many background clips to keep cached locally
BROLL_CACHE_COUNT = 10

# Audio volume of background video (0.0 = silent, 1.0 = full)
BROLL_VOLUME = 0.05

# ─── Output Settings ──────────────────────────────────────────────────────────

OUTPUT_DIR = "output"
ASSETS_DIR = os.path.join("assets", "broll")

# ─── YouTube Settings ─────────────────────────────────────────────────────────

YOUTUBE_CATEGORY_ID = "22"   # 22 = People & Blogs
YOUTUBE_PRIVACY = "public"   # "public", "private", "unlisted"

YOUTUBE_TAGS = [
    "reddit", "reddit stories", "tts", "reddit tts",
    "aita", "tifu", "storytime", "reddit readings"
]

YOUTUBE_TITLE_TEMPLATE = "Reddit Story: {title} #reddit #storytime"
YOUTUBE_DESCRIPTION_TEMPLATE = (
    "📖 Today's Reddit story:\n\n"
    "{title}\n\n"
    "Posted in r/{subreddit} | {score} upvotes\n\n"
    "🔔 Subscribe for daily Reddit stories!\n\n"
    "#reddit #{subreddit} #storytime #tts"
)
