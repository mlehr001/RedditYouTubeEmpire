import os
from dotenv import load_dotenv

load_dotenv()

# ─── Reddit Settings ──────────────────────────────────────────────────────────

# Personal-story subreddits — conflict, relationships, emotional stakes
SUBREDDITS = [
    "AmItheAsshole",
    "tifu",
    "relationship_advice",
    "confession",
    "offmychest",
]

# Only pull posts with this score or higher
MIN_POST_SCORE = 1000

# How many posts to check per run
POST_LIMIT = 25

# Time filter: "day", "week", "month", "year", "all"
TIME_FILTER = "week"

# Skip posts that have already been used (stored in used_posts.txt)
SKIP_USED_POSTS = True

# ─── Script Settings ──────────────────────────────────────────────────────────

# Max words in the final TTS script (keep videos under ~10 min)
MAX_SCRIPT_WORDS = 1200

# Conversational intro — sets TikTok/YouTube Shorts tone
INTRO_TEMPLATE = "So this person posts to r/{subreddit}... and it gets wild fast."

# Outro to drive engagement
OUTRO = (
    "So... what would YOU have done? Drop it in the comments. "
    "And subscribe — new stories drop every day."
)

# ─── TTS Settings ─────────────────────────────────────────────────────────────

def _auto_tts_engine():
    """Pick the best available TTS engine based on configured API keys."""
    if os.getenv("ELEVENLABS_API_KEY"):
        return "elevenlabs"
    if os.getenv("OPENAI_API_KEY"):
        return "openai"
    return "gtts"

TTS_ENGINE = os.getenv("TTS_ENGINE", _auto_tts_engine())
TTS_LANGUAGE = "en"
TTS_SPEED = False

ELEVENLABS_VOICE_ID = os.getenv("ELEVENLABS_VOICE_ID", "21m00Tcm4TlvDq8ikWAM")
# stability 35-50, similarity_boost (clarity) 70-80, slight style exaggeration
ELEVENLABS_STABILITY = 0.40
ELEVENLABS_SIMILARITY_BOOST = 0.75
ELEVENLABS_STYLE = 0.30
OPENAI_TTS_VOICE = "onyx"

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
