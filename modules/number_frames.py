"""
number_frames.py — Generates countdown entry cards for mystery Top 5 videos.

Specs:
- Full screen dark background (#0a0a0a)
- Large countdown number centered
- Entry title in smaller text below
- Subtle vignette overlay
- Duration: 2–3 seconds per card
- UNIFORM across every video — same font, same layout, same style
- Text color: cold white #E8E8E8
- Accent color: deep red #8B0000
- Output: assets/frames/number_{n}_{slug}.mp4

Uses MoviePy (numpy array approach — no external font files required).
"""

import os
import re
import logging

import numpy as np

# MoviePy 1.x references Image.ANTIALIAS which was removed in Pillow 10+.
import PIL.Image
if not hasattr(PIL.Image, "ANTIALIAS"):
    PIL.Image.ANTIALIAS = PIL.Image.LANCZOS

log = logging.getLogger(__name__)

FRAMES_DIR = os.path.join("assets", "frames")

# Visual constants
BG_COLOR = (10, 10, 10)         # #0a0a0a — near-black
TEXT_COLOR = (232, 232, 232)    # #E8E8E8 — cold white
ACCENT_COLOR = (139, 0, 0)      # #8B0000 — deep red
WIDTH = 1920
HEIGHT = 1080
FPS = 30
CARD_DURATION = 2.5             # seconds


def _slugify(text: str) -> str:
    """Convert title to safe filename slug."""
    slug = re.sub(r"[^\w\s-]", "", text.lower())
    slug = re.sub(r"[\s-]+", "_", slug)
    return slug[:40]


def _make_vignette_array(width: int, height: int) -> np.ndarray:
    """
    Build a vignette mask (dark edges, bright center) as an RGBA numpy array.
    Applied as a semi-transparent dark overlay.
    """
    xs = np.linspace(-1, 1, width)
    ys = np.linspace(-1, 1, height)
    xx, yy = np.meshgrid(xs, ys)
    dist = np.sqrt(xx ** 2 + yy ** 2)
    # Normalize 0–1: center=0, edge=1
    dist = np.clip(dist / 1.4, 0, 1)
    alpha = (dist ** 2 * 180).astype(np.uint8)  # 0 center → 180 edge

    vignette = np.zeros((height, width, 4), dtype=np.uint8)
    vignette[:, :, 3] = alpha  # alpha channel only (RGB stays 0 = black overlay)
    return vignette


def _make_card_frame(number: int, title: str) -> np.ndarray:
    """
    Render a single countdown card frame as a (HEIGHT, WIDTH, 3) numpy array.
    Uses PIL for text rendering.
    """
    from PIL import Image, ImageDraw, ImageFont

    img = Image.new("RGB", (WIDTH, HEIGHT), color=BG_COLOR)
    draw = ImageDraw.Draw(img)

    # ── Accent bar (top and bottom) ──────────────────────────────────────────
    bar_height = 6
    draw.rectangle([0, 0, WIDTH, bar_height], fill=ACCENT_COLOR)
    draw.rectangle([0, HEIGHT - bar_height, WIDTH, HEIGHT], fill=ACCENT_COLOR)

    # ── Number ───────────────────────────────────────────────────────────────
    # Try to load a bold system font; fall back to PIL default
    num_font = None
    num_size = 320
    font_candidates = [
        # Windows system fonts
        "C:/Windows/Fonts/impact.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
        "C:/Windows/Fonts/arial.ttf",
        # Linux fallbacks (for cross-platform)
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    ]
    for font_path in font_candidates:
        if os.path.exists(font_path):
            try:
                num_font = ImageFont.truetype(font_path, num_size)
                break
            except Exception:
                continue

    num_str = str(number)
    num_color = ACCENT_COLOR if number == 1 else TEXT_COLOR

    if num_font:
        bbox = draw.textbbox((0, 0), num_str, font=num_font)
        num_w = bbox[2] - bbox[0]
        num_h = bbox[3] - bbox[1]
        num_x = (WIDTH - num_w) // 2
        num_y = HEIGHT // 2 - num_h - 30
        # Shadow
        draw.text((num_x + 4, num_y + 4), num_str, font=num_font,
                  fill=(30, 30, 30))
        draw.text((num_x, num_y), num_str, font=num_font, fill=num_color)
    else:
        # PIL default — still works, just not as stylized
        draw.text((WIDTH // 2, HEIGHT // 2 - 120), num_str, fill=num_color,
                  anchor="mm")

    # ── Title ─────────────────────────────────────────────────────────────────
    title_font = None
    title_size = 64
    for font_path in font_candidates:
        if os.path.exists(font_path):
            try:
                title_font = ImageFont.truetype(font_path, title_size)
                break
            except Exception:
                continue

    # Wrap title to two lines if long
    max_chars_per_line = 42
    if len(title) > max_chars_per_line:
        words = title.split()
        line1, line2 = [], []
        for word in words:
            if len(" ".join(line1 + [word])) <= max_chars_per_line:
                line1.append(word)
            else:
                line2.append(word)
        title_display = "\n".join([" ".join(line1), " ".join(line2)])
    else:
        title_display = title

    title_y_center = HEIGHT // 2 + 80
    if title_font:
        lines = title_display.split("\n")
        line_spacing = 72
        total_h = len(lines) * line_spacing
        y_start = title_y_center - total_h // 2
        for li, line in enumerate(lines):
            bbox = draw.textbbox((0, 0), line, font=title_font)
            lw = bbox[2] - bbox[0]
            lx = (WIDTH - lw) // 2
            ly = y_start + li * line_spacing
            draw.text((lx + 2, ly + 2), line, font=title_font, fill=(20, 20, 20))
            draw.text((lx, ly), line, font=title_font, fill=TEXT_COLOR)
    else:
        draw.text((WIDTH // 2, title_y_center), title_display,
                  fill=TEXT_COLOR, anchor="mm")

    # ── Vignette ──────────────────────────────────────────────────────────────
    vignette_array = _make_vignette_array(WIDTH, HEIGHT)
    vignette_img = Image.fromarray(vignette_array, "RGBA")
    img_rgba = img.convert("RGBA")
    img_rgba.paste(vignette_img, (0, 0), vignette_img)
    img = img_rgba.convert("RGB")

    return np.array(img)


def generate_number_card(number: int, title: str, force: bool = False) -> str:
    """
    Generate a countdown entry card video (2.5 seconds).

    Args:
        number: Countdown number (5 down to 1).
        title:  Entry title displayed below the number.
        force:  Re-generate even if file already exists.

    Returns:
        Absolute path to the generated .mp4 card file.
    """
    from moviepy.editor import ImageClip

    os.makedirs(FRAMES_DIR, exist_ok=True)
    slug = _slugify(title)
    filename = f"number_{number}_{slug}.mp4"
    output_path = os.path.join(FRAMES_DIR, filename)

    if os.path.exists(output_path) and not force:
        print(f"  [FRAMES] Card cached: {filename}")
        return output_path

    print(f"  [FRAMES] Rendering card #{number}: '{title[:50]}'...")

    frame = _make_card_frame(number, title)

    clip = ImageClip(frame, duration=CARD_DURATION)
    clip.write_videofile(
        output_path,
        fps=FPS,
        codec="libx264",
        audio=False,
        logger=None,
    )
    clip.close()

    print(f"  [FRAMES] Saved: {filename}")
    return output_path


def generate_all_cards(entries: list) -> list:
    """
    Generate countdown cards for all entries in a mystery Top 5 script.

    Args:
        entries: List of entry dicts, each with "number" and "title" keys.
                 Expected: [{"number": 5, "title": "..."}, ..., {"number": 1, "title": "..."}]

    Returns:
        List of dicts: [{"number": int, "title": str, "card_path": str}, ...]
    """
    results = []
    for entry in entries:
        number = entry.get("number", 0)
        title = entry.get("title", f"Entry {number}")
        card_path = generate_number_card(number, title)
        results.append({
            "number": number,
            "title": title,
            "card_path": card_path,
        })
    return results
