"""
Content Generation System - The "Cheat" Pipeline
Automated topic discovery → Research → AI script writing
"""

import aiohttp
import asyncio
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime
import re
import json
import os

@dataclass
class MysteryCase:
    """Structured mystery case for video production"""
    title: str
    year: Optional[int]
    location: str
    category: str  # "disappearance", "death", "ufo", "paranormal"
    summary: str
    key_details: List[str]
    theories: List[str]
    source_urls: List[str]
    images: List[str] = field(default_factory=list)
    relevance_score: float = 0.0  # 0-1

    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "year": self.year,
            "location": self.location,
            "category": self.category,
            "summary": self.summary,
            "key_details": self.key_details,
            "theories": self.theories,
            "source_urls": self.source_urls,
            "relevance_score": self.relevance_score
        }

class WikipediaMysteryScraper:
    """Scrapes Wikipedia for unsolved mysteries - reliable, structured"""

    def __init__(self):
        self.base_url = "https://en.wikipedia.org/w/api.php"

    async def get_mystery_lists(self) -> List[str]:
        """Get list pages of unsolved cases"""
        return [
            "List of people who disappeared mysteriously",
            "List of unsolved deaths",
            "List of unsolved murders",
            "List of missing aircraft",
            "List of unexplained disappearances"
        ]

    async def scrape_list_page(self, session: aiohttp.ClientSession, list_title: str) -> List[MysteryCase]:
        """Extract cases from a Wikipedia list page"""
        params = {
            "action": "parse",
            "page": list_title,
            "prop": "text",
            "format": "json",
            "redirects": 1
        }

        async with session.get(self.base_url, params=params) as resp:
            data = await resp.json()
            html = data["parse"]["text"]["*"]
            return self._parse_wiki_table(html, list_title)

    def _parse_wiki_table(self, html: str, category: str) -> List[MysteryCase]:
        """Extract cases from Wikipedia HTML"""
        cases = []

        # Look for table rows
        row_pattern = r'<tr[^>]*>(.*?)</tr>'
        rows = re.findall(row_pattern, html, re.DOTALL)

        for row in rows[:25]:  # First 25 entries
            # Extract year
            year_match = re.search(r'(19|20)\d{2}', row)
            year = int(year_match.group()) if year_match else None

            # Extract name
            name_match = re.search(r'<a[^>]*>([^<]+)</a>', row)
            if name_match:
                name = name_match.group(1).strip()

                # Skip headers
                if any(x in name.lower() for x in ["name", "date", "case"]):
                    continue

                case = MysteryCase(
                    title=name,
                    year=year,
                    location="Unknown",
                    category=self._categorize(category),
                    summary=f"Unsolved case from {year}" if year else "Unsolved mystery",
                    key_details=[],
                    theories=[],
                    source_urls=[f"https://en.wikipedia.org/wiki/{name.replace(' ', '_')}"],
                    relevance_score=0.7 if year and 1950 < year < 2010 else 0.5
                )
                cases.append(case)

        return cases

    def _categorize(self, list_title: str) -> str:
        title_lower = list_title.lower()
        if "disappeared" in title_lower:
            return "disappearance"
        elif "death" in title_lower or "murder" in title_lower:
            return "unsolved_death"
        else:
            return "mystery"

    async def enrich_case(self, session: aiohttp.ClientSession, case: MysteryCase) -> MysteryCase:
        """Get details from individual case page"""
        params = {
            "action": "query",
            "titles": case.title,
            "prop": "extracts",
            "exintro": True,
            "explaintext": True,
            "format": "json"
        }

        async with session.get(self.base_url, params=params) as resp:
            data = await resp.json()
            pages = data["query"]["pages"]

            for page_id, page in pages.items():
                if "extract" in page:
                    extract = page["extract"]
                    # First paragraph as summary
                    first_para = extract.split("\n")[0][:400]
                    case.summary = first_para

                    # Extract location
                    location_patterns = [
                        r'in ([A-Z][a-z]+(?:, [A-Z][a-z]+)?)',
                        r'near ([A-Z][a-z]+)',
                        r'([A-Z][a-z]+), (?:California|Texas|Florida|New York|England)'
                    ]
                    for pattern in location_patterns:
                        match = re.search(pattern, extract)
                        if match:
                            case.location = match.group(1)
                            break

        return case


class AIScriptWriter:
    """
    Generates TTS-ready scripts using AI prompts.
    Formatted for edge-tts with timing cues.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.base_url = "https://api.openai.com/v1"

    def generate_script_prompt(self, case: MysteryCase, case_num: int) -> str:
        """
        Generate a prompt for AI to write a script section.
        This prompt you feed to ChatGPT/Claude/whatever.
        """
        word_count = 450  # ~3 minutes at 150 wpm

        prompt = f"""Write a dramatic true crime documentary script section.

CASE {case_num}: {case.title}
LOCATION: {case.location}
YEAR: {case.year or "Unknown"}

FACTS TO INCLUDE:
{case.summary}

KEY DETAILS TO MENTION:
{chr(10).join(f"- {d}" for d in case.key_details[:5]) if case.key_details else "- Research and add 3-5 specific details"}

REQUIREMENTS:
- Target: {word_count} words (exactly 3 minutes at 150 wpm)
- Tone: Dramatic, mysterious, respectful to victims
- Structure: Setup (30s) → The Event (90s) → Theories/Unsolved (60s)
- Include specific dates, names, locations when known
- Add [PAUSE] markers for dramatic beats
- End with a hook to next case
- NO bullet points, NO headers, pure narration text

OUTPUT FORMAT:
Just the narration text. No stage directions except [PAUSE].

Example opening:
"It was a cold November morning in 1987 when Sarah Mitchell left her home in rural Vermont, never to be seen again. [PAUSE] What happened over the next 48 hours would baffle investigators for decades..."
"""
        return prompt

    def generate_full_video_prompt(
        self,
        cases: List[MysteryCase],
        video_title: str = "5 Unsolved Mysteries"
    ) -> str:
        """Generate prompt for full video script"""

        case_sections = []
        for i, case in enumerate(cases, 1):
            case_sections.append(f"""
CASE {i}: {case.title}
- Location: {case.location}
- Year: {case.year or "Unknown"}
- Summary: {case.summary[:150]}...""")

        full_prompt = f"""Write a complete documentary script for a YouTube video titled "{video_title}".

TOTAL RUNTIME: 18-20 minutes
PACE: 150 words per minute (slow, dramatic)
TARGET WORD COUNT: 2700-3000 words

CASES TO COVER:
{chr(10).join(case_sections)}

SCRIPT STRUCTURE:
1. INTRO (45 seconds, ~110 words)
   - Hook: "Five mysteries. Five unanswered questions."
   - Channel intro, subscribe reminder
   - Transition to Case 1

2. FIVE CASE SECTIONS (3-4 minutes each, ~450-500 words each)
   Each case follows this structure:

   a) SETUP (30 seconds, ~75 words)
      - Who, where, when
      - Establish the "normal" before the mystery

   b) THE EVENT (90-120 seconds, ~225-300 words)
      - What happened
      - Immediate aftermath
      - Initial investigation details

   c) THEORIES/UNSOLVED (60-90 seconds, ~150-225 words)
      - What investigators considered
      - Why it remains unsolved
      - Lasting impact

3. OUTRO (60 seconds, ~150 words)
   - Recap hook
   - Call to action (subscribe, comment)
   - Tease next video

WRITING RULES:
- Use present tense for immediacy ("Sarah walks out the door...")
- Include [PAUSE] for dramatic beats (2-3 per case)
- Include [SFX: ambient sound description] where helpful
- NO visual directions, NO camera notes, pure narration
- Respectful tone - these are real people/events
- Specific details > general statements
- Each case ends with an unanswered question

OUTPUT FORMAT:
Return as:

[INTRO]
(script text)

[CASE 1: {cases[0].title}]
(script text)

[CASE 2: {cases[1].title}]
(script text)

...etc through Case 5 and Outro

Make it ready to paste into a TTS engine."""

        return full_prompt

    def parse_ai_output_to_scenes(self, ai_output: str) -> List[Dict]:
        """
        Parse AI-generated script into scene blocks.
        Returns list of scenes with timing.
        """
        scenes = []

        # Split by case headers
        sections = re.split(r'\n\[([A-Z\d\s:]+)\]\n', ai_output)

        for i in range(1, len(sections), 2):
            if i >= len(sections):
                break

            header = sections[i]
            content = sections[i+1] if i+1 < len(sections) else ""

            # Clean content
            content = content.strip()
            if not content:
                continue

            # Calculate timing
            word_count = len(content.split())
            duration = (word_count / 150) * 60  # seconds at 150 wpm

            # Determine mood
            mood = "neutral"
            if "CASE" in header:
                mood = "tense"
            elif "INTRO" in header:
                mood = "setup"
            elif "OUTRO" in header:
                mood = "conclusion"

            scene = {
                "scene_id": header.lower().replace(" ", "_").replace(":", ""),
                "header": header,
                "content": content,
                "word_count": word_count,
                "duration": duration,
                "mood": mood,
                "b_roll_tags": self._extract_broll_tags(content)
            }
            scenes.append(scene)

        return scenes

    def _extract_broll_tags(self, content: str) -> List[str]:
        """Extract B-roll search terms from script content"""
        tags = []

        # Look for location mentions
        location_patterns = [
            r'(?:in|near|at) ([A-Z][a-z]+(?: [A-Z][a-z]+)?)',
            r'([A-Z][a-z]+(?: [A-Z][a-z]+)?), (?:California|Texas|Florida|New York|England)'
        ]
        for pattern in location_patterns:
            matches = re.findall(pattern, content)
            tags.extend([m.lower() for m in matches])

        # Time indicators
        if any(word in content.lower() for word in ["night", "evening", "dark"]):
            tags.append("night")
        if any(word in content.lower() for word in ["morning", "dawn", "day"]):
            tags.append("day")

        # Mood indicators
        if any(word in content.lower() for word in ["police", "investigator", "detective"]):
            tags.extend(["police", "investigation"])
        if any(word in content.lower() for word in ["house", "home", "building"]):
            tags.append("house")
        if any(word in content.lower() for word in ["forest", "woods", "trees"]):
            tags.append("forest")

        return list(set(tags))


class ContentCurator:
    """Main orchestrator: Find topics → Generate AI prompts → Output scripts"""

    def __init__(self):
        self.wikipedia = WikipediaMysteryScraper()
        self.script_writer = AIScriptWriter()

    async def get_daily_topics(self, count: int = 5) -> List[MysteryCase]:
        """Get curated mystery topics for today"""
        print("Sourcing mystery topics...")

        async with aiohttp.ClientSession() as session:
            all_cases = []

            # Wikipedia (reliable base)
            lists = await self.wikipedia.get_mystery_lists()
            for list_page in lists[:2]:  # Top 2 lists
                cases = await self.wikipedia.scrape_list_page(session, list_page)
                # Enrich top 10
                for case in cases[:10]:
                    enriched = await self.wikipedia.enrich_case(session, case)
                    all_cases.append(enriched)

            # Score and rank
            scored = self._score_cases(all_cases)
            return scored[:count]

    def _score_cases(self, cases: List[MysteryCase]) -> List[MysteryCase]:
        """Score by video production potential"""
        for case in cases:
            score = case.relevance_score

            # Bonus for vintage (more B-roll available)
            if case.year and 1970 <= case.year <= 2000:
                score += 0.2

            # Bonus for location
            if case.location and case.location != "Unknown":
                score += 0.1

            # Penalty for too recent
            if case.year and case.year > 2015:
                score -= 0.15

            case.relevance_score = min(1.0, max(0.0, score))

        return sorted(cases, key=lambda x: x.relevance_score, reverse=True)

    def generate_production_package(
        self,
        cases: List[MysteryCase],
        video_title: str = "5 Unsolved Mysteries"
    ) -> Dict:
        """
        Generate complete production package:
        - AI prompts for script writing
        - Scene breakdown with B-roll tags
        - Research notes
        """

        # Generate AI script prompt
        full_prompt = self.script_writer.generate_full_video_prompt(cases, video_title)

        # Generate individual case prompts (for iterative writing)
        case_prompts = []
        for i, case in enumerate(cases, 1):
            prompt = self.script_writer.generate_script_prompt(case, i)
            case_prompts.append({
                "case_num": i,
                "title": case.title,
                "prompt": prompt,
                "suggested_b_roll": [
                    f"{case.location.lower()} {case.year or 'vintage'}",
                    "mystery atmosphere",
                    f"{case.category.replace('_', ' ')}"
                ]
            })

        # Research notes
        research_notes = {
            "video_title": video_title,
            "total_runtime": "18-20 minutes",
            "cases": [case.to_dict() for case in cases],
            "b_roll_search_terms": self._aggregate_broll_terms(cases),
            "music_mood": "tense, mysterious, documentary"
        }

        return {
            "ai_full_script_prompt": full_prompt,
            "ai_case_prompts": case_prompts,
            "research_notes": research_notes,
            "ready_for_tts": False,  # Set True after you paste AI output
            "ai_output_parse_instructions": "Paste AI output into parse_ai_output_to_scenes()"
        }

    def _aggregate_broll_terms(self, cases: List[MysteryCase]) -> List[str]:
        """Aggregate B-roll search terms from all cases"""
        terms = set()

        for case in cases:
            if case.location:
                terms.add(f"{case.location.lower()} vintage")
            if case.year:
                decade = (case.year // 10) * 10
                terms.add(f"{decade}s {case.category}")
            terms.add(case.category.replace("_", " "))

        # Add generic mystery terms
        terms.update([
            "abandoned place night",
            "dark forest road",
            "vintage investigation",
            "old photograph",
            "mystery fog"
        ])

        return list(terms)


# CLI Usage
async def main():
    """Example: Generate today's production package"""
    curator = ContentCurator()

    # 1. Get topics
    print("Finding mystery topics...")
    cases = await curator.get_daily_topics(count=5)

    print(f"\nSelected Cases:")
    for i, case in enumerate(cases, 1):
        print(f"{i}. {case.title} ({case.year}) - Score: {case.relevance_score:.2f}")

    # 2. Generate production package
    package = curator.generate_production_package(cases, "5 Unsolved Vanishings")

    # 3. Save outputs
    with open("ai_script_prompt.txt", "w") as f:
        f.write(package["ai_full_script_prompt"])

    with open("case_prompts.json", "w") as f:
        json.dump(package["ai_case_prompts"], f, indent=2)

    with open("research_notes.json", "w") as f:
        json.dump(package["research_notes"], f, indent=2)

    print(f"\nProduction package saved:")
    print(f"  - ai_script_prompt.txt (paste to ChatGPT/Claude)")
    print(f"  - case_prompts.json (individual case prompts)")
    print(f"  - research_notes.json (B-roll search terms)")

    # 4. Show B-roll search terms
    print(f"\nB-Roll Search Terms:")
    for term in package["research_notes"]["b_roll_search_terms"][:10]:
        print(f"  - {term}")

if __name__ == "__main__":
    asyncio.run(main())
