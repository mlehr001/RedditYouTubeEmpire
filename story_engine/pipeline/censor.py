"""
Censor stage — hardened for all sources including 4chan.
Hard rules enforced before any output is published.
Blocking rules reject the story. Warn rules flag but allow through.
Every match is logged to censor_log table.

Severity levels:
  block  → story is rejected, never published
  warn   → story is flagged for review but can proceed
"""

import logging
import re
from dataclasses import dataclass
from typing import List, Tuple

from story_engine.db.database import get_db
from story_engine.queue.worker import BaseWorker

logger = logging.getLogger(__name__)


@dataclass
class CensorRule:
    name: str
    pattern: re.Pattern
    severity: str       # 'block' | 'warn'
    field: str          # 'body' | 'title' | 'any'
    action: str         # 'blocked' | 'flagged' | 'redacted'
    description: str


def _c(pattern: str, flags=re.IGNORECASE) -> re.Pattern:
    return re.compile(pattern, flags)


# ─────────────────────────────────────────────────────────────────────────────
# CENSOR RULE REGISTRY — ordered block → warn
# ─────────────────────────────────────────────────────────────────────────────

CENSOR_RULES: List[CensorRule] = [

    # ── BLOCK: Minor sexual content ───────────────────────────────────────────
    CensorRule("minor_sexual_content",
        _c(r"\b(minor|child|underage|teen|juvenile|jailbait|preteen|pre-teen)\b.{0,100}\b(sex|naked|nude|sexual|assault|abuse|molest|rape|fondle|groom)\b"),
        "block", "any", "blocked", "Sexual content involving minors"),

    CensorRule("csam_terms",
        _c(r"\b(cp|csam|child.?porn|kiddie.?porn|loli(?:con)?|shotacon)\b"),
        "block", "any", "blocked", "CSAM reference"),

    CensorRule("age_and_sexual",
        _c(r"\b(1[0-7]|seventeen|sixteen|fifteen|fourteen|thirteen|twelve|eleven|ten)\s*(?:year[s]?\s*old|y/?o)\b.{0,200}\b(sex|naked|nude|sexual|rape|assault)\b"),
        "block", "any", "blocked", "Age + sexual context involving possible minor"),

    # ── BLOCK: Self-harm instructions ─────────────────────────────────────────
    CensorRule("self_harm_instructions",
        _c(r"\b(how to|ways to|methods to|guide to|step[s]? to).{0,50}\b(kill yourself|commit suicide|end your life|overdose on|hang yourself|slit your|cut your)\b"),
        "block", "any", "blocked", "Self-harm instructions"),

    CensorRule("suicide_method_detail",
        _c(r"\b(lethal dose|effective method|painless way).{0,50}\b(die|suicide|death|kill)\b"),
        "block", "any", "blocked", "Detailed suicide method"),

    # ── BLOCK: WMD / weapons of mass destruction ──────────────────────────────
    CensorRule("wmd_instructions",
        _c(r"\b(how to (make|build|synthesize|create|produce)).{0,80}\b(bomb|explosive|nerve agent|ricin|anthrax|sarin|vx gas|fentanyl|c4|pipe bomb|ied)\b"),
        "block", "any", "blocked", "WMD/explosives instructions"),

    CensorRule("bioweapon",
        _c(r"\b(weaponize|weaponized).{0,50}\b(anthrax|botulinum|plague|smallpox|ricin|polonium)\b"),
        "block", "any", "blocked", "Bioweapon reference"),

    # ── BLOCK: PII / doxxing ──────────────────────────────────────────────────
    CensorRule("ssn_pattern",
        _c(r"\b\d{3}-\d{2}-\d{4}\b"),
        "block", "any", "blocked", "SSN pattern detected"),

    CensorRule("credit_card",
        _c(r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|[0-9]{4}[\s\-][0-9]{4}[\s\-][0-9]{4}[\s\-][0-9]{4})\b"),
        "block", "any", "blocked", "Credit card number pattern"),

    # ── BLOCK: Slurs (hard blocks) ────────────────────────────────────────────
    CensorRule("racial_slur_n_hard",
        _c(r"\bn[i*1!]+gg[e3a@]+r[s]?\b"),
        "block", "body", "blocked", "Hard racial slur (n-word)"),

    CensorRule("racial_slur_variants",
        _c(r"\b(ch[i*]nk|g[o0]ok|sp[i*]c|k[i*]ke|w[e3]tb[a@]ck|c[o0][o0]n(?!\s*dog)|r[e3]dsk[i*]n|z[i*][o0]n[i*]st\s+pig)\b"),
        "block", "body", "blocked", "Hard racial slur (variant)"),

    # ── BLOCK: Hate speech / incitement ──────────────────────────────────────
    CensorRule("genocide_incitement",
        _c(r"\b(exterminate|extermination|ethnic cleansing|final solution|gas the|kill all (jews|blacks|muslims|whites|asians|hispanics))\b"),
        "block", "any", "blocked", "Genocide incitement"),

    CensorRule("targeted_violence",
        _c(r"\b(shoot up|mass shooting at|bomb the|attack the).{0,50}\b(school|church|mosque|synagogue|temple|mall|concert|crowd)\b"),
        "block", "any", "blocked", "Targeted violence incitement"),

    # ── BLOCK: Non-consensual content ────────────────────────────────────────
    CensorRule("non_consensual_explicit",
        _c(r"\b(rape\s+fantasy|rape\s+porn|snuff\s+(porn|film)|necrophilia|bestiality|zoophilia)\b"),
        "block", "any", "blocked", "Non-consensual or illegal sexual content"),

    # ── BLOCK: Drug synthesis ─────────────────────────────────────────────────
    CensorRule("drug_synthesis",
        _c(r"\b(how to (make|cook|synthesize|produce)).{0,50}\b(meth(?:amphetamine)?|heroin|fentanyl|crack cocaine|mdma synthesis)\b"),
        "block", "any", "blocked", "Drug synthesis instructions"),

    # ── WARN: Profanity density ───────────────────────────────────────────────
    CensorRule("high_profanity",
        _c(r"\b(fuck|shit|cunt|ass(?:hole)?|bitch|bastard|cock|dick|pussy)\b"),
        "warn", "body", "flagged", "Profanity detected (warn — density check applied)"),

    # ── WARN: Graphic violence description ───────────────────────────────────
    CensorRule("graphic_violence",
        _c(r"\b(decapitat|dismember|eviscerat|torture|mutilat).{0,100}\b(in detail|described|how to|instructions)\b"),
        "warn", "any", "flagged", "Graphic violence description"),

    # ── WARN: Unverified medical advice ──────────────────────────────────────
    CensorRule("unverified_medical",
        _c(r"\b(cure[sd]?|treat[s]?|heal[s]?|prevent[s]?).{0,50}\b(cancer|diabetes|hiv|aids|covid|autism|dementia)\b"),
        "warn", "any", "flagged", "Potentially unverified medical claim"),

    # ── WARN: Named person + serious allegation ───────────────────────────────
    CensorRule("named_allegation",
        _c(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b.{0,150}\b(raped|murdered|killed|assaulted|abused|molested)\b"),
        "warn", "body", "flagged", "Named person + serious allegation"),

    # ── WARN: Doxxing intent ──────────────────────────────────────────────────
    CensorRule("doxxing_intent",
        _c(r"\b(here is (his|her|their) (address|phone|social security|home address|workplace))\b"),
        "warn", "any", "flagged", "Possible doxxing content"),

    # ── WARN: 4chan-specific artifacts (not blocking, but flag) ───────────────
    CensorRule("anon_formatting",
        _c(r"^(>>?\d{5,}|&gt;&gt;\d{5,})", re.MULTILINE),
        "warn", "body", "flagged", "4chan post quote formatting detected"),

    # ── WARN: Extremist ideology signals ─────────────────────────────────────
    CensorRule("extremist_ideology",
        _c(r"\b(white genocide|great replacement|14\s*words|88(?:\s+heil)?|race war|accelerationism|accelerate the collapse)\b"),
        "warn", "any", "flagged", "Extremist ideology signal"),
]

# Profanity density threshold (warn only if count exceeds this per 100 words)
PROFANITY_PER_100_WORDS_THRESHOLD = 5


class Censor:
    def run(self, story_id: str, title: str, body: str) -> Tuple[bool, List[dict]]:
        """
        Returns (blocked: bool, log_entries: List[dict]).
        blocked=True → story must not proceed.
        """
        blocked = False
        log_entries = []
        profanity_matches = 0
        word_count = max(len(body.split()), 1)

        for rule in CENSOR_RULES:
            fields_to_check = {}
            if rule.field in ("title", "any"):
                fields_to_check["title"] = title
            if rule.field in ("body", "any"):
                fields_to_check["body"] = body

            for field_name, field_value in fields_to_check.items():
                matches = rule.pattern.findall(field_value)
                if not matches:
                    continue

                # Profanity density check — warn only if dense
                if rule.name == "high_profanity":
                    profanity_matches += len(matches)
                    density = (profanity_matches / word_count) * 100
                    if density < PROFANITY_PER_100_WORDS_THRESHOLD:
                        continue

                # 4chan formatting: warn but don't block or count as issue
                if rule.name == "anon_formatting":
                    log_entries.append({
                        "story_id": story_id,
                        "rule_hit": rule.name,
                        "severity": "warn",
                        "field": field_name,
                        "matched": "4chan post format",
                        "action": "flagged",
                    })
                    continue

                matched_text = str(matches[0])[:200] if isinstance(matches[0], str) \
                    else str(matches[0][0])[:200]

                log_entries.append({
                    "story_id": story_id,
                    "rule_hit": rule.name,
                    "severity": rule.severity,
                    "field": field_name,
                    "matched": matched_text,
                    "action": rule.action,
                })

                if rule.severity == "block":
                    blocked = True
                    logger.warning("[censor] BLOCK story=%s rule=%s", story_id, rule.name)
                else:
                    logger.info("[censor] WARN story=%s rule=%s", story_id, rule.name)

        return blocked, log_entries


class CensorWorker(BaseWorker):
    stage_name = "censor"
    next_stage = "format"

    def __init__(self):
        super().__init__()
        self.censor = Censor()

    def process(self, story_id: str, meta: dict) -> dict:
        db = get_db()

        story = db.execute_one(
            "SELECT id, title, body FROM stories WHERE id=%s",
            (story_id,),
        )
        if not story:
            raise ValueError(f"story {story_id} not found")

        blocked, log_entries = self.censor.run(story_id, story["title"], story["body"])

        for entry in log_entries:
            db.execute(
                """
                INSERT INTO censor_log
                    (story_id, rule_hit, severity, field, matched, action)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    entry["story_id"],
                    entry["rule_hit"],
                    entry["severity"],
                    entry["field"],
                    entry["matched"],
                    entry["action"],
                ),
            )

        if blocked:
            block_rules = [e["rule_hit"] for e in log_entries if e["action"] == "blocked"]
            reason = f"censored: {', '.join(block_rules)}"
            db.execute(
                """
                UPDATE stories
                SET pipeline_status='rejected', rejection_reason=%s, updated_at=NOW()
                WHERE id=%s
                """,
                (reason, story_id),
            )
            logger.warning("[censor] BLOCKED story=%s rules=%s", story_id, block_rules)
            return {"success": False, "reason": reason}

        db.execute(
            "UPDATE stories SET pipeline_status='censored', updated_at=NOW() WHERE id=%s",
            (story_id,),
        )
        warn_count = sum(1 for e in log_entries if e["severity"] == "warn")
        logger.info("[censor] story=%s | warns=%d", story_id, warn_count)
        return {"success": True, "meta": {"warn_count": warn_count}}