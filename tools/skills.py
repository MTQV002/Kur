"""Skills Loader — Dynamic prompt injection (like Genie's readSkillFile)"""
import os

SKILLS_DIR = os.path.join(os.path.dirname(__file__), "..", "skills")


def load_skill(skill_name: str) -> str:
    """Load a skill markdown file. Returns empty string if not found."""
    skill_path = os.path.join(SKILLS_DIR, skill_name, "SKILL.md")
    try:
        with open(skill_path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return ""


def list_skills() -> list[str]:
    """List all available skill names."""
    try:
        return [
            d for d in os.listdir(SKILLS_DIR)
            if os.path.isdir(os.path.join(SKILLS_DIR, d))
            and os.path.exists(os.path.join(SKILLS_DIR, d, "SKILL.md"))
        ]
    except FileNotFoundError:
        return []
