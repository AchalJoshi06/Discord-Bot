"""Rush calculations and player analysis logic."""
from typing import Dict, Any, Optional
from datetime import datetime, timezone

from config import (
    HERO_CAPS, LAB_CAPS, BASE_CAPS,
    HERO_RUSH_THRESHOLD, LAB_RUSH_THRESHOLD, BASE_RUSH_THRESHOLD
)


def extract_hero_levels(player_json: Dict[str, Any]) -> Dict[str, int]:
    """Extract hero levels from player JSON."""
    hero_levels = {"BK": 0, "AQ": 0, "GW": 0, "RC": 0}
    
    # Check heroes array
    if isinstance(player_json.get("heroes"), list):
        for h in player_json.get("heroes", []):
            name = (h.get("name") or "").lower()
            lvl = h.get("level") or 0
            try:
                lvl = int(lvl)
            except Exception:
                lvl = 0
            
            if "barbarian king" in name:
                hero_levels["BK"] = lvl
            elif "archer queen" in name:
                hero_levels["AQ"] = lvl
            elif "grand warden" in name:
                hero_levels["GW"] = lvl
            elif "royal champion" in name:
                hero_levels["RC"] = lvl
    
    # Fallback to direct keys
    mapping = {
        "barbarianKingLevel": "BK",
        "archerQueenLevel": "AQ",
        "grandWardenLevel": "GW",
        "royalChampionLevel": "RC",
    }
    for k, code in mapping.items():
        if k in player_json and player_json[k] is not None:
            try:
                hero_levels[code] = int(player_json[k])
            except Exception:
                pass
    
    return hero_levels


def calculate_hero_rush(player_json: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Calculate hero rush percentage.
    
    Returns dict with:
        - percent: Rush percentage
        - counted: Whether it exceeds threshold
        - hero_levels: Dict of hero levels
        - required: Required total hero levels for previous TH
        - current: Current total hero levels
        - missing: Missing hero levels
    """
    th = player_json.get("townHallLevel")
    if th is None:
        return None
    
    try:
        th = int(th)
    except (ValueError, TypeError):
        return None
    
    prev_th = th - 1
    caps = HERO_CAPS.get(prev_th)
    if not caps:
        return None
    
    hero_levels = extract_hero_levels(player_json)
    required_total = sum(caps.values())
    current_total = sum(hero_levels.values())
    missing_total = max(0, required_total - current_total)
    rush_percent = (missing_total / required_total) * 100 if required_total > 0 else 0.0
    counted = rush_percent >= HERO_RUSH_THRESHOLD
    
    return {
        "percent": round(rush_percent, 2),
        "counted": counted,
        "hero_levels": hero_levels,
        "required": required_total,
        "current": current_total,
        "missing": missing_total
    }


def extract_lab_total(player_json: Dict[str, Any]) -> int:
    """Extract total lab levels (troops + spells + pets)."""
    total = 0
    for key in ("troops", "spells", "pets"):
        if isinstance(player_json.get(key), list):
            for item in player_json.get(key, []):
                lvl = item.get("level") or 0
                try:
                    total += int(lvl)
                except Exception:
                    pass
    return total


def calculate_lab_rush(player_json: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Calculate lab rush percentage.
    
    Returns dict with:
        - percent: Rush percentage
        - counted: Whether it exceeds threshold
        - required: Required total for previous TH
        - current: Current total
        - missing: Missing levels
    """
    th = player_json.get("townHallLevel")
    if th is None:
        return None
    
    try:
        th = int(th)
    except (ValueError, TypeError):
        return None
    
    prev_th = th - 1
    caps = LAB_CAPS.get(prev_th)
    if not caps:
        return None
    
    required = caps.get("total", 0)
    current = extract_lab_total(player_json)
    missing = max(0, required - current)
    percent = (missing / required) * 100 if required > 0 else 0.0
    counted = percent >= LAB_RUSH_THRESHOLD
    
    return {
        "percent": round(percent, 2),
        "counted": counted,
        "required": required,
        "current": current,
        "missing": missing
    }


def calculate_base_rush(player_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculate base rush (currently returns N/A as base calculation is complex).
    """
    th = player_json.get("townHallLevel")
    if th is None:
        return {"status": "N/A"}
    
    try:
        th = int(th)
    except (ValueError, TypeError):
        return {"status": "N/A"}
    
    prev_th = th - 1
    caps = BASE_CAPS.get(prev_th)
    if not caps:
        return {"status": "N/A"}
    
    return {"status": "N/A", "required": caps.get("total", 0)}


def analyze_player_for_kick(
    player: Dict[str, Any],
    war_data: Optional[Dict[str, Any]] = None,
    clan_tag: Optional[str] = None
) -> Dict[str, Any]:
    """
    Analyze a player to determine if they should be considered for kicking.
    
    Returns dict with:
        - should_kick: bool
        - reasons: List of reason strings
        - score: int (higher = more problematic)
        - details: Dict with specific metrics
    """
    reasons = []
    score = 0
    details = {}
    
    # Check hero rush
    hero_rush = calculate_hero_rush(player)
    if hero_rush and hero_rush["counted"]:
        reasons.append(f"Hero rush: {hero_rush['percent']:.1f}% (missing {hero_rush['missing']} levels)")
        score += 3
        details["hero_rush"] = hero_rush
    
    # Check lab rush
    lab_rush = calculate_lab_rush(player)
    if lab_rush and lab_rush["counted"]:
        reasons.append(f"Lab rush: {lab_rush['percent']:.1f}% (missing {lab_rush['missing']} levels)")
        score += 2
        details["lab_rush"] = lab_rush
    
    # Check war participation
    if war_data and war_data.get("state") == "inWar":
        clan_members = (war_data.get("clan") or {}).get("members") or []
        player_tag = player.get("tag")
        war_member = next((m for m in clan_members if m.get("tag") == player_tag), None)
        
        if war_member:
            attacks = war_member.get("attacks", [])
            if len(attacks) == 0:
                reasons.append("No war attacks used")
                score += 5
                details["war_attacks"] = 0
            elif len(attacks) < 2:
                reasons.append(f"Only {len(attacks)}/2 war attacks used")
                score += 2
                details["war_attacks"] = len(attacks)
    
    # Check donation ratio
    donations = player.get("donations", 0)
    received = player.get("donationsReceived", 0)
    if received > 0:
        ratio = donations / received
        if ratio < 0.3:  # Very low donator
            reasons.append(f"Low donation ratio: {ratio:.2f} ({donations}/{received})")
            score += 1
            details["donation_ratio"] = ratio
    elif donations == 0 and received > 100:
        reasons.append(f"No donations, received {received}")
        score += 1
        details["donation_ratio"] = 0
    
    # Check war stars (very low might indicate inactivity)
    war_stars = player.get("warStars", 0)
    if war_stars < 100:  # Very low war stars
        reasons.append(f"Low war stars: {war_stars}")
        score += 1
        details["war_stars"] = war_stars
    
    return {
        "should_kick": len(reasons) > 0,
        "reasons": reasons,
        "score": score,
        "details": details
    }

