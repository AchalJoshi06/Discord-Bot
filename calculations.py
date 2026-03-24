"""Rush calculations and player analysis logic.

v3.0 — Uses clash_rush module for enhanced multi-pillar rush analysis.
Formula: RushScore = 0.50·H + 0.30·L + 0.20·P
"""
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, timezone

from config import (
    HERO_CAPS, LAB_CAPS, BASE_CAPS,
    HERO_RUSH_THRESHOLD, LAB_RUSH_THRESHOLD, BASE_RUSH_THRESHOLD,
)
import clash_rush
from storage import load_war_player_stats, load_raid_history

# Re-export thresholds/weights from clash_rush for any code that imports from here
PET_CAPS = clash_rush.PET_CAPS
RUSH_SCORE_THRESHOLD: float = clash_rush.RUSH_SCORE_THRESHOLD
RUSH_WEIGHT_HERO: float = clash_rush.RUSH_WEIGHT_HERO
RUSH_WEIGHT_LAB: float = clash_rush.RUSH_WEIGHT_LAB
RUSH_WEIGHT_EQUIP: float = clash_rush.RUSH_WEIGHT_EQUIP
RUSH_WEIGHT_PET: float = clash_rush.RUSH_WEIGHT_PET


def extract_hero_levels(player_json: Dict[str, Any]) -> Dict[str, int]:
    """Extract hero levels from player JSON, including Minion Prince."""
    hero_levels = {"BK": 0, "AQ": 0, "GW": 0, "RC": 0, "MP": 0}
    
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
            elif "minion prince" in name:
                hero_levels["MP"] = lvl
    
    # Fallback to direct keys
    mapping = {
        "barbarianKingLevel": "BK",
        "archerQueenLevel": "AQ",
        "grandWardenLevel": "GW",
        "royalChampionLevel": "RC",
        "minionPrinceLevel": "MP",
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
    Calculate hero rush percentage using clash_rush module.
    Compares against previous TH caps (TH n-1).

    Returns dict with:
        - percent: Rush percentage
        - counted / is_rushed: Whether it exceeds threshold
        - hero_levels: Dict of hero levels
        - required: Required total hero levels
        - current: Current total hero levels (clamped)
        - missing: Missing hero levels
    """
    th = player_json.get("townHallLevel")
    if th is None:
        return None
    
    try:
        th = int(th)
    except (ValueError, TypeError):
        return None
    
    hero_levels = extract_hero_levels(player_json)
    result = clash_rush.calculate_hero_rush(th, hero_levels)
    
    return {
        "percent": result["percent"],
        "counted": result.get("counted", result["is_rushed"]),
        "is_rushed": result["is_rushed"],
        "hero_levels": hero_levels,
        "required": result["required"],
        "current": result["current"],
        "missing": max(0, result["required"] - result["current"]),
    }


def extract_lab_total(player_json: Dict[str, Any]) -> int:
    """Extract total lab levels (troops + spells only, pets excluded)."""
    total = 0
    for key in ("troops", "spells"):
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
    Calculate lab rush percentage using clash_rush module.
    Compares troops + spells against previous TH caps (TH n-1).
    Clamps current total to the required cap.

    Returns dict with:
        - percent: Rush percentage
        - counted / is_rushed: Whether it exceeds threshold
        - required: Required total for previous TH
        - current: Current total (clamped)
        - missing: Missing levels
        - estimated: True if lab data looks incomplete
    """
    th = player_json.get("townHallLevel")
    if th is None:
        return None
    
    try:
        th = int(th)
    except (ValueError, TypeError):
        return None
    
    lab_total = extract_lab_total(player_json)

    # Heuristic: if the API returned zero troops+spells at TH>3 the data
    # is almost certainly incomplete.
    estimated = False
    troops_list = player_json.get("troops")
    spells_list = player_json.get("spells")
    if th > 3 and lab_total == 0 and not troops_list and not spells_list:
        estimated = True

    result = clash_rush.calculate_lab_rush(th, lab_total, estimated=estimated)
    if result is None:
        return None
    
    return {
        "percent": result["percent"],
        "counted": result.get("counted", result["is_rushed"]),
        "is_rushed": result["is_rushed"],
        "required": result["required"],
        "current": result["current"],
        "missing": max(0, result["required"] - result["current"]),
        "estimated": result.get("estimated", False),
    }


def calculate_base_rush(player_json: Dict[str, Any]) -> Dict[str, Any]:
    """Estimate base rush using aggregate building levels vs previous TH base cap.

    The API does not always expose detailed building levels. When no reliable
    building-total signal is present, returns ``counted=False``.
    """
    th_raw = player_json.get("townHallLevel")
    try:
        th = int(th_raw or 0)
    except (ValueError, TypeError):
        th = 0

    if th <= 1:
        return {
            "status": "N/A",
            "counted": False,
            "is_rushed": False,
            "percent": 0.0,
            "required": 0,
            "current": 0,
            "missing": 0,
        }

    prev_th = max(1, th - 1)
    required = int((BASE_CAPS.get(prev_th) or {}).get("total", 0) or 0)
    if required <= 0:
        return {
            "status": "N/A",
            "counted": False,
            "is_rushed": False,
            "percent": 0.0,
            "required": 0,
            "current": 0,
            "missing": 0,
        }

    current_total = 0
    data_present = False

    # Preferred source: explicit aggregate field.
    if "baseTotalLevels" in player_json:
        try:
            current_total = max(0, int(player_json.get("baseTotalLevels", 0) or 0))
            data_present = True
        except (ValueError, TypeError):
            pass

    # Fallback: custom buildings payload from snapshots/integrations.
    if not data_present:
        buildings = player_json.get("buildings")
        if isinstance(buildings, dict):
            if "total" in buildings:
                try:
                    current_total = max(0, int(buildings.get("total", 0) or 0))
                    data_present = True
                except (ValueError, TypeError):
                    pass
            else:
                subtotal = 0
                for value in buildings.values():
                    try:
                        if isinstance(value, dict):
                            subtotal += int(value.get("level", 0) or 0)
                        else:
                            subtotal += int(value or 0)
                    except (ValueError, TypeError):
                        continue
                if subtotal > 0:
                    current_total = subtotal
                    data_present = True
        elif isinstance(buildings, list):
            subtotal = 0
            for row in buildings:
                if not isinstance(row, dict):
                    continue
                try:
                    subtotal += int(row.get("level", 0) or 0)
                except (ValueError, TypeError):
                    continue
            if subtotal > 0:
                current_total = subtotal
                data_present = True

    if not data_present:
        return {
            "status": "N/A",
            "counted": False,
            "is_rushed": False,
            "percent": 0.0,
            "required": required,
            "current": 0,
            "missing": required,
        }

    clamped_current = min(required, current_total)
    missing = max(0, required - clamped_current)
    rush_pct = round((missing / required) * 100.0, 2)

    if rush_pct <= 20:
        status = "OK"
    elif rush_pct <= 50:
        status = "Semi-Rushed"
    else:
        status = "Rushed"

    return {
        "status": status,
        "counted": True,
        "is_rushed": rush_pct > float(BASE_RUSH_THRESHOLD),
        "percent": rush_pct,
        "required": required,
        "current": clamped_current,
        "missing": missing,
    }


def analyze_player_for_kick(
    player: Dict[str, Any],
    war_data: Optional[Dict[str, Any]] = None,
    clan_tag: Optional[str] = None
) -> Dict[str, Any]:
    """
    Analyze a player to determine if they should be considered for kicking.
    Uses the same weighted rush score as embeds for consistency.
    
    Returns dict with:
        - should_kick: bool
        - reasons: List of reason strings
        - score: int (higher = more problematic)
        - details: Dict with specific metrics
    """
    reasons = []
    score = 0
    details = {}
    
    # Use weighted rush score (same as embeds) for a single consistent check
    rush = calculate_weighted_rush_score(player)
    if rush and rush["is_rushed"]:
        reasons.append(f"Rush score: {rush['score']:.1f}% (heroes {rush['hero_gap']}%)")
        score += 3
        details["rush"] = rush
    
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


def calculate_building_rush(
    player_buildings: Dict[str, int],
    max_buildings: Dict[int, Dict[str, int]],
    town_hall: int,
) -> Tuple[float, str]:
    """Calculate building rush percentage and status."""
    if town_hall not in max_buildings:
        return 0.0, "N/A"

    total_max = 0
    total_current = 0

    for building, max_level in max_buildings[town_hall].items():
        current_level = player_buildings.get(building)

        # Skip missing buildings safely
        if current_level is None:
            continue

        total_max += max_level
        total_current += current_level

    if total_max == 0:
        return 0.0, "N/A"

    rush_pct = round(((total_max - total_current) / total_max) * 100, 2)

    # Status labels
    if rush_pct <= 20:
        status = "OK"
    elif rush_pct <= 50:
        status = "Semi-Rushed"
    else:
        status = "Rushed"

    return rush_pct, status


# ════════════════════════════════════════════
# Enhanced rush scoring (v3.0 — clash_rush)
# ════════════════════════════════════════════

def extract_pet_total(player_json: Dict[str, Any]) -> Tuple[int, bool]:
    """Sum of all pet levels, excluding Minion Prince (moved to heroes).

    Returns ``(total, data_present)`` where *data_present* is ``True``
    only when the API actually provided a non-empty pets list.
    """
    pets_list = player_json.get("pets") or []
    if not isinstance(pets_list, list) or len(pets_list) == 0:
        return 0, False
    total = 0
    for pet in pets_list:
        name = (pet.get("name") or "").lower()
        if "minion prince" in name:
            continue
        try:
            total += int(pet.get("level", 0))
        except (ValueError, TypeError):
            pass
    return total, True


def extract_equipment_totals(
    player_json: Dict[str, Any],
) -> Tuple[int, int]:
    """Return ``(current_total, max_total)`` from the ``heroEquipment`` array.

    The API provides ``maxLevel`` per equipment piece, so we don't need a
    per-TH caps table — it adjusts automatically.
    """
    current_total = 0
    max_total = 0
    for eq in player_json.get("heroEquipment", []) or []:
        lvl = eq.get("level") or 0
        max_lvl = eq.get("maxLevel") or 0
        try:
            current_total += int(lvl)
            max_total += int(max_lvl)
        except (ValueError, TypeError):
            pass
    return current_total, max_total


def extract_equipment_offenders(player_json: Dict[str, Any], top_n: int = 3) -> List[Dict[str, Any]]:
    """Return most-behind hero equipment pieces sorted by level gap.

    Each item includes: name, level, max_level, gap, progress_pct.
    """
    offenders: List[Dict[str, Any]] = []
    for eq in player_json.get("heroEquipment", []) or []:
        if not isinstance(eq, dict):
            continue
        try:
            level = int(eq.get("level", 0) or 0)
            max_level = int(eq.get("maxLevel", 0) or 0)
        except (ValueError, TypeError):
            continue
        if max_level <= 0:
            continue
        gap = max(0, max_level - level)
        if gap <= 0:
            continue

        pct = round((level / max_level) * 100.0, 2)
        offenders.append(
            {
                "name": str(eq.get("name", "Unknown Equipment")),
                "level": level,
                "max_level": max_level,
                "gap": gap,
                "progress_pct": pct,
            }
        )

    offenders.sort(key=lambda x: (x["gap"], -x["progress_pct"]), reverse=True)
    return offenders[:max(1, int(top_n or 3))]


def extract_wall_total(player_json: Dict[str, Any]) -> Tuple[int, bool]:
    """Sum of all wall segment levels from the player's building list.

    Returns ``(total, data_present)`` where *data_present* is ``True``
    only when reliable wall data was found.

    The CoC API doesn't reliably expose wall segments, so we must be
    careful not to treat missing data as 0 progress.
    """
    total = 0
    found = False

    # Check dedicated walls key (custom pre-processed)
    walls = player_json.get("walls")
    if isinstance(walls, list) and len(walls) > 0:
        for w in walls:
            try:
                total += int(w.get("level", 0))
            except (ValueError, TypeError):
                pass
        return total, True
    if isinstance(walls, dict) and walls:
        return sum(walls.values()), True

    # Check buildings list for wall entries
    for b in player_json.get("buildings", []) or []:
        if "wall" in (b.get("name") or "").lower():
            try:
                total += int(b.get("level", 0))
                found = True
            except (ValueError, TypeError):
                pass

    return total, found


def calculate_weighted_rush_score(
    player_json: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Calculate a weighted aggregate rush score across four pillars using clash_rush.

    Formula (only counted pillars participate, re-normalised)::

        RushScore = (W_H · hero_gap%) + (W_L · lab_gap%) +
                    (W_P · pet_gap%)

    Uses TH(n-1) caps.  Clamps current values to required caps.
    Missing / empty API data for pets or walls → excluded from aggregate.
    Returns ``None`` for TH < 7 or missing TH data.
    """
    th = player_json.get("townHallLevel")
    if th is None:
        return None
    try:
        th = int(th)
    except (ValueError, TypeError):
        return None
    if th < 7:
        return None

    # ── Hero gap (calculate ONCE, reuse everywhere) ──
    hero_levels = extract_hero_levels(player_json)
    hero_r = clash_rush.calculate_hero_rush(th, hero_levels)
    hero_gap_pct = max(0.0, hero_r["percent"])

    # ── Lab gap (clamped) ──
    lab_total = extract_lab_total(player_json)
    troops_list = player_json.get("troops")
    spells_list = player_json.get("spells")
    lab_estimated = th > 3 and lab_total == 0 and not troops_list and not spells_list
    lab_r = clash_rush.calculate_lab_rush(th, lab_total, estimated=lab_estimated)
    lab_gap_pct = max(0.0, lab_r["percent"]) if lab_r else 0.0

    # ── Equipment gap (dynamic from API maxLevel) ──
    eq_current, eq_max = extract_equipment_totals(player_json)
    eq_offenders = extract_equipment_offenders(player_json, top_n=3)
    eq_r = clash_rush.calculate_equipment_rush(eq_current, eq_max)
    eq_gap_pct = max(0.0, eq_r["percent"])

    # ── Pet gap (skip if data missing) ──
    pet_total, pets_data_present = extract_pet_total(player_json)
    pet_r = clash_rush.calculate_pet_rush(th, pet_total, pets_data_present=pets_data_present)
    pet_gap_pct = max(0.0, pet_r["percent"]) if pet_r and pet_r.get("counted") else 0.0

    # ── Wall gap (skip if data missing) ──
    wall_total, wall_data_present = extract_wall_total(player_json)
    wall_r = clash_rush.calculate_wall_rush(th, wall_total, wall_data_present=wall_data_present)
    wall_gap_pct = (
        max(0.0, wall_r["percent"])
        if wall_r and wall_r.get("counted") and wall_r["percent"] is not None
        else None
    )

    # Exclude low-confidence synthetic lab estimates from the weighted score.
    if lab_r and lab_estimated:
        lab_r = dict(lab_r)
        lab_r["counted"] = False

    # ── Weighted score (only counted pillars) ──
    pillars = {
        'heroes': hero_r,
        'lab': lab_r if lab_r else {"percent": 0.0, "counted": False},
        'pets': pet_r,
    }
    score, is_rushed = clash_rush.weighted_rush_score(pillars)

    # Coverage/confidence: rush classification should be stricter when fewer pillars are trusted.
    included_pillars: List[str] = []
    for key, result in pillars.items():
        if not isinstance(result, dict):
            continue
        if not bool(result.get("counted", True)):
            continue
        if result.get("percent") is None:
            continue
        included_pillars.append(key)

    coverage_pct = round((len(included_pillars) / 3.0) * 100.0, 1)
    if coverage_pct >= 75.0:
        confidence = "High"
    elif coverage_pct >= 50.0:
        confidence = "Medium"
    else:
        confidence = "Low"

    # When confidence is low, avoid over-flagging from sparse/incomplete API data.
    if confidence == "Low" and score < 45.0:
        is_rushed = False

    if score <= 2.0:
        severity = "Maxed"
    elif score <= 12.0:
        severity = "Healthy"
    elif score <= 25.0:
        severity = "Watch"
    elif score <= 40.0:
        severity = "Rushed"
    else:
        severity = "Critical"

    return {
        "score": round(score, 2),
        "hero_gap": round(hero_gap_pct, 2),
        "lab_gap": round(lab_gap_pct, 2) if lab_r and bool(lab_r.get("counted", True)) else "N/A",
        "equipment_gap": round(eq_gap_pct, 2),
        "pet_gap": round(pet_gap_pct, 2) if pet_r and pet_r.get("counted") else "N/A",
        "wall_gap": round(wall_gap_pct, 2) if wall_gap_pct is not None else "N/A",
        "is_rushed": is_rushed,
        "severity": severity,
        "confidence": confidence,
        "data_coverage": coverage_pct,
        "included_pillars": included_pillars,
        "lab_estimated": lab_estimated,
        "pets_counted": bool(pet_r and pet_r.get("counted")),
        "walls_counted": bool(wall_r and wall_r.get("counted")),
        "equipment_counted": False,
        "equipment_offenders": eq_offenders,
        "breakdown": {
            "heroes": {"current": hero_r["current"], "required": hero_r["required"]},
            "lab": {"current": lab_r["current"] if lab_r else 0, "required": lab_r["required"] if lab_r else 0},
            "equipment": {"current": eq_current, "max": eq_max},
            "pets": {"current": pet_total, "required": pet_r["required"] if pet_r else 0},
            "walls": {"current": wall_total, "required": wall_r["required"] if wall_r else 0},
        },
        # Cached hero result for reuse by callers (embed, kick, etc.)
        "_hero_result": hero_r,
    }


def calculate_donation_ratio_score(donations: int, received: int) -> float:
    """Convert donation ratio into a 0-100 score."""
    try:
        donated = max(0, int(donations or 0))
        recv = max(0, int(received or 0))
    except (ValueError, TypeError):
        return 0.0

    if recv <= 0:
        return 100.0 if donated > 0 else 0.0

    ratio = donated / recv
    return round(min(100.0, ratio * 100.0), 2)


def calculate_activity_score(
    player_json: Dict[str, Any],
    war_attack_rate_pct: Optional[float] = None,
    raid_completion_rate_pct: Optional[float] = None,
) -> Dict[str, Any]:
    """Compute a 0-100 activity score from war, raid, and donation behavior.

    Formula:
    ActivityScore = 0.40*war_attack_rate + 0.35*raid_completion_rate + 0.25*donation_ratio_score
    """
    if war_attack_rate_pct is None:
        try:
            war_stars = int(player_json.get("warStars", 0) or 0)
        except (ValueError, TypeError):
            war_stars = 0
        war_attack_rate_pct = min(100.0, (war_stars / 200.0) * 100.0)

    if raid_completion_rate_pct is None:
        raid_completion_rate_pct = 0.0

    donation_ratio_score = calculate_donation_ratio_score(
        player_json.get("donations", 0),
        player_json.get("donationsReceived", 0),
    )

    war_component = 0.40 * max(0.0, min(100.0, float(war_attack_rate_pct)))
    raid_component = 0.35 * max(0.0, min(100.0, float(raid_completion_rate_pct)))
    donation_component = 0.25 * donation_ratio_score
    score = round(war_component + raid_component + donation_component, 2)

    return {
        "score": score,
        "war_attack_rate": round(float(war_attack_rate_pct), 2),
        "raid_completion_rate": round(float(raid_completion_rate_pct), 2),
        "donation_ratio_score": donation_ratio_score,
    }


def calculate_clan_health_score(
    player_rows: List[Dict[str, Any]],
    war_win_rate_pct: float,
    raid_completion_rate_pct: float,
) -> Dict[str, Any]:
    """Compute clan health as a single 0-100 score.

    Inputs:
    - player_rows: list of player payloads from API
    - war_win_rate_pct: last-N war win-rate percent for the clan
    - raid_completion_rate_pct: recent raid completion percent for the clan

    Blend:
    - 30% average activity score
    - 25% war win-rate
    - 20% raid completion rate
    - 15% average donation ratio quality
    - 10% percentage of non-rushed members
    """
    players = [p for p in (player_rows or []) if isinstance(p, dict)]

    activity_scores: List[float] = []
    donation_scores: List[float] = []
    unrushed_count = 0
    rushed_counted = 0

    for p in players:
        activity = calculate_activity_score(
            p,
            raid_completion_rate_pct=raid_completion_rate_pct,
        )
        activity_scores.append(float(activity.get("score", 0.0) or 0.0))

        donation_scores.append(
            calculate_donation_ratio_score(
                p.get("donations", 0),
                p.get("donationsReceived", 0),
            )
        )

        rush = calculate_weighted_rush_score(p)
        if isinstance(rush, dict):
            rushed_counted += 1
            if not bool(rush.get("is_rushed", False)):
                unrushed_count += 1

    avg_activity = sum(activity_scores) / len(activity_scores) if activity_scores else 0.0
    avg_donation = sum(donation_scores) / len(donation_scores) if donation_scores else 0.0
    non_rushed_pct = (unrushed_count / rushed_counted) * 100.0 if rushed_counted else 50.0

    war_win = max(0.0, min(100.0, float(war_win_rate_pct or 0.0)))
    raid_completion = max(0.0, min(100.0, float(raid_completion_rate_pct or 0.0)))

    health = round(
        0.30 * avg_activity +
        0.25 * war_win +
        0.20 * raid_completion +
        0.15 * avg_donation +
        0.10 * non_rushed_pct,
        2,
    )

    if health >= 80:
        tier = "Strong"
    elif health >= 65:
        tier = "Stable"
    elif health >= 50:
        tier = "At Risk"
    else:
        tier = "Critical"

    return {
        "score": health,
        "tier": tier,
        "avg_activity": round(avg_activity, 2),
        "war_win_rate": round(war_win, 2),
        "raid_completion_rate": round(raid_completion, 2),
        "avg_donation_ratio_score": round(avg_donation, 2),
        "non_rushed_pct": round(non_rushed_pct, 2),
        "member_count": len(players),
    }


def suggest_promotion(player_json: Dict[str, Any]) -> Dict[str, Any]:
    """Suggest promotion readiness for a member.

    Heuristic score out of 100 using:
    - activity score (40%)
    - low rush score quality (25%)
    - donation ratio quality (15%)
    - war reliability proxy (10%)
    - TH progression (10%)
    """
    activity = calculate_activity_score(player_json)
    activity_score = float(activity.get("score", 0.0))

    rush = calculate_weighted_rush_score(player_json)
    rush_score = float(rush.get("score", 100.0) if rush else 100.0)
    unrush_quality = max(0.0, 100.0 - min(100.0, rush_score))

    try:
        th_level = int(player_json.get("townHallLevel", 0) or 0)
    except (ValueError, TypeError):
        th_level = 0
    th_quality = min(100.0, (th_level / 16.0) * 100.0)

    try:
        war_stars = int(player_json.get("warStars", 0) or 0)
    except (ValueError, TypeError):
        war_stars = 0
    war_reliability = min(100.0, (war_stars / 1200.0) * 100.0)

    donation_quality = calculate_donation_ratio_score(
        player_json.get("donations", 0),
        player_json.get("donationsReceived", 0),
    )

    try:
        donations = int(player_json.get("donations", 0) or 0)
        received = int(player_json.get("donationsReceived", 0) or 0)
    except (ValueError, TypeError):
        donations = 0
        received = 0
    donation_ratio = float(donations) / float(max(1, received))

    readiness = round(
        0.40 * activity_score +
        0.25 * unrush_quality +
        0.15 * donation_quality +
        0.10 * war_reliability +
        0.10 * th_quality,
        2,
    )

    if readiness >= 82:
        tier = "High"
    elif readiness >= 68:
        tier = "Medium"
    else:
        tier = "Low"

    blockers: List[str] = []
    if activity_score < 55:
        blockers.append("Low activity")
    if rush_score > 35:
        blockers.append("High rush score")
    if donation_ratio < 0.8:
        blockers.append("Low donation ratio")

    reasons = [
        f"Activity {activity_score:.1f}/100",
        f"Rush {rush_score:.1f}%",
        f"TH {th_level}",
        f"Donation ratio {donation_ratio:.2f}x",
        f"War reliability {war_reliability:.1f}/100",
    ]

    return {
        "readiness": readiness,
        "tier": tier,
        "activity_score": activity_score,
        "rush_score": rush_score,
        "th_level": th_level,
        "donation_quality": donation_quality,
        "donation_ratio": donation_ratio,
        "war_reliability": war_reliability,
        "reasons": reasons,
        "blockers": blockers,
    }


def _raid_full_streak(clan_tag: str, player_tag: str) -> int:
    data = load_raid_history()
    if not isinstance(data, dict):
        return 0
    clan_data = data.get(clan_tag, {})
    if not isinstance(clan_data, dict) or not clan_data:
        return 0

    weekends = sorted(clan_data.items(), key=lambda x: x[0])
    streak = 0
    for _, weekend in reversed(weekends):
        members = weekend.get("members", {})
        row = members.get(player_tag) if isinstance(members, dict) else None
        if not isinstance(row, dict):
            break
        used = int(row.get("attacks", 0) or 0)
        limit = int(row.get("limit", 6) or 6)
        if limit > 0 and used >= limit:
            streak += 1
        else:
            break
    return streak


def calculate_player_streaks(player_tag: str, clan_tags: Optional[List[str]] = None) -> Dict[str, Any]:
    """Return current war participation and raid full-completion streaks.

    War streak is sourced from ``war_player_stats`` rows using
    ``participation_streak`` when available (fallback heuristic when absent).
    Raid streak is computed from trailing full-completion weekends in
    ``raid_history``.
    """
    tag = str(player_tag or "").upper()
    if not tag:
        return {
            "war_participation_streak": 0,
            "raid_full_streak": 0,
            "war_clan_tag": None,
            "raid_clan_tag": None,
        }

    scoped = {str(t).upper() for t in (clan_tags or []) if t}

    war_data = load_war_player_stats()
    best_war = 0
    best_war_clan = None
    if isinstance(war_data, dict):
        for clan_tag, rows in war_data.items():
            ctag = str(clan_tag).upper()
            if scoped and ctag not in scoped:
                continue
            if not isinstance(rows, dict):
                continue
            row = rows.get(tag)
            if not isinstance(row, dict):
                continue

            if "participation_streak" in row:
                streak = int(row.get("participation_streak", 0) or 0)
            else:
                missed_streak = int(row.get("missed_streak", 0) or 0)
                if missed_streak > 0:
                    streak = 0
                else:
                    # Legacy fallback before explicit participation_streak existed.
                    streak = int(row.get("wars_participated", 0) or 0)

            if streak > best_war:
                best_war = streak
                best_war_clan = clan_tag

    raid_data = load_raid_history()
    best_raid = 0
    best_raid_clan = None
    if isinstance(raid_data, dict):
        for clan_tag in raid_data.keys():
            ctag = str(clan_tag).upper()
            if scoped and ctag not in scoped:
                continue
            streak = _raid_full_streak(clan_tag, tag)
            if streak > best_raid:
                best_raid = streak
                best_raid_clan = clan_tag

    return {
        "war_participation_streak": best_war,
        "raid_full_streak": best_raid,
        "war_clan_tag": best_war_clan,
        "raid_clan_tag": best_raid_clan,
    }


def estimate_progression_speed(player_json: Dict[str, Any]) -> Dict[str, Any]:
    """Estimate TH progression timeline from current progress and lifetime activity proxies.

    This is a heuristic estimate intended for leadership context, not an exact history.
    """
    try:
        th = int(player_json.get("townHallLevel", 0) or 0)
    except (ValueError, TypeError):
        th = 0
    if th <= 1:
        return {
            "available": False,
            "reason": "missing_th",
            "timeline_lines": [],
        }

    prev_th = max(1, th - 1)
    hero_caps = HERO_CAPS.get(prev_th, {}) if isinstance(HERO_CAPS.get(prev_th, {}), dict) else {}
    hero_levels = extract_hero_levels(player_json)

    hero_required = 0
    hero_current = 0
    for code, cap in hero_caps.items():
        try:
            cap_int = max(0, int(cap or 0))
        except (ValueError, TypeError):
            cap_int = 0
        if cap_int <= 0:
            continue
        hero_required += cap_int
        hero_current += min(cap_int, int(hero_levels.get(code, 0) or 0))
    hero_progress = (hero_current / hero_required) if hero_required > 0 else min(1.0, th / 16.0)

    lab_cap_raw = LAB_CAPS.get(prev_th, 0)
    lab_required = int(lab_cap_raw.get("total", 0) if isinstance(lab_cap_raw, dict) else (lab_cap_raw or 0))
    lab_current_raw = extract_lab_total(player_json)
    lab_current = min(max(0, int(lab_current_raw or 0)), max(0, lab_required))
    lab_progress = (lab_current / lab_required) if lab_required > 0 else min(1.0, th / 16.0)

    th_progress = min(1.0, max(0.0, th / 16.0))

    achievements = player_json.get("achievements", []) or []

    def _achievement_value(name: str) -> int:
        for row in achievements:
            if not isinstance(row, dict):
                continue
            if str(row.get("name", "")).strip().lower() == name.strip().lower():
                try:
                    return int(row.get("value", 0) or 0)
                except (ValueError, TypeError):
                    return 0
        return 0

    war_stars_total = int(player_json.get("warStars", 0) or 0)
    lifetime_donations = _achievement_value("Friend in Need")
    if lifetime_donations <= 0:
        lifetime_donations = int(player_json.get("donations", 0) or 0)

    war_proxy = min(1.0, max(0.0, war_stars_total / 3000.0))
    donation_proxy = min(1.0, max(0.0, lifetime_donations / 2_000_000.0))
    experience_proxy = (0.6 * war_proxy) + (0.4 * donation_proxy)

    progression_score = (0.45 * th_progress) + (0.30 * hero_progress) + (0.25 * lab_progress)
    maturity_score = (0.65 * progression_score) + (0.35 * experience_proxy)

    total_months = int(round(6 + (maturity_score * 66)))
    total_months = max(4, min(84, total_months))

    current_th_readiness = max(0.0, min(1.0, (hero_progress + lab_progress) / 2.0))
    weights: List[float] = []
    for lvl in range(1, th + 1):
        stage_weight = 1.0 + ((lvl / float(th)) * 1.8)
        if lvl == th:
            stage_weight *= (0.8 + (0.8 * current_th_readiness))
        weights.append(stage_weight)

    total_weight = sum(weights) or 1.0
    timeline: List[Dict[str, Any]] = []
    for idx, lvl in enumerate(range(1, th + 1)):
        months = round((weights[idx] / total_weight) * total_months, 1)
        timeline.append({"th": lvl, "months": months})

    recent = timeline[-5:]
    timeline_lines = [f"TH{row['th']}: ~{row['months']:.1f} mo" for row in recent]

    avg_months_per_th = total_months / max(1, th - 1)
    if avg_months_per_th < 2.2:
        pace_label = "Fast"
    elif avg_months_per_th < 3.4:
        pace_label = "Balanced"
    else:
        pace_label = "Patient"

    return {
        "available": True,
        "current_th": th,
        "estimated_total_months": total_months,
        "avg_months_per_th": round(avg_months_per_th, 2),
        "pace": pace_label,
        "hero_progress_pct": round(hero_progress * 100.0, 1),
        "lab_progress_pct": round(lab_progress * 100.0, 1),
        "war_stars_proxy": war_stars_total,
        "donations_proxy": lifetime_donations,
        "timeline": timeline,
        "timeline_lines": timeline_lines,
    }

