# clash_rush.py
"""
Module to compute rush percentages for Clash of Clans player profiles.
Data (caps/thresholds) can be adjusted as needed.
"""

# --- Configuration Data (caps and weights) ---

# Hero max levels by Town Hall (TH7–18) from game updates:
HERO_CAPS = {
    7:  {'BK':10,  'AQ':0,   'GW':0,  'RC':0,  'MP':0},
    8:  {'BK':20,  'AQ':10,  'GW':0,  'RC':0,  'MP':0},
    9:  {'BK':30,  'AQ':30,  'GW':0,  'RC':0,  'MP':0},
    10: {'BK':40,  'AQ':40,  'GW':20, 'RC':0,  'MP':0},
    11: {'BK':50,  'AQ':50,  'GW':40, 'RC':0,  'MP':0},
    12: {'BK':65,  'AQ':65,  'GW':40, 'RC':0,  'MP':0},
    13: {'BK':75,  'AQ':75,  'GW':50, 'RC':25, 'MP':0},
    14: {'BK':80,  'AQ':80,  'GW':55, 'RC':30, 'MP':0},
    15: {'BK':90,  'AQ':90,  'GW':65, 'RC':40, 'MP':0},
    16: {'BK':95,  'AQ':95,  'GW':70, 'RC':45, 'MP':0},
    17: {'BK':100, 'AQ':100, 'GW':75, 'RC':50, 'MP':90},
    18: {'BK':105, 'AQ':105, 'GW':80, 'RC':55, 'MP':95},
}

# Total lab-level caps (troops + spells) by TH (approximate) from reference data:
LAB_CAPS = {
    3:   10,   4:   40,   5:   90,
    6:  160,   7:  250,   8:  360,
    9:  500,  10:  700,  11:  900,
   12: 1100,  13: 1300,  14: 1500,
   15: 1700,  16: 1900,  17: 2100,
   18: 2300
}

# Pet level caps (sum of all pet levels) by TH (sum of all pets at their max level)
# Based on the number of pets unlocked and their max (each ~lvl10):
PET_CAPS = {
   14: 4*10,  # 4 pets @ lvl10
   15: 4*10,
   16: 4*10,
   17: 8*10,  # (4 original + 4 new) @ lvl10 each
   18: 10*10  # (adds 2 more pets) @ lvl10 each
}

# Number of wall pieces by TH (from wiki):
WALL_COUNT = {
   2:25,  3:50,  4:75,  5:100, 6:125, 7:175, 8:225, 9:250,
   10:275, 11:300, 12:300, 13:300, 14:325, 15:325, 16:325, 17:325, 18:325
}
# Max wall level by TH (from wiki):
WALL_LEVEL = {
    2: 1,  3: 2,  4: 3,  5: 4,  6: 5,  7: 6,  8: 7,  9: 8,
    10: 9, 11:10, 12:11, 13:12, 14:13, 15:14, 16:15, 17:16, 18:17
}

# Rush thresholds:
HERO_RUSH_THRESHOLD = 5.0   # in percent (flag if gap >= 5%)
LAB_RUSH_THRESHOLD  = 25.0  # in percent
PET_RUSH_THRESHOLD  = 25.0  # can customize
WALL_RUSH_THRESHOLD = 25.0
BUILD_RUSH_THRESHOLD = 5.0  # (small value, as base should be mostly maxed)

# Weights for aggregate rush score (equipment excluded from rush score):
RUSH_WEIGHT_HERO  = 0.50
RUSH_WEIGHT_LAB   = 0.30
RUSH_WEIGHT_EQUIP = 0.00
RUSH_WEIGHT_PET   = 0.20
RUSH_SCORE_THRESHOLD = 30.0  # % score above which we consider "rushed"

# --- Calculation Functions ---

def calculate_hero_rush(th, hero_levels):
    """Compute hero rush percent for a given TH and hero levels dict.

    Uses TH(n-1) caps so only the *previous* TH's requirements count.
    Clamps current levels to the required cap (over-levelling doesn't
    give negative rush).
    """
    prev_th = th - 1 if th else None
    caps = HERO_CAPS.get(prev_th, {})
    total_cap = sum(caps.values())
    if total_cap <= 0:
        # TH too low for hero tracking
        return {
            "percent": 0.0,
            "required": 0,
            "current": 0,
            "is_rushed": False,
            "counted": False,
        }
    total_have = min(
        sum(hero_levels.get(h, 0) for h in ['BK','AQ','GW','RC','MP']),
        total_cap,
    )
    total_gap = total_cap - total_have
    percent_gap = (total_gap / total_cap) * 100
    rushed = percent_gap >= HERO_RUSH_THRESHOLD
    return {
        "percent": round(percent_gap, 1),
        "required": total_cap,
        "current": total_have,
        "is_rushed": rushed,
        "counted": True,
    }

def calculate_lab_rush(th, lab_total_levels, *, estimated: bool = False):
    """Compute lab rush percent (troops+spells combined) for TH.

    Uses TH(n-1) caps.  Clamps current to the required cap so
    over-levelling cannot produce negative rush.
    """
    prev_th = th - 1 if th else None
    cap = LAB_CAPS.get(prev_th, None)
    if cap is None or cap <= 0:
        return None
    clamped = min(lab_total_levels, cap)
    gap = cap - clamped
    percent_gap = (gap / cap) * 100
    rushed = percent_gap >= LAB_RUSH_THRESHOLD
    return {
        "percent": round(percent_gap, 1),
        "required": cap,
        "current": clamped,
        "is_rushed": rushed,
        "estimated": estimated,
        "counted": True,
    }

def calculate_pet_rush(th, pet_total_levels, *, pets_data_present: bool = True):
    """Compute pet rush percent (sum of all pet levels) for TH.

    Uses TH(n-1) caps.  Returns a non-counted result when:
      - TH < 14 (pets don't exist yet)
      - pets_data_present is False (API didn't return pet data)
      - pet array was empty/missing
    Never defaults missing data to 100% rush.
    """
    prev_th = th - 1 if th else None
    cap = PET_CAPS.get(prev_th, None)

    # No cap means TH too low for pets
    if cap is None or cap <= 0:
        return {
            "percent": 0.0,
            "required": 0,
            "current": 0,
            "is_rushed": False,
            "counted": False,
        }

    # Missing or empty pet data → skip, do NOT treat as 100% rush
    if not pets_data_present or pet_total_levels <= 0:
        return {
            "percent": 0.0,
            "required": cap,
            "current": 0,
            "is_rushed": False,
            "counted": False,
        }

    clamped = min(pet_total_levels, cap)
    gap = cap - clamped
    percent_gap = (gap / cap) * 100
    rushed = percent_gap >= PET_RUSH_THRESHOLD
    return {
        "percent": round(percent_gap, 1),
        "required": cap,
        "current": clamped,
        "is_rushed": rushed,
        "counted": True,
    }

def calculate_wall_rush(th, wall_levels_sum, *, wall_data_present: bool = True):
    """Compute wall rush percent for TH (sum of player's wall levels).

    Uses TH(n-1) caps.  Returns a non-counted / N/A result when wall
    data is missing or incomplete (Supercell API does NOT reliably
    return wall segments).
    """
    prev_th = th - 1 if th else None
    count = WALL_COUNT.get(prev_th, 0)
    level_cap = WALL_LEVEL.get(prev_th, 0)
    total_wall_cap = count * level_cap

    if total_wall_cap <= 0:
        return {
            "percent": 0.0,
            "required": 0,
            "current": 0,
            "is_rushed": False,
            "counted": False,
        }

    # Missing / incomplete wall data → N/A, never assume 0 progress
    if not wall_data_present or wall_levels_sum <= 0:
        return {
            "percent": None,
            "required": total_wall_cap,
            "current": 0,
            "is_rushed": False,
            "counted": False,
        }

    clamped = min(wall_levels_sum, total_wall_cap)
    gap = total_wall_cap - clamped
    percent_gap = (gap / total_wall_cap) * 100
    rushed = percent_gap >= WALL_RUSH_THRESHOLD
    return {
        "percent": round(percent_gap, 1),
        "required": total_wall_cap,
        "current": clamped,
        "is_rushed": rushed,
        "counted": True,
    }

def calculate_equipment_rush(equip_current, equip_max):
    """
    Equipment rush based on ratio of current equipment level sum to max possible.
    equip_current and equip_max are sums of levels of all gear/equipment.

    If no equipment data is available (max == 0), mark as not counted.
    """
    if equip_max <= 0:
        return {
            "percent": 0.0,
            "required": 0,
            "current": 0,
            "is_rushed": False,
            "counted": False,
        }
    clamped = min(equip_current, equip_max)
    gap = equip_max - clamped
    percent_gap = (gap / equip_max) * 100
    return {
        "percent": round(percent_gap, 1),
        "required": equip_max,
        "current": clamped,
        "is_rushed": percent_gap > 0.0,
        "counted": True,
    }

def calculate_building_rush():
    """
    Approximate building rush logic placeholder.
    Typically one would sum levels of all buildings and compare to expected.
    Here we return no rush info (or implement a custom heuristic).
    """
    return {"percent": 0.0, "required": 0, "current": 0, "is_rushed": False}

def weighted_rush_score(rush_dicts):
    """Compute weighted aggregate rush score from individual pillar gaps.

    Only pillars whose result has ``counted=True`` participate.
    If no pillar is counted the score is 0 and ``is_rushed`` is ``False``.
    """
    pillar_weights = {
        'heroes':    RUSH_WEIGHT_HERO,
        'lab':       RUSH_WEIGHT_LAB,
        'pets':      RUSH_WEIGHT_PET,
    }

    total_weight = 0.0
    raw_score = 0.0
    for key, weight in pillar_weights.items():
        result = rush_dicts.get(key)
        if result is None:
            continue
        # Skip pillars with missing / invalid data
        if not result.get('counted', True):
            continue
        pct = result.get('percent')
        if pct is None:
            continue
        raw_score += weight * pct
        total_weight += weight

    if total_weight <= 0:
        return 0.0, False

    # Normalise so that skipped pillars don't deflate the score
    score = raw_score / total_weight * sum(pillar_weights.values())
    # "More than 30%" means strictly greater than the threshold.
    is_rushed = score > RUSH_SCORE_THRESHOLD
    return round(score, 1), is_rushed

def analyze_rush(player_profile):
    """
    Given a player profile (with keys 'townHallLevel', 'heroLevels', 'troopLevels',
    'spellLevels', 'petLevels', 'equipmentLevels', 'wallLevels'), compute all rush metrics.
    Returns a dict with detailed breakdown and summary.
    """
    th = player_profile.get('townHallLevel')
    # HERO RUSH
    hero_levels = {
        'BK': player_profile.get('barbarianKingLevel', 0),
        'AQ': player_profile.get('archerQueenLevel', 0),
        'GW': player_profile.get('grandWardenLevel', 0),
        'RC': player_profile.get('royalChampionLevel', 0),
        'MP': player_profile.get('minionPrinceLevel', 0)
    }
    hero_r = calculate_hero_rush(th, hero_levels)

    # LAB RUSH
    troops_data = player_profile.get('troops', {})
    spells_data = player_profile.get('spells', {})
    lab_total = sum(troops_data.values()) + sum(spells_data.values()) if troops_data or spells_data else 0
    lab_r = calculate_lab_rush(th, lab_total)

    # PET RUSH — guard against missing/empty data
    pets_raw = player_profile.get('pets', {})
    pets_data_present = bool(pets_raw)
    pet_total = sum(pets_raw.values()) if pets_raw else 0
    pet_r = calculate_pet_rush(th, pet_total, pets_data_present=pets_data_present)

    # EQUIPMENT RUSH
    equip_current = sum(player_profile.get('equipment', {}).values())
    equip_max = player_profile.get('equipmentMaxTotal', 0)
    equip_r = calculate_equipment_rush(equip_current, equip_max)

    # WALL RUSH — guard against missing/incomplete data
    walls_raw = player_profile.get('walls', {})
    wall_data_present = bool(walls_raw)
    wall_total = sum(walls_raw.values()) if walls_raw else 0
    wall_r = calculate_wall_rush(th, wall_total, wall_data_present=wall_data_present)

    # BUILDING RUSH (approximate/unimplemented)
    base_r = calculate_building_rush()

    # Weighted score (only counted pillars participate)
    gaps = {'heroes': hero_r, 'lab': lab_r, 'equipment': equip_r, 'pets': pet_r}
    score, is_rushed = weighted_rush_score(gaps)
    severity = 'RUSHED' if is_rushed else 'OK'

    summary = {
        'rushScore': score,
        'is_rushed': is_rushed,
        'severity': severity
    }
    breakdown = {
        'heroes': hero_r,
        'lab': lab_r,
        'pets': pet_r,
        'equipment': equip_r,
        'walls': wall_r,
        'buildings': base_r
    }
    return {'breakdown': breakdown, 'summary': summary}
