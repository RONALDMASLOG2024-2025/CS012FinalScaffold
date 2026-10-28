"""
WiCare sensing pipeline for the local cluster.

One transmitter builds a CSI window (static room + breathing + heartbeat).
Each receiver runs the same edge pipeline on its own link:
empty-room removal, subcarrier selection, and band-limited peak picking.
The coordinator fuses the two links and is the only place that assigns urgency.
"""

from __future__ import annotations

import math
import random
from typing import Optional

FS_HZ = 20.0
N_SUBCARRIERS = 16
HSR_TOP_K = 8
DURATION_S = 20.0

# Methodology bands: breathing 0.1–0.5 Hz (6–30 /min), heart 0.8–2.5 Hz (48–150 bpm).
# The search is slightly wider so an out-of-range rate is still measurable.
BREATH_SEARCH = (0.08, 0.60, 0.01)
HEART_SEARCH = (0.70, 2.60, 0.01)

EMPTY_ROOM = [1.5 + 0.08 * index for index in range(N_SUBCARRIERS)]

SCENARIOS: dict[str, dict] = {
    "resting": {
        "heart_bpm": 72,
        "breath_bpm": 16,
        "noise": 0.04,
        "motion_amp": 0.0,
        "fall": False,
        "expected_urgency": "Normal",
        "description": "Quiet resident. Heart rate and breathing stay in the normal band.",
    },
    "warning": {
        "heart_bpm": 112,
        "breath_bpm": 26,
        "noise": 0.04,
        "motion_amp": 0.0,
        "fall": False,
        "expected_urgency": "Warning",
        "description": "Elevated heart rate and breathing. Rules should raise Warning, not Emergency.",
    },
    "emergency": {
        "heart_bpm": 136,
        "breath_bpm": 18,
        "noise": 0.05,
        "motion_amp": 0.0,
        "fall": False,
        "expected_urgency": "Emergency",
        "description": "Heart rate above 130 bpm. Rules should raise Emergency.",
    },
    "fall": {
        "heart_bpm": 78,
        "breath_bpm": 17,
        "noise": 0.04,
        "motion_amp": 0.0,
        "fall": True,
        "expected_urgency": "Emergency",
        "description": "Vitals stay ordinary, then a short motion burst. The fall flag raises Emergency.",
    },
    "walking": {
        "heart_bpm": 108,
        "breath_bpm": 21,
        "noise": 0.05,
        "motion_amp": 1.15,
        "fall": False,
        "expected_urgency": "Normal",
        "description": "Walking through the link. Motion gate suppresses a vital-sign alarm.",
    },
}

# Person standing in one Fresnel zone: that receiver gets a strong link,
# the other receiver gets a weak copy (neighbor leakage).
ZONE_LINKS: dict[str, list[dict]] = {
    "zone-a": [
        {"receiver_id": "Worker-RX-A", "link_id": "tx-rx-a", "gain": 1.0, "noise_scale": 1.0},
        {"receiver_id": "Worker-RX-B", "link_id": "tx-rx-b", "gain": 0.30, "noise_scale": 2.6},
    ],
    "zone-b": [
        {"receiver_id": "Worker-RX-A", "link_id": "tx-rx-a", "gain": 0.30, "noise_scale": 2.6},
        {"receiver_id": "Worker-RX-B", "link_id": "tx-rx-b", "gain": 1.0, "noise_scale": 1.0},
    ],
}


def scenario_catalog() -> list[dict]:
    rows = []
    for name, spec in SCENARIOS.items():
        rows.append(
            {
                "scenario": name,
                "heart_bpm": spec["heart_bpm"],
                "breath_bpm": spec["breath_bpm"],
                "expected_urgency": spec["expected_urgency"],
                "description": spec["description"],
            }
        )
    return rows


def synthesize_session(
    scenario: str,
    zone_id: str,
    session_id: str,
    duration_s: float = DURATION_S,
    seed: Optional[int] = None,
) -> dict:
    """Build two CSI links for one sensing window. Raw samples stay with the transmitter."""
    if scenario not in SCENARIOS:
        raise ValueError(f"Unknown scenario '{scenario}'.")
    if zone_id not in ZONE_LINKS:
        raise ValueError(f"Unknown zone '{zone_id}'.")

    spec = SCENARIOS[scenario]
    base_seed = seed if seed is not None else random.Random(session_id).randint(1, 1_000_000)
    links = []
    for offset, link in enumerate(ZONE_LINKS[zone_id]):
        amplitudes = _synthesize_link(
            heart_bpm=spec["heart_bpm"],
            breath_bpm=spec["breath_bpm"],
            noise=spec["noise"],
            motion_amp=spec["motion_amp"],
            fall=spec["fall"],
            gain=link["gain"],
            noise_scale=link["noise_scale"],
            duration_s=duration_s,
            seed=base_seed + offset * 97,
        )
        links.append(
            {
                "receiver_id": link["receiver_id"],
                "link_id": link["link_id"],
                "gain": link["gain"],
                "amplitudes": amplitudes,
            }
        )

    return {
        "session_id": session_id,
        "scenario": scenario,
        "zone_id": zone_id,
        "fs_hz": FS_HZ,
        "n_subcarriers": N_SUBCARRIERS,
        "duration_s": duration_s,
        "ground_truth": {
            "heart_bpm": spec["heart_bpm"],
            "breath_bpm": spec["breath_bpm"],
            "fall": spec["fall"],
        },
        "links": links,
    }


def process_link(amplitudes: list[list[float]], fs_hz: float = FS_HZ) -> dict:
    """Edge pipeline for one receiver: Fresnel removal, subcarrier pick, peak rates."""
    if not amplitudes or not amplitudes[0]:
        raise ValueError("CSI window is empty.")

    n_samples = len(amplitudes)
    n_sub = len(amplitudes[0])
    normalized = _fresnel_normalize(amplitudes)

    ranked = _rank_subcarriers(normalized, fs_hz)
    chosen = ranked[: min(HSR_TOP_K, len(ranked))]
    chosen_index = [index for index, _score in chosen]
    mean_series = _mean_columns(normalized, chosen_index)

    fall = _detect_fall(mean_series)
    # Keep the vital-sign peak on the quiet part of the window when a burst lands at the end.
    vital_series = mean_series[: int(n_samples * 0.72)] if fall else mean_series
    vital_series = _demean(vital_series)

    breath_hz, breath_conf = _peak_hz(vital_series, fs_hz, *BREATH_SEARCH, strength_knee=12.0)
    heart_hz, heart_conf = _peak_hz(
        vital_series,
        fs_hz,
        *HEART_SEARCH,
        reject_harmonics_of=breath_hz,
        strength_knee=4.0,
    )

    residual = _remove_tone(vital_series, fs_hz, breath_hz)
    residual = _remove_tone(residual, fs_hz, heart_hz)
    motion_rms = _rms(residual)
    movement, activity, motion_gated = _activity_from_motion(motion_rms, fall)
    if motion_gated:
        heart_conf *= 0.25
        breath_conf *= 0.25

    reliable = (not motion_gated) and heart_conf >= 0.45 and breath_conf >= 0.45
    return {
        "heart_bpm": round(heart_hz * 60.0, 1),
        "breath_bpm": round(breath_hz * 60.0, 1),
        "heart_hz": round(heart_hz, 4),
        "breath_hz": round(breath_hz, 4),
        "heart_confidence": round(heart_conf, 3),
        "breath_confidence": round(breath_conf, 3),
        "movement": movement,
        "activity": activity,
        "fall_detected": fall,
        "motion_gated": motion_gated,
        "vitals_reliable": reliable,
        "motion_rms": round(motion_rms, 4),
        "selected_subcarriers": chosen_index,
        "n_samples": n_samples,
        "n_subcarriers": n_sub,
        "fs_hz": fs_hz,
    }


def fuse_links(estimates: list[dict], primary_receiver_id: str) -> dict:
    """
    Fuse receiver pipelines.

    A weak neighbor link is left out of the average. If the two strong links
    disagree, the higher-confidence link is kept instead of averaging them
    into a rate neither receiver measured.
    """
    if not estimates:
        raise ValueError("No receiver estimates to fuse.")

    primary = next((row for row in estimates if row["node_id"] == primary_receiver_id), estimates[0])
    others = [row for row in estimates if row is not primary]
    # A walking resident corrupts the zone link. Do not replace those rates with the quieter neighbor.
    rate_pool = [primary] if primary["motion_gated"] else estimates

    heart_bpm, heart_conf, heart_method = _fuse_channel(
        rate_pool, "heart_bpm", "heart_confidence", disagreement=8.0
    )
    breath_bpm, breath_conf, breath_method = _fuse_channel(
        rate_pool, "breath_bpm", "breath_confidence", disagreement=3.0
    )

    neighbor_fall = any(row["fall_detected"] for row in others)
    fall = bool(primary["fall_detected"])
    leakage_note = None
    if neighbor_fall and not fall:
        leakage_note = (
            "The neighbor receiver reported a motion spike. "
            "The zone link did not confirm it, so it was not treated as a fall."
        )

    motion_gated = bool(primary["motion_gated"])
    reliable = (not motion_gated) and heart_conf >= 0.45 and breath_conf >= 0.45
    if motion_gated:
        method = "zone-link-only"
    elif len(estimates) == 1:
        method = "single-link"
    elif heart_method == breath_method:
        method = heart_method
    else:
        method = f"heart:{heart_method}, breath:{breath_method}"

    return {
        "heart_bpm": heart_bpm,
        "breath_bpm": breath_bpm,
        "heart_confidence": heart_conf,
        "breath_confidence": breath_conf,
        "movement": primary["movement"],
        "activity": "falling" if fall else primary["activity"],
        "fall_detected": fall,
        "motion_gated": motion_gated,
        "vitals_reliable": reliable,
        "fusion": method,
        "primary_receiver": primary["node_id"],
        "receivers_used": [row["node_id"] for row in estimates],
        "neighbor_leakage_note": leakage_note,
    }


def classify_urgency(
    heart_bpm: float,
    breath_bpm: float,
    activity: str,
    fall_detected: bool,
    vitals_reliable: bool,
) -> str:
    """Rule core. A wording template is not allowed to change this label."""
    if fall_detected or activity == "falling":
        return "Emergency"
    if not vitals_reliable:
        return "Normal"

    hr_ok = 20.0 <= heart_bpm <= 220.0
    rr_ok = 2.0 <= breath_bpm <= 60.0
    if not hr_ok and not rr_ok:
        return "Normal"
    if rr_ok and (breath_bpm < 6 or breath_bpm > 30):
        return "Emergency"
    if hr_ok and (heart_bpm < 40 or heart_bpm > 130):
        return "Emergency"
    if rr_ok and (breath_bpm <= 8 or breath_bpm >= 25):
        return "Warning"
    if hr_ok and (heart_bpm <= 50 or heart_bpm >= 110):
        return "Warning"
    return "Normal"


def abnormality_tags(
    heart_bpm: float,
    breath_bpm: float,
    fall_detected: bool,
    vitals_reliable: bool,
) -> list[str]:
    tags: list[str] = []
    if fall_detected:
        tags.append("fall")
    # Unreliable windows still report a fall, but they do not get vital-sign tags.
    if not vitals_reliable:
        return tags
    if heart_bpm > 100:
        tags.append("tachycardia")
    elif 0 < heart_bpm < 50:
        tags.append("bradycardia")
    if breath_bpm > 24:
        tags.append("tachypnea")
    elif 0 < breath_bpm < 8:
        tags.append("bradypnea")
    return tags


def alert_summary(
    urgency: str,
    heart_bpm: float,
    breath_bpm: float,
    activity: str,
    fall_detected: bool,
    motion_gated: bool,
    resident_name: str,
    zone_id: str,
    care_level: str,
    tags: list[str],
) -> str:
    """Caregiver sentence. It restates the rule label and does not choose a new one."""
    place = f"{resident_name} ({zone_id}, {care_level} care)"
    tag_text = f" Marked as {', '.join(tags)}." if tags else ""
    rates = f"Heart rate {heart_bpm:.0f} bpm, breathing {breath_bpm:.0f}/min"

    if urgency == "Emergency" and fall_detected:
        sentence = (
            f"Emergency for {place}: possible fall. {rates}.{tag_text} "
            "Check the resident in person. This is not a diagnosis."
        )
    elif urgency == "Emergency":
        sentence = (
            f"Emergency for {place}: vital estimates are outside the safe band while {activity}. "
            f"{rates}.{tag_text} Check the resident and confirm with a pulse oximeter. "
            "This is not a diagnosis."
        )
    elif urgency == "Warning":
        sentence = (
            f"Warning for {place}: review needed while {activity}. {rates}.{tag_text} "
            "This is not a diagnosis."
        )
    else:
        sentence = (
            f"Normal for {place}: {rates}, activity {activity}.{tag_text} No alert is sent."
        )
        if motion_gated:
            sentence += " Body movement made this vital-sign window unreliable, so no vital-sign alarm was raised."
    return sentence


def _synthesize_link(
    heart_bpm: float,
    breath_bpm: float,
    noise: float,
    motion_amp: float,
    fall: bool,
    gain: float,
    noise_scale: float,
    duration_s: float,
    seed: int,
) -> list[list[float]]:
    rng = random.Random(seed)
    n = int(round(duration_s * FS_HZ))
    heart_hz = heart_bpm / 60.0
    breath_hz = breath_bpm / 60.0
    fall_at = int(n * 0.8)
    amplitudes: list[list[float]] = []

    for index in range(n):
        t = index / FS_HZ
        breath = math.sin(2.0 * math.pi * breath_hz * t)
        heart = math.sin(2.0 * math.pi * heart_hz * t + 0.4)
        motion = motion_amp * rng.gauss(0.0, 1.0) if motion_amp else 0.0
        if fall and index >= fall_at:
            motion += 7.0 * math.sin(2.0 * math.pi * 3.0 * t)
        row = []
        for sub in range(N_SUBCARRIERS):
            # Lower subcarriers carry the person. Higher ones stay near the empty-room floor.
            physiology = 1.0 if sub < 10 else 0.08
            dynamic = physiology * gain * (breath + 0.45 * heart + motion)
            jitter = rng.gauss(0.0, noise * noise_scale)
            row.append(round(EMPTY_ROOM[sub] + dynamic + jitter, 5))
        amplitudes.append(row)
    return amplitudes


def _fresnel_normalize(amplitudes: list[list[float]]) -> list[list[float]]:
    """Remove the empty-room baseline, then any leftover per-subcarrier mean."""
    n_sub = len(amplitudes[0])
    baseline = EMPTY_ROOM if n_sub == len(EMPTY_ROOM) else [0.0] * n_sub
    shifted = [
        [sample - baseline[sub] for sub, sample in enumerate(row)]
        for row in amplitudes
    ]
    means = []
    count = float(len(shifted))
    for sub in range(n_sub):
        means.append(sum(row[sub] for row in shifted) / count)
    return [
        [sample - means[sub] for sub, sample in enumerate(row)]
        for row in shifted
    ]


def _rank_subcarriers(normalized: list[list[float]], fs_hz: float) -> list[tuple[int, float]]:
    """Prefer subcarriers whose energy sits in the heart and breathing bands."""
    scores = []
    for sub in range(len(normalized[0])):
        column = [row[sub] for row in normalized]
        heart_power = _tone_power(column, fs_hz, 1.2) + _tone_power(column, fs_hz, 1.8)
        breath_power = _tone_power(column, fs_hz, 0.2) + _tone_power(column, fs_hz, 0.35)
        scores.append((sub, heart_power + breath_power))
    scores.sort(key=lambda item: item[1], reverse=True)
    return scores


def _mean_columns(normalized: list[list[float]], columns: list[int]) -> list[float]:
    width = float(len(columns))
    return [
        sum(row[sub] for sub in columns) / width
        for row in normalized
    ]


def _detect_fall(series: list[float]) -> bool:
    n = len(series)
    if n < 40:
        return False
    early = series[: int(n * 0.6)]
    late = series[int(n * 0.78) :]
    early_rms = _rms(early)
    late_rms = _rms(late)
    return late_rms > 1.5 and late_rms > early_rms * 3.5


def _activity_from_motion(motion_rms: float, fall: bool) -> tuple[str, str, bool]:
    if fall:
        return "high", "falling", False
    # Tuned for this simulator's amplitude scale, using the thesis labels.
    if motion_rms >= 0.45:
        return "high", "walking", True
    if motion_rms >= 0.18:
        return "moderate", "standing", False
    if motion_rms >= 0.07:
        return "low", "sitting", False
    return "very low", "resting", False


def _peak_hz(
    series: list[float],
    fs_hz: float,
    low: float,
    high: float,
    step: float,
    reject_harmonics_of: Optional[float] = None,
    strength_knee: float = 4.0,
) -> tuple[float, float]:
    windowed = _hann(series)
    best_hz = low
    best_power = -1.0
    powers: list[float] = []
    hz = low
    while hz <= high + 1e-9:
        if reject_harmonics_of and _is_breath_harmonic(hz, reject_harmonics_of):
            hz = round(hz + step, 5)
            continue
        power = _tone_power(windowed, fs_hz, hz)
        powers.append(power)
        if power > best_power:
            best_power = power
            best_hz = hz
        hz = round(hz + step, 5)

    if not powers:
        return low, 0.0

    # Compare the peak with the middle of the band so a flat spectrum scores low.
    ordered = sorted(powers)
    median = ordered[len(ordered) // 2]
    # Peakiness catches a flat spectrum. Strength separates the zone link from a weak neighbor:
    # the same tone at a third of the gain has much less power, so it should not outvote the zone.
    ratio = 0.0 if median <= 1e-12 else best_power / median
    peakiness = max(0.0, min(1.0, (ratio - 1.8) / 10.0))
    strength = best_power / (best_power + strength_knee) if best_power > 0 else 0.0
    confidence = max(0.0, min(1.0, peakiness * strength))
    return best_hz, confidence


def _is_breath_harmonic(heart_hz: float, breath_hz: float) -> bool:
    if breath_hz <= 0:
        return False
    ratio = heart_hz / breath_hz
    nearest = round(ratio)
    if nearest < 2:
        return False
    return abs(heart_hz - nearest * breath_hz) <= 0.04


def _tone_power(series: list[float], fs_hz: float, freq_hz: float) -> float:
    step = 2.0 * math.pi * freq_hz / fs_hz
    cosine = 0.0
    sine = 0.0
    for index, value in enumerate(series):
        angle = step * index
        cosine += value * math.cos(angle)
        sine += value * math.sin(angle)
    n = float(len(series)) or 1.0
    return (cosine * cosine + sine * sine) / n


def _remove_tone(series: list[float], fs_hz: float, freq_hz: float) -> list[float]:
    step = 2.0 * math.pi * freq_hz / fs_hz
    cosine_proj = 0.0
    sine_proj = 0.0
    cosine_norm = 0.0
    sine_norm = 0.0
    angles = []
    for index, value in enumerate(series):
        angle = step * index
        cosine = math.cos(angle)
        sine = math.sin(angle)
        angles.append((cosine, sine))
        cosine_proj += value * cosine
        sine_proj += value * sine
        cosine_norm += cosine * cosine
        sine_norm += sine * sine
    cosine_amp = cosine_proj / cosine_norm if cosine_norm else 0.0
    sine_amp = sine_proj / sine_norm if sine_norm else 0.0
    return [
        value - cosine_amp * cosine - sine_amp * sine
        for value, (cosine, sine) in zip(series, angles)
    ]


def _fuse_channel(
    estimates: list[dict],
    value_key: str,
    confidence_key: str,
    disagreement: float,
) -> tuple[float, float, str]:
    ranked = sorted(estimates, key=lambda row: row[confidence_key], reverse=True)
    strong = [row for row in ranked if row[confidence_key] >= 0.45]
    pool = strong or ranked[:1]

    if len(pool) == 1:
        winner = pool[0]
        return round(winner[value_key], 1), round(winner[confidence_key], 3), "higher-confidence-link"

    spread = max(row[value_key] for row in pool) - min(row[value_key] for row in pool)
    if spread > disagreement:
        winner = pool[0]
        return round(winner[value_key], 1), round(winner[confidence_key], 3), "higher-confidence-link"

    weight = sum(max(row[confidence_key], 0.05) for row in pool)
    value = sum(row[value_key] * max(row[confidence_key], 0.05) for row in pool) / weight
    confidence = sum(row[confidence_key] for row in pool) / len(pool)
    return round(value, 1), round(confidence, 3), "confidence-weighted"


def _hann(series: list[float]) -> list[float]:
    n = len(series)
    if n < 2:
        return list(series)
    scale = 2.0 * math.pi / (n - 1)
    return [
        value * (0.5 - 0.5 * math.cos(scale * index))
        for index, value in enumerate(series)
    ]


def _demean(series: list[float]) -> list[float]:
    if not series:
        return []
    mean = sum(series) / len(series)
    return [value - mean for value in series]


def _rms(series: list[float]) -> float:
    if not series:
        return 0.0
    return math.sqrt(sum(value * value for value in series) / len(series))


def _self_check() -> None:
    """Run the five demo scenarios and print both receivers plus the fused label."""
    failures = []
    for scenario, spec in SCENARIOS.items():
        for zone, primary in (("zone-a", "Worker-RX-A"), ("zone-b", "Worker-RX-B")):
            session = synthesize_session(scenario, zone, f"check-{scenario}-{zone}", seed=7)
            estimates = []
            for link in session["links"]:
                estimate = process_link(link["amplitudes"], session["fs_hz"])
                estimate["node_id"] = link["receiver_id"]
                estimates.append(estimate)
            fused = fuse_links(estimates, primary)
            urgency = classify_urgency(
                fused["heart_bpm"],
                fused["breath_bpm"],
                fused["activity"],
                fused["fall_detected"],
                fused["vitals_reliable"],
            )
            by_id = {row["node_id"]: row for row in estimates}
            print(
                f"{scenario:10} {zone:7} "
                f"HR {fused['heart_bpm']:6.1f} (truth {spec['heart_bpm']:3}) "
                f"RR {fused['breath_bpm']:5.1f} (truth {spec['breath_bpm']:2}) "
                f"urg {urgency:9} exp {spec['expected_urgency']:9} "
                f"act {fused['activity']:8} "
                f"A {by_id['Worker-RX-A']['heart_bpm']:6.1f}/{by_id['Worker-RX-A']['heart_confidence']:.2f} "
                f"B {by_id['Worker-RX-B']['heart_bpm']:6.1f}/{by_id['Worker-RX-B']['heart_confidence']:.2f} "
                f"rms {by_id[primary]['motion_rms']:.3f} fall {fused['fall_detected']}"
            )
            if urgency != spec["expected_urgency"]:
                failures.append(f"{scenario}/{zone} urgency {urgency}")
            if spec["expected_urgency"] != "Normal" or scenario == "resting":
                if scenario != "walking" and abs(fused["heart_bpm"] - spec["heart_bpm"]) > 3:
                    failures.append(f"{scenario}/{zone} heart {fused['heart_bpm']}")
                if scenario != "walking" and abs(fused["breath_bpm"] - spec["breath_bpm"]) > 2:
                    failures.append(f"{scenario}/{zone} breath {fused['breath_bpm']}")
            if scenario == "fall" and not fused["fall_detected"]:
                failures.append(f"{scenario}/{zone} missed fall")
            if scenario == "walking" and not fused["motion_gated"]:
                failures.append(f"{scenario}/{zone} motion gate did not trip")
            if fused["primary_receiver"] != primary:
                failures.append(f"{scenario}/{zone} primary {fused['primary_receiver']}")
            if scenario in {"resting", "warning", "emergency"}:
                neighbor_id = "Worker-RX-B" if primary == "Worker-RX-A" else "Worker-RX-A"
                if by_id[primary]["heart_confidence"] <= by_id[neighbor_id]["heart_confidence"]:
                    failures.append(f"{scenario}/{zone} neighbor outranked the zone link")

    if failures:
        print("SELF-CHECK FAILED")
        for item in failures:
            print(" -", item)
        raise SystemExit(1)
    print("SELF-CHECK OK")


if __name__ == "__main__":
    _self_check()
