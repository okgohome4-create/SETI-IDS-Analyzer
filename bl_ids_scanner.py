#!/usr/bin/env python3
"""
IDS/TSDC Candidate Ranking Scanner

A lightweight technosignature-ranking CLI for independent researchers.

What it does:
1) Fetches exoplanet target metadata from NASA Exoplanet Archive.
2) Loads candidate signal tables (CSV) from a prior detection stage
   such as turboSETI/hyperseti exports or hand-curated candidate lists.
3) Computes IDS/TSDC-inspired scores and ranks low-energy/high-structure
   candidates.

What it does NOT do by itself:
- It does not run raw narrowband searches on Breakthrough Listen .fil/.h5
  files unless you provide candidate outputs from tools such as turboSETI
  or hyperseti.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

NASA_EXO_API = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"


@dataclass
class Candidate:
    target_name: str
    signal_id: str
    snr: float
    drift_rate_hz_s: float
    bandwidth_hz: float
    center_frequency_hz: float
    distance_pc: float
    periodicity_score: float
    modulation_score: float
    complexity_score: float
    stability_score: float
    rfi_score: float
    host_teq_k: Optional[float] = None
    host_insol: Optional[float] = None
    host_sy_dist: Optional[float] = None
    host_st_teff: Optional[float] = None
    estimated_eirp_w: Optional[float] = None
    structure_score: Optional[float] = None
    ids_proxy: Optional[float] = None
    tsdc_score: Optional[float] = None
    overall_score: Optional[float] = None
    notes: Optional[str] = None


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def normalize_feature(value: float, floor: float, ceiling: float) -> float:
    if ceiling <= floor:
        return 0.0
    return clamp((value - floor) / (ceiling - floor))


def estimate_eirp_w(snr: float, distance_pc: float, bandwidth_hz: float) -> float:
    """
    Simple comparative proxy, not a rigorous radiometry result.

    Assumptions are intentionally conservative and approximate:
    - required emitted power scales with distance^2
    - wider bandwidth increases energy cost
    - higher observed SNR increases required EIRP proxy

    This is a ranking proxy, not a physical detection claim.
    """
    distance_scale = max(distance_pc, 0.1) ** 2
    bw_scale = max(bandwidth_hz, 1.0) ** 0.5
    snr_scale = max(snr, 1.0)
    return snr_scale * distance_scale * bw_scale


def compute_structure_score(c: Candidate) -> float:
    weighted = (
        0.30 * clamp(c.complexity_score)
        + 0.25 * clamp(c.periodicity_score)
        + 0.25 * clamp(c.modulation_score)
        + 0.20 * clamp(c.stability_score)
    )
    # penalize very broad signals because narrow/organized signals are often
    # preferred in technosignature triage.
    bw_penalty = 1.0 / (1.0 + math.log10(max(c.bandwidth_hz, 1.0)))
    return clamp(weighted * bw_penalty * 4.0)


def compute_ids_proxy(c: Candidate) -> float:
    eirp = c.estimated_eirp_w if c.estimated_eirp_w is not None else estimate_eirp_w(c.snr, c.distance_pc, c.bandwidth_hz)
    structure = c.structure_score if c.structure_score is not None else compute_structure_score(c)
    # IDS-inspired comparative ratio: more structure for less energy cost.
    return structure / (1.0 + math.log10(max(eirp, 1.0)))


def compute_tsdc_score(c: Candidate) -> float:
    """
    TSDC-inspired score favoring decoupling between signal organization and
    radiative cost. We reward:
    - high structure
    - modest bandwidth
    - moderate/low EIRP proxy
    - stability
    - low RFI contamination
    """
    eirp = c.estimated_eirp_w if c.estimated_eirp_w is not None else estimate_eirp_w(c.snr, c.distance_pc, c.bandwidth_hz)
    structure = c.structure_score if c.structure_score is not None else compute_structure_score(c)
    energy_efficiency = 1.0 / (1.0 + math.log10(max(eirp, 1.0)))
    narrowband_bonus = 1.0 / (1.0 + math.log10(max(c.bandwidth_hz, 1.0)))
    cleanliness = 1.0 - clamp(c.rfi_score)
    return clamp(0.40 * structure + 0.25 * energy_efficiency + 0.15 * narrowband_bonus + 0.10 * clamp(c.stability_score) + 0.10 * cleanliness)


def compute_overall_score(c: Candidate) -> float:
    ids = c.ids_proxy if c.ids_proxy is not None else compute_ids_proxy(c)
    tsdc = c.tsdc_score if c.tsdc_score is not None else compute_tsdc_score(c)
    anti_rfi = 1.0 - clamp(c.rfi_score)
    drift_pref = 1.0 / (1.0 + abs(c.drift_rate_hz_s) / 10.0)
    return clamp(0.45 * ids + 0.35 * tsdc + 0.10 * anti_rfi + 0.10 * drift_pref)


def parse_float(row: Dict[str, str], key: str, default: float = 0.0) -> float:
    raw = row.get(key, "")
    if raw in (None, ""):
        return default
    return float(raw)


REQUIRED_COLUMNS = {
    "target_name",
    "signal_id",
    "snr",
    "drift_rate_hz_s",
    "bandwidth_hz",
    "center_frequency_hz",
    "distance_pc",
    "periodicity_score",
    "modulation_score",
    "complexity_score",
    "stability_score",
    "rfi_score",
}


def load_candidates_csv(path: Path) -> List[Candidate]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        cols = set(reader.fieldnames or [])
        missing = REQUIRED_COLUMNS - cols
        if missing:
            raise ValueError(f"Missing required CSV columns: {', '.join(sorted(missing))}")
        out: List[Candidate] = []
        for row in reader:
            out.append(
                Candidate(
                    target_name=row["target_name"],
                    signal_id=row["signal_id"],
                    snr=parse_float(row, "snr"),
                    drift_rate_hz_s=parse_float(row, "drift_rate_hz_s"),
                    bandwidth_hz=parse_float(row, "bandwidth_hz"),
                    center_frequency_hz=parse_float(row, "center_frequency_hz"),
                    distance_pc=parse_float(row, "distance_pc"),
                    periodicity_score=parse_float(row, "periodicity_score"),
                    modulation_score=parse_float(row, "modulation_score"),
                    complexity_score=parse_float(row, "complexity_score"),
                    stability_score=parse_float(row, "stability_score"),
                    rfi_score=parse_float(row, "rfi_score"),
                )
            )
        return out


def fetch_exoplanet_metadata(limit: int = 50) -> List[Dict[str, object]]:
    query = (
        "select top {limit} pl_name,hostname,sy_dist,pl_orbsmax,pl_eqt,pl_insol,st_teff "
        "from pscomppars where sy_dist is not null order by sy_dist asc"
    ).format(limit=limit)
    params = {
        "query": query,
        "format": "json",
    }
    url = NASA_EXO_API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "IDS-TSDC-Scanner/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = resp.read().decode("utf-8")
        return json.loads(data)


def build_host_lookup(rows: Iterable[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    lookup: Dict[str, Dict[str, object]] = {}
    for row in rows:
        for key in (row.get("pl_name"), row.get("hostname")):
            if isinstance(key, str) and key:
                lookup[key.strip().lower()] = row
    return lookup


def enrich_candidates(candidates: List[Candidate], host_lookup: Dict[str, Dict[str, object]]) -> None:
    for c in candidates:
        meta = host_lookup.get(c.target_name.strip().lower())
        if not meta:
            continue
        c.host_teq_k = _to_float(meta.get("pl_eqt"))
        c.host_insol = _to_float(meta.get("pl_insol"))
        c.host_sy_dist = _to_float(meta.get("sy_dist"))
        c.host_st_teff = _to_float(meta.get("st_teff"))
        if c.host_sy_dist is not None and (c.distance_pc <= 0 or math.isnan(c.distance_pc)):
            c.distance_pc = c.host_sy_dist


def _to_float(value: object) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def score_candidates(candidates: List[Candidate]) -> List[Candidate]:
    for c in candidates:
        c.estimated_eirp_w = estimate_eirp_w(c.snr, c.distance_pc, c.bandwidth_hz)
        c.structure_score = compute_structure_score(c)
        c.ids_proxy = compute_ids_proxy(c)
        c.tsdc_score = compute_tsdc_score(c)
        c.overall_score = compute_overall_score(c)
        notes: List[str] = []
        if c.rfi_score >= 0.7:
            notes.append("High-RFI-likelihood")
        if c.bandwidth_hz <= 10:
            notes.append("Very-narrowband")
        if c.structure_score >= 0.75:
            notes.append("High-structure")
        if c.ids_proxy >= 0.2:
            notes.append("High-IDS-proxy")
        c.notes = "; ".join(notes) if notes else ""
    return sorted(candidates, key=lambda x: (x.overall_score or 0.0), reverse=True)


def write_ranked_csv(candidates: List[Candidate], path: Path) -> None:
    if not candidates:
        raise ValueError("No candidates to write")
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(candidates[0]).keys()))
        writer.writeheader()
        for c in candidates:
            writer.writerow(asdict(c))


def print_top(candidates: List[Candidate], top_n: int = 10) -> None:
    print(f"Top {min(top_n, len(candidates))} ranked candidates")
    print("=" * 96)
    for i, c in enumerate(candidates[:top_n], start=1):
        print(
            f"{i:2d}. {c.signal_id:<12} target={c.target_name:<20} score={c.overall_score:.3f} "
            f"IDS={c.ids_proxy:.3f} TSDC={c.tsdc_score:.3f} SNR={c.snr:.1f} BW={c.bandwidth_hz:.2f}Hz "
            f"drift={c.drift_rate_hz_s:.3f}Hz/s notes={c.notes or '-'}"
        )


def write_example_candidates(path: Path) -> None:
    rows = [
        {
            "target_name": "Proxima Cen b",
            "signal_id": "cand_001",
            "snr": 31.2,
            "drift_rate_hz_s": 0.18,
            "bandwidth_hz": 2.7,
            "center_frequency_hz": 1420405751.0,
            "distance_pc": 1.30,
            "periodicity_score": 0.82,
            "modulation_score": 0.67,
            "complexity_score": 0.76,
            "stability_score": 0.88,
            "rfi_score": 0.10,
        },
        {
            "target_name": "Tau Ceti e",
            "signal_id": "cand_002",
            "snr": 19.8,
            "drift_rate_hz_s": 1.90,
            "bandwidth_hz": 14.0,
            "center_frequency_hz": 982500000.0,
            "distance_pc": 3.60,
            "periodicity_score": 0.71,
            "modulation_score": 0.52,
            "complexity_score": 0.64,
            "stability_score": 0.79,
            "rfi_score": 0.22,
        },
        {
            "target_name": "TRAPPIST-1 e",
            "signal_id": "cand_003",
            "snr": 27.5,
            "drift_rate_hz_s": 0.05,
            "bandwidth_hz": 1.2,
            "center_frequency_hz": 4531000000.0,
            "distance_pc": 12.10,
            "periodicity_score": 0.88,
            "modulation_score": 0.81,
            "complexity_score": 0.83,
            "stability_score": 0.91,
            "rfi_score": 0.08,
        },
        {
            "target_name": "HD 40307 g",
            "signal_id": "cand_004",
            "snr": 15.0,
            "drift_rate_hz_s": 7.20,
            "bandwidth_hz": 120.0,
            "center_frequency_hz": 1100000000.0,
            "distance_pc": 12.90,
            "periodicity_score": 0.28,
            "modulation_score": 0.24,
            "complexity_score": 0.35,
            "stability_score": 0.43,
            "rfi_score": 0.74,
        },
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="IDS/TSDC technosignature candidate ranker")
    sub = parser.add_subparsers(dest="command", required=True)

    p_fetch = sub.add_parser("fetch-targets", help="Fetch nearby exoplanet metadata from NASA Exoplanet Archive")
    p_fetch.add_argument("--limit", type=int, default=25, help="Number of rows to fetch")
    p_fetch.add_argument("--output", type=Path, default=Path("targets.json"))

    p_example = sub.add_parser("make-example", help="Write an example candidate CSV")
    p_example.add_argument("--output", type=Path, default=Path("example_candidates.csv"))

    p_rank = sub.add_parser("rank", help="Rank candidates from CSV")
    p_rank.add_argument("--input", type=Path, required=True, help="Input candidate CSV")
    p_rank.add_argument("--output", type=Path, default=Path("ranked_candidates.csv"))
    p_rank.add_argument("--targets-json", type=Path, help="Optional NASA target metadata JSON")
    p_rank.add_argument("--top", type=int, default=10, help="How many top rows to print")

    args = parser.parse_args(argv)

    try:
        if args.command == "fetch-targets":
            rows = fetch_exoplanet_metadata(limit=args.limit)
            args.output.write_text(json.dumps(rows, indent=2), encoding="utf-8")
            print(f"Saved {len(rows)} target rows to {args.output}")
            return 0

        if args.command == "make-example":
            write_example_candidates(args.output)
            print(f"Wrote example candidate CSV to {args.output}")
            return 0

        if args.command == "rank":
            candidates = load_candidates_csv(args.input)
            if args.targets_json and args.targets_json.exists():
                rows = json.loads(args.targets_json.read_text(encoding="utf-8"))
                enrich_candidates(candidates, build_host_lookup(rows))
            ranked = score_candidates(candidates)
            write_ranked_csv(ranked, args.output)
            print_top(ranked, top_n=args.top)
            print(f"\nSaved ranked output to {args.output}")
            return 0

        parser.error("Unknown command")
        return 2
    except urllib.error.URLError as exc:
        print(f"Network error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
