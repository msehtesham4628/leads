#!/usr/bin/env python3
"""Collect U.S. business leads from OpenStreetMap.

This CLI fetches business-like features from OpenStreetMap and exports entries
that have no website tag but do have contact/social details.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence
from urllib.error import HTTPError, URLError

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
OVERPASS_ENDPOINTS = (
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
)
USER_AGENT = "us-business-leads-collector/1.2 (contact: local-script)"

US_STATES: Sequence[str] = (
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware",
    "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky",
    "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
    "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island",
    "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming", "District of Columbia",
)


@dataclass
class Lead:
    business_name: str
    phone: str
    email: str
    facebook: str
    instagram: str
    city: str
    state: str
    country: str
    osm_type: str
    osm_id: int
    lat: str
    lon: str


class ApiError(RuntimeError):
    """Raised when an upstream OSM API call fails."""


def _http_request_json(
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, str]] = None,
    data: Optional[Dict[str, str]] = None,
    timeout: int = 120,
    retries: int = 3,
) -> Dict:
    request_url = url if not params else f"{url}?{urllib.parse.urlencode(params)}"
    body = urllib.parse.urlencode(data).encode("utf-8") if data is not None else None
    headers = {"User-Agent": USER_AGENT}
    if data is not None:
        headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"

    for attempt in range(1, retries + 1):
        req = urllib.request.Request(request_url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except HTTPError as exc:
            if attempt < retries and exc.code in {429, 500, 502, 503, 504}:
                time.sleep((2 ** attempt) + random.random())
                continue
            raise ApiError(f"HTTP {exc.code} calling {url}: {exc.reason}") from exc
        except URLError as exc:
            if attempt < retries:
                time.sleep((2 ** attempt) + random.random())
                continue
            raise ApiError(f"Network error calling {url}: {exc}") from exc
        except json.JSONDecodeError as exc:
            if attempt < retries:
                time.sleep((2 ** attempt) + random.random())
                continue
            raise ApiError(f"Invalid JSON response from {url}") from exc

    raise ApiError(f"Failed calling {url}")


def get_state_area_id(state: str) -> int:
    data = _http_request_json(
        "GET",
        NOMINATIM_URL,
        params={
            "q": f"{state}, United States",
            "format": "jsonv2",
            "limit": "10",
            "addressdetails": "1",
            "extratags": "1",
        },
        timeout=120,
    )
    for item in data:
        if item.get("osm_type") == "relation":
            address = item.get("address", {})
            if "united states" in (address.get("country") or "").lower() or address.get("country_code") == "us":
                return 3600000000 + int(item["osm_id"])
    raise ApiError(f"Could not resolve a U.S. relation area for state: {state}")


def build_overpass_query(area_id: int, limit: int) -> str:
    return f"""
[out:json][timeout:300];
area({area_id})->.searchArea;
(
  node["name"]["amenity"][!"website"][!"contact:website"](area.searchArea);
  way["name"]["amenity"][!"website"][!"contact:website"](area.searchArea);
  relation["name"]["amenity"][!"website"][!"contact:website"](area.searchArea);
  node["name"]["shop"][!"website"][!"contact:website"](area.searchArea);
  way["name"]["shop"][!"website"][!"contact:website"](area.searchArea);
  relation["name"]["shop"][!"website"][!"contact:website"](area.searchArea);
  node["name"]["office"][!"website"][!"contact:website"](area.searchArea);
  way["name"]["office"][!"website"][!"contact:website"](area.searchArea);
  relation["name"]["office"][!"website"][!"contact:website"](area.searchArea);
);
out center tags {limit};
""".strip()


def query_overpass(area_id: int, limit: int) -> Dict:
    query = build_overpass_query(area_id, limit)
    last_error: Optional[Exception] = None
    for endpoint in OVERPASS_ENDPOINTS:
        try:
            return _http_request_json("POST", endpoint, data={"data": query}, timeout=300, retries=3)
        except ApiError as exc:
            last_error = exc
            print(f"WARN: {endpoint} failed: {exc}", file=sys.stderr)
    raise ApiError(f"All Overpass endpoints failed: {last_error}")


def pick(tags: Dict[str, str], *keys: str) -> str:
    for key in keys:
        value = tags.get(key)
        if value:
            return value.strip()
    return ""


def extract_lead(element: Dict, fallback_state: str) -> Optional[Lead]:
    tags = element.get("tags", {})
    phone = pick(tags, "phone", "contact:phone", "mobile", "contact:mobile")
    email = pick(tags, "email", "contact:email")
    facebook = pick(tags, "facebook", "contact:facebook")
    instagram = pick(tags, "instagram", "contact:instagram")
    if not any([phone, email, facebook, instagram]):
        return None

    return Lead(
        business_name=pick(tags, "name"),
        phone=phone,
        email=email,
        facebook=facebook,
        instagram=instagram,
        city=pick(tags, "addr:city", "city"),
        state=pick(tags, "addr:state", "state") or fallback_state,
        country=pick(tags, "addr:country", "country", "addr:country_code") or "US",
        osm_type=element.get("type", ""),
        osm_id=int(element.get("id", 0)),
        lat=str(element.get("lat") or element.get("center", {}).get("lat") or ""),
        lon=str(element.get("lon") or element.get("center", {}).get("lon") or ""),
    )


def dedupe(leads: Iterable[Lead]) -> List[Lead]:
    seen = set()
    unique: List[Lead] = []
    for lead in leads:
        key = (lead.business_name.lower(), lead.phone, lead.email.lower(), lead.facebook.lower(), lead.instagram.lower())
        if key not in seen:
            seen.add(key)
            unique.append(lead)
    return unique


def save_csv(leads: List[Lead], output: Path) -> None:
    fields = ["business_name", "phone", "email", "facebook", "instagram", "city", "state", "country", "osm_type", "osm_id", "lat", "lon"]
    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        for lead in leads:
            writer.writerow(asdict(lead))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect U.S. business leads from OSM where website tags are missing.")
    parser.add_argument("--state", help='Single state to process, e.g. "Texas"')
    parser.add_argument("--all-states", action="store_true", help="Run for all U.S. states + District of Columbia")
    parser.add_argument("--limit", type=int, default=5000, help="Max OSM rows to fetch per state")
    parser.add_argument("--output", default="leads.csv", help="CSV output path")
    parser.add_argument("--sleep", type=float, default=1.0, help="Pause between states/requests")
    parser.add_argument("--dry-run", action="store_true", help="Print resolved run plan and exit without API calls")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail immediately on first state error (default: continue in nationwide mode)",
    )
    args = parser.parse_args()
    if args.state and args.all_states:
        parser.error("Use either --state or --all-states, not both")
    if not args.state and not args.all_states:
        args.all_states = True
    return args


def run_for_state(state: str, limit: int, sleep_s: float) -> List[Lead]:
    print(f"Resolving OSM area for {state}...")
    area_id = get_state_area_id(state)
    print(f"Area id for {state}: {area_id}")
    if sleep_s > 0:
        time.sleep(sleep_s)
    print(f"Querying Overpass for {state}...")
    elements = query_overpass(area_id=area_id, limit=limit).get("elements", [])
    leads = [lead for element in elements if (lead := extract_lead(element, state))]
    print(f"Collected {len(leads)} raw leads for {state}")
    return leads


def main() -> int:
    args = parse_args()
    states = [args.state] if args.state else list(US_STATES)

    if args.dry_run:
        print(f"Dry run mode: {'single-state' if args.state else 'nationwide'}")
        print(f"States to process: {len(states)}")
        print(", ".join(states))
        print(f"Limit per state: {args.limit}")
        print(f"Output file: {args.output}")
        return 0

    all_leads: List[Lead] = []
    failed_states: List[str] = []

    for idx, state in enumerate(states, start=1):
        print(f"[{idx}/{len(states)}] Processing {state}")
        try:
            all_leads.extend(run_for_state(state, args.limit, args.sleep))
        except ApiError as exc:
            print(f"WARN: Skipping {state} due to error: {exc}", file=sys.stderr)
            failed_states.append(state)
            if args.strict or args.state:
                return 1

    unique = dedupe(all_leads)
    save_csv(unique, Path(args.output))
    print(f"Wrote {len(unique)} unique leads to {args.output}")

    if failed_states:
        print(f"Completed with {len(failed_states)} failed state(s): {', '.join(failed_states)}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
