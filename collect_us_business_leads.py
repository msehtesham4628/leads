#!/usr/bin/env python3
"""Collect U.S. business leads from OpenStreetMap.

This script fetches businesses from OSM via Overpass and returns entries that
appear to have no website but do have useful contact/social details.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import urllib.parse
import urllib.request
from urllib.error import URLError
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
USER_AGENT = "us-business-leads-collector/1.0 (contact: local-script)"


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


def http_get_json(url: str, params: Dict[str, str]) -> Dict:
    query = urllib.parse.urlencode(params)
    req = urllib.request.Request(
        f"{url}?{query}", headers={"User-Agent": USER_AGENT}
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except URLError as exc:
        raise RuntimeError(f"Request failed for {url}: {exc}") from exc


def http_post_json(url: str, data: str) -> Dict:
    encoded = data.encode("utf-8")
    req = urllib.request.Request(
        url,
        data=encoded,
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": USER_AGENT,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except URLError as exc:
        raise RuntimeError(f"Request failed for {url}: {exc}") from exc


def get_state_area_id(state: str) -> int:
    payload = {
        "q": f"{state}, United States",
        "format": "jsonv2",
        "limit": "10",
        "addressdetails": "1",
        "extratags": "1",
    }
    data = http_get_json(NOMINATIM_URL, payload)

    for item in data:
        if item.get("osm_type") == "relation":
            add = item.get("address", {})
            country = (add.get("country") or "").lower()
            if "united states" in country or add.get("country_code") == "us":
                return 3600000000 + int(item["osm_id"])

    raise RuntimeError(f"Could not resolve a relation area for state: {state}")


def build_overpass_query(area_id: int, limit: int) -> str:
    # "website" and "contact:website" both represent business websites in OSM.
    # Query amenity/shop/office entries that DO NOT include website fields.
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


def pick(tags: Dict[str, str], *keys: str) -> str:
    for key in keys:
        value = tags.get(key)
        if value:
            return value.strip()
    return ""


def extract_lead(element: Dict) -> Optional[Lead]:
    tags = element.get("tags", {})

    phone = pick(tags, "phone", "contact:phone", "mobile", "contact:mobile")
    email = pick(tags, "email", "contact:email")
    facebook = pick(tags, "facebook", "contact:facebook")
    instagram = pick(tags, "instagram", "contact:instagram")

    if not any([phone, email, facebook, instagram]):
        return None

    address = tags
    lat = str(element.get("lat") or element.get("center", {}).get("lat") or "")
    lon = str(element.get("lon") or element.get("center", {}).get("lon") or "")

    return Lead(
        business_name=pick(tags, "name"),
        phone=phone,
        email=email,
        facebook=facebook,
        instagram=instagram,
        city=pick(address, "addr:city", "city"),
        state=pick(address, "addr:state", "state"),
        country=pick(address, "addr:country", "country", "addr:country_code"),
        osm_type=element.get("type", ""),
        osm_id=int(element.get("id", 0)),
        lat=lat,
        lon=lon,
    )


def dedupe(leads: Iterable[Lead]) -> List[Lead]:
    seen = set()
    unique: List[Lead] = []
    for lead in leads:
        key = (
            lead.business_name.lower(),
            lead.phone,
            lead.email.lower(),
            lead.facebook.lower(),
            lead.instagram.lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(lead)
    return unique


def save_csv(leads: List[Lead], output: str) -> None:
    fields = [
        "business_name",
        "phone",
        "email",
        "facebook",
        "instagram",
        "city",
        "state",
        "country",
        "osm_type",
        "osm_id",
        "lat",
        "lon",
    ]
    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for lead in leads:
            writer.writerow(lead.__dict__)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Collect business contacts from OSM in a U.S. state where website fields are missing."
        )
    )
    parser.add_argument("--state", required=True, help="State name, e.g. Texas")
    parser.add_argument("--limit", type=int, default=5000, help="Max OSM rows to fetch")
    parser.add_argument("--output", default="leads.csv", help="CSV output path")
    parser.add_argument(
        "--sleep",
        type=float,
        default=1.0,
        help="Optional pause before Overpass request (good API etiquette)",
    )

    args = parser.parse_args()

    try:
        print(f"Resolving OSM area for {args.state}...")
        area_id = get_state_area_id(args.state)
        print(f"Area id: {area_id}")

        if args.sleep > 0:
            time.sleep(args.sleep)

        print("Querying Overpass (this can take a while)...")
        query = build_overpass_query(area_id=area_id, limit=args.limit)
        data = http_post_json(OVERPASS_URL, data=query)

        elements = data.get("elements", [])
        leads = [lead for e in elements if (lead := extract_lead(e))]
        leads = dedupe(leads)
        save_csv(leads, args.output)

        print(f"Wrote {len(leads)} leads to {args.output}")
        return 0
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
