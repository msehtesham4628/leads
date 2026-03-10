## U.S. Business Lead Collector (No Website)

This repo provides a CLI to export public business leads from OpenStreetMap where `website` / `contact:website` tags are missing.

### Exported fields
- business_name
- phone / mobile number
- email
- facebook
- instagram
- city, state, country
- osm_type, osm_id, lat, lon

### Usage
Single state:
```bash
python3 collect_us_business_leads.py --state "Texas" --limit 8000 --output texas_leads.csv
```

All U.S. states + DC:
```bash
python3 collect_us_business_leads.py --all-states --limit 4000 --output usa_leads.csv --sleep 1.5
```

### Improvements in this version
- Supports `--all-states` for nationwide collection.
- Uses retry/backoff for transient network/API errors.
- Falls back across multiple Overpass endpoints.
- Uses proper Overpass form payload (`data=<query>`), improving compatibility.

### Notes
- Data quality depends on OpenStreetMap tagging completeness.
- A missing OSM website tag does **not** guarantee the business has no website in reality.
- Respect API limits; keep `--sleep` above zero for large runs.
