## U.S. Business Lead Collector (No Website)

This repository includes a script that pulls public business records from OpenStreetMap and exports leads where website fields are missing.

### Output columns
- business_name
- phone / mobile number
- email
- facebook
- instagram
- city, state, country
- OSM metadata (type, id, lat, lon)

### Usage
```bash
python3 collect_us_business_leads.py --state "Texas" --limit 8000 --output texas_leads.csv
```

### Notes
- Data quality depends on what is available in OpenStreetMap.
- Missing website in OSM means `website`/`contact:website` tags are absent; some businesses might still have a site not listed in OSM.
- For nationwide coverage, run once per state and merge the CSV files.
