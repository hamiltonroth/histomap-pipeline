# histomap-pipeline

Data pipeline for [HistoMap](https://github.com/rothhamilton/histomap) — fetches historical places from Wikidata, enriches them with Wikipedia summaries and Wikimedia thumbnails, and publishes a PMTiles file to Cloudflare R2.

## What it does

1. Queries Wikidata SPARQL for 10 categories of European historical places (castles, battlefields, shipwrecks, churches, monasteries, fortifications, archaeological sites, sieges, bridges, naval battles)
2. Enriches each record with a Wikipedia extract and a Wikimedia Commons thumbnail
3. Writes a GeoJSON FeatureCollection
4. Runs [tippecanoe](https://github.com/felt/tippecanoe) to produce a PMTiles file (zoom 2–16)
5. Uploads atomically to Cloudflare R2 — the live file is never removed until the new one is confirmed

Runs weekly via GitHub Actions (Sunday 03:00 UTC). Unlimited free minutes on a public repo.

## Local setup

```bash
pip install -e ".[dev]"
pytest
```

## Running the pipeline locally

Set the four R2 environment variables, then:

```bash
python -m pipeline.main
```

Required env vars: `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT_URL`, `R2_BUCKET`

## Adding categories

Edit `config/categories.yml` — add a new entry with a Wikidata QID. No code changes needed.
