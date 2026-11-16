#!/usr/bin/env python3
"""Préparer un extrait reproductible, sans modifier le cache Kaggle original."""
import csv
import hashlib
import json
import shutil
from collections import Counter
from pathlib import Path

import kagglehub

ROOT = Path(__file__).resolve().parents[1]
MONTHS = {1, 4, 7, 11}


def main():
    source = Path(kagglehub.dataset_download("usdot/flight-delays/versions/1"))
    target = ROOT / "data_raw" / "flight-delays"
    target.mkdir(parents=True, exist_ok=True)
    counts = Counter()
    with (source / "flights.csv").open(newline="", encoding="utf-8-sig") as src:
        reader = csv.DictReader(src)
        with (target / "flights.csv").open("w", newline="", encoding="utf-8") as dst:
            writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
            writer.writeheader()
            for row in reader:
                month = int(row["MONTH"])
                if month in MONTHS:
                    writer.writerow(row)
                    counts[month] += 1
    for name in ("airlines.csv", "airports.csv"):
        shutil.copy2(source / name, target / name)
    files = []
    for path in sorted(target.glob("*.csv")):
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        files.append({"name": path.name, "bytes": path.stat().st_size, "sha256": digest})
    manifest = {
        "dataset": "usdot/flight-delays", "version": 1,
        "url": "https://www.kaggle.com/datasets/usdot/flight-delays",
        "year": 2015, "months": sorted(MONTHS),
        "selection": "Tous les vols de janvier, avril, juillet et novembre ; aucune sélection selon le retard.",
        "limitations": "Quatre mois seulement : résultats descriptifs de cet extrait, pas des estimations annuelles. Novembre inclut Thanksgiving. Octobre exclu : codes aéroports numériques dans la source.",
        "rows_by_month": dict(sorted(counts.items())), "flight_rows": sum(counts.values()),
        "files": files,
    }
    out = ROOT / "reports"
    out.mkdir(exist_ok=True)
    (out / "data_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(manifest, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
