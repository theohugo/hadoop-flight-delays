#!/usr/bin/env python3
"""Orchestration depuis l'hôte Windows ; redémarre toujours datanode2."""
import json
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]


def docker(*args):
    return subprocess.check_output(["docker", *args], encoding="utf-8", errors="replace", timeout=240)


def state():
    with urlopen("http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem", timeout=10) as response:
        bean = json.load(response)["beans"][0]
    keys = ("NumLiveDataNodes", "NumDeadDataNodes", "UnderReplicatedBlocks", "MissingBlocks", "BlocksTotal")
    return {k: bean[k] for k in keys}


def wait_for(predicate, observations, started, timeout=150):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = state()
        observations.append({"elapsed_seconds": round(time.monotonic() - started, 1), **current})
        if predicate(current):
            return current
        time.sleep(3)
    raise TimeoutError("État HDFS attendu non atteint ; consulter les observations du rapport.")


def main():
    report = {"started_at": datetime.now(UTC).isoformat(), "observations": []}
    started = time.monotonic()
    report["before"] = state()
    assert report["before"]["NumLiveDataNodes"] == 3
    assert report["before"]["BlocksTotal"] > 0, "Exécuter le notebook J1 avant le crash-test."
    try:
        print("Arrêt de datanode2 ; attente de la détection par le NameNode...", flush=True)
        docker("stop", "datanode2")
        report["detected"] = wait_for(lambda s: s["NumDeadDataNodes"] >= 1 and s["NumLiveDataNodes"] == 2,
                                      report["observations"], started)
        report["detection_seconds"] = round(time.monotonic() - started, 1)
        print("Panne détectée ; lecture Spark sur les deux survivants...", flush=True)
        output = docker("exec", "spark-jupyter", "python", "/workspace/scripts/read_flights_hdfs.py")
        marker = next(line for line in output.splitlines() if line.startswith("FLIGHTS_READ_OK="))
        report["read_during_outage"] = json.loads(marker.split("=", 1)[1])
        report["repaired_while_down"] = wait_for(
            lambda s: s["UnderReplicatedBlocks"] == 0 and s["MissingBlocks"] == 0,
            report["observations"], started)
        report["fsck_while_down"] = docker("exec", "namenode", "hdfs", "fsck", "/lakehouse/bronze/raw/flight-delays", "-files", "-blocks", "-locations")
        report["success"] = True
    except Exception as exc:
        report["success"] = False
        report["error"] = str(exc)
        raise
    finally:
        print("Redémarrage de datanode2...", flush=True)
        docker("start", "datanode2")
        try:
            report["after_restore"] = wait_for(lambda s: s["NumLiveDataNodes"] == 3,
                                                report["observations"], started)
        finally:
            (ROOT / "reports/crash_test_flights.json").write_text(
                json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print("Crash-test réussi, rapport : reports/crash_test_flights.json", flush=True)


if __name__ == "__main__":
    main()
