#!/usr/bin/env python3
"""Jour 1 — relevé des jobs Spark Web UI pour la lecture du CSV des vols et de la table Bronze.

Rejoue les lectures du notebook J1 (même configuration local[2]) sans rien écrire sur HDFS, puis
interroge l'API REST de Spark Web UI (celle qui alimente http://localhost:4040) pour enregistrer
jobs, stages, tâches et volumes lus dans reports/j1_spark_ui.json.

Usage : docker exec spark-jupyter python /workspace/scripts/spark_ui_j1.py
"""
import json
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

FLIGHTS_CSV = '/lakehouse/bronze/raw/flight-delays/flights.csv'
BRONZE_FLIGHTS = '/lakehouse/bronze/flights'
REPORT = Path('/workspace/reports/j1_spark_ui.json')

spark = (SparkSession.builder.appName('J1_Spark_UI').master('local[2]')
         .config('spark.sql.shuffle.partitions', '8').getOrCreate())
sc = spark.sparkContext
sc.setLogLevel('ERROR')
ui_url = sc.uiWebUrl
api = f'{ui_url}/api/v1/applications/{sc.applicationId}'


def get(path):
    with urllib.request.urlopen(f'{api}/{path}', timeout=30) as response:
        return json.load(response)


def run(label, action):
    """Exécute une action Spark sous un groupe de jobs nommé, pour la retrouver dans Spark UI."""
    sc.setJobGroup(label, label)
    value = action()
    sc.setJobGroup('', '')
    return value


try:
    # 1. Évaluation paresseuse. La lecture avec en-tête lance un petit job (lecture de la 1re ligne
    #    pour nommer les colonnes) ; filter/select ne font que compléter le plan, sans aucun job.
    df = spark.read.option('header', True).csv(FLIGHTS_CSV)
    jobs_after_read = len(get('jobs'))
    filtered = df.filter(F.col('cancelled') == '0').select('airline', 'departure_delay')
    jobs_after_transformations = len(get('jobs'))

    # 2. Actions observées dans Spark UI (onglets Jobs et Stages).
    rows = run('J1 count CSV vols', df.count)
    partitions = df.rdd.getNumPartitions()
    run('J1 count filtre narrow', filtered.count)
    run('J1 groupBy compagnie (wide)', lambda: filtered.groupBy('airline').count().collect())
    bronze_rows = run('J1 count Bronze Delta', lambda: spark.read.format('delta').load(BRONZE_FLIGHTS).count())

    jobs = sorted(get('jobs'), key=lambda j: j['jobId'])
    stages = {s['stageId']: s for s in get('stages')}
    observed = []
    for job in jobs:
        job_stages = [stages[i] for i in job['stageIds'] if i in stages]
        observed.append({
            'job_id': job['jobId'], 'group': job.get('jobGroup', ''), 'status': job['status'],
            'stages': len(job['stageIds']),
            'stages_skipped': job.get('numSkippedStages', 0),
            # numTasks inclut les tâches des stages ignorés (skipped), déjà calculés par un job précédent.
            'tasks_executed': job['numTasks'] - job.get('numSkippedTasks', 0),
            'tasks_skipped': job.get('numSkippedTasks', 0),
            'input_mib': round(sum(s.get('inputBytes', 0) for s in job_stages) / 2**20, 1),
            'shuffle_write_kib': round(sum(s.get('shuffleWriteBytes', 0) for s in job_stages) / 2**10, 1),
            'shuffle_read_kib': round(sum(s.get('shuffleReadBytes', 0) for s in job_stages) / 2**10, 1),
        })
    by_group = {}
    for job in observed:
        g = by_group.setdefault(job['group'] or "(lecture de l'en-tête CSV)",
                                {'jobs': 0, 'tasks_executed': 0, 'input_mib': 0.0, 'shuffle_write_kib': 0.0})
        g['jobs'] += 1
        g['tasks_executed'] += job['tasks_executed']
        g['input_mib'] = round(g['input_mib'] + job['input_mib'], 1)
        g['shuffle_write_kib'] = round(g['shuffle_write_kib'] + job['shuffle_write_kib'], 1)

    report = {
        'checked_at': datetime.now(UTC).isoformat(),
        'spark_ui': 'http://localhost:4040 (port publié par le conteneur spark-jupyter)',
        'application': sc.applicationId,
        'jobs_after_csv_read_header': jobs_after_read,
        'jobs_after_filter_select': jobs_after_transformations,
        'csv_rows': rows, 'bronze_rows': bronze_rows, 'csv_read_partitions': partitions,
        'by_action': by_group, 'jobs': observed,
    }
    assert rows == bronze_rows == 1943809
    REPORT.write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps({k: v for k, v in report.items() if k != 'jobs'}, indent=2, ensure_ascii=False))
finally:
    spark.stop()
