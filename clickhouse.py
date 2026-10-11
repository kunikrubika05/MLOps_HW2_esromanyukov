import argparse
import csv
import json
import os
import statistics
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

SQL_DIR = Path(__file__).resolve().parent


def execute(
    sql: str,
) -> tuple[str, dict[str, str]]:
    url = os.environ.get('CLICKHOUSE_URL', 'http://clickhouse:8123')
    parameters = urlencode(
        {
            'database': 'fraud',
            'max_threads': 2,
            'output_format_json_quote_decimals': 1,
        }
    )
    request = Request(
        f'{url}/?{parameters}',
        data=sql.encode(),
        headers={
            'X-ClickHouse-User': os.environ.get('CLICKHOUSE_USER', 'fraud'),
            'X-ClickHouse-Key': os.environ.get(
                'CLICKHOUSE_PASSWORD', 'local-demo-password'
            ),
        },
    )
    try:
        with urlopen(request, timeout=120) as response:
            return response.read().decode(), dict(response.headers)
    except HTTPError as error:
        raise RuntimeError(error.read().decode()) from error


def query(
    sql: str,
) -> list[dict[str, object]]:
    result, _ = execute(sql.rstrip().rstrip(';') + ' format JSONEachRow')
    return [json.loads(line) for line in result.splitlines()]


def read_sql(
    filename: str,
) -> str:
    return (SQL_DIR / filename).read_text().strip().rstrip(';')


def benchmark(
    output: Path,
    runs: int,
) -> None:
    if runs < 3:
        raise ValueError('Use at least three measured runs')
    count = int(query('select count() as rows from fraud.transactions')[0]['rows'])
    if not count:
        raise ValueError('Load and verify a dataset first')
    execute('truncate table fraud.baseline')
    execute('insert into fraud.baseline select * from fraud.transactions')
    for table in ('baseline', 'transactions'):
        execute(f'optimize table fraud.{table} final')
    queries = {
        'baseline': read_sql('max_category.sql').replace(
            'fraud.transactions', 'fraud.baseline'
        ),
        'sorted_low_cardinality': read_sql('max_category.sql'),
    }
    results = [query(sql) for sql in queries.values()]
    if results[0] != results[1]:
        raise RuntimeError('Benchmark queries disagree')
    measurements = []
    labels = list(queries)
    for iteration in range(runs + 2):
        offset = iteration % len(labels)
        for label in labels[offset:] + labels[:offset]:
            start = time.perf_counter()
            _, headers = execute(queries[label] + ' format CSVWithNames')
            elapsed = time.perf_counter() - start
            summary = json.loads(headers.get('X-ClickHouse-Summary', '{}'))
            if iteration >= 2:
                measurements.append(
                    {
                        'variant': label,
                        'run': iteration - 1,
                        'elapsed_seconds': elapsed,
                        'read_rows': int(summary.get('read_rows', 0)),
                        'read_bytes': int(summary.get('read_bytes', 0)),
                    }
                )
    output.mkdir(parents=True, exist_ok=True)
    storage = query(
        'select table, sum(rows) as rows, sum(data_compressed_bytes) as compressed, '
        'sum(data_uncompressed_bytes) as uncompressed, sum(bytes_on_disk) as disk '
        "from system.parts where active and database = 'fraud' "
        "and table in ('baseline', 'transactions') "
        'group by table order by table'
    )
    medians = {
        label: statistics.median(
            row['elapsed_seconds'] for row in measurements if row['variant'] == label
        )
        for label in labels
    }
    report = {
        'clickhouse_version': query('select version() as version')[0]['version'],
        'source_rows': count,
        'runs': runs,
        'warmups': 2,
        'max_threads': 2,
        'timing': 'HTTP request and response, warm cache, rotated query order',
        'median_seconds': medians,
        'baseline_to_optimized_speedup': medians['baseline']
        / medians['sorted_low_cardinality'],
        'storage': storage,
        'results_equal': True,
        'measurements': measurements,
    }
    (output / 'benchmark.json').write_text(json.dumps(report, indent=2) + '\n')
    print(f'Benchmark saved to {output}')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', type=Path, required=True)
    args = parser.parse_args()
    with args.csv.open(newline='', encoding='utf-8-sig') as stream:
        count = sum(1 for _ in csv.DictReader(stream))
    if not count:
        raise ValueError('CSV is empty')
    deadline = time.monotonic() + 180
    while True:
        loaded = int(query('select count() as rows from fraud.transactions')[0]['rows'])
        if loaded == count:
            break
        if loaded > count:
            raise RuntimeError('Extra rows: reset the project before loading')
        if time.monotonic() >= deadline:
            raise TimeoutError(f'ClickHouse contains {loaded}/{count} rows')
        time.sleep(1)
    output = Path('/results')
    output.mkdir(parents=True, exist_ok=True)
    result, _ = execute(read_sql('max_category.sql') + ' format CSVWithNames')
    (output / 'max_category.csv').write_text(result)
    benchmark(output, 9)


if __name__ == '__main__':
    main()
