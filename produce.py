import argparse
import csv
import json
import os
from datetime import datetime
from pathlib import Path

from confluent_kafka import KafkaError, Message, Producer


def publish(
    path: Path,
) -> None:
    producer = Producer(
        {
            'bootstrap.servers': os.environ.get(
                'KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'
            ),
            'enable.idempotence': True,
            'delivery.timeout.ms': 60000,
        }
    )
    errors: list[str] = []

    def on_delivery(
        error: KafkaError | None,
        message: Message,
    ) -> None:
        if error is not None:
            errors.append(str(error))

    count = 0
    with path.open(newline='', encoding='utf-8-sig') as stream:
        for count, row in enumerate(csv.DictReader(stream), start=1):
            transaction: dict[str, object] = dict(row)
            transaction['transaction_time'] = datetime.fromisoformat(
                row['transaction_time']
            ).strftime('%Y-%m-%d %H:%M:%S')
            for field in ('lat', 'lon', 'merchant_lat', 'merchant_lon'):
                transaction[field] = float(row[field])
            transaction['population_city'] = int(row['population_city'])
            transaction['cat_id'] = row['cat_id'] or None
            transaction['target'] = int(row['target']) if row.get('target') else None
            transaction['source_row'] = count
            payload = json.dumps(transaction, allow_nan=False).encode()
            while True:
                try:
                    producer.produce('transactions', payload, on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.1)
            producer.poll(0)
            if errors:
                raise RuntimeError(errors[0])
    remaining = producer.flush(65)
    if errors or remaining:
        raise RuntimeError(errors[0] if errors else 'Kafka delivery timed out')
    print(f'Sent {count} transactions')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', type=Path, required=True)
    args = parser.parse_args()
    publish(args.csv)


if __name__ == '__main__':
    main()
