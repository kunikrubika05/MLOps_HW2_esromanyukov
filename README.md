# Транзакции в ClickHouse

Загрузка: CSV → Kafka → ClickHouse. [Таблицы](init.sql) · [Запрос](max_category.sql) · [Результат](results/max_category.csv).

Нужны Git, Docker и Compose v2.

```sh
git clone https://github.com/kunikrubika05/MLOps_HW2_esromanyukov.git
cd MLOps_HW2_esromanyukov
docker compose build tools
```

Положите `test.csv` из [соревнования](https://www.kaggle.com/competitions/teta-ml-1-2025) в папку `data/` (создайте её).

```sh
docker compose up -d --wait
docker compose run --rm --no-deps tools python produce.py --csv /data/test.csv
docker compose run --rm --no-deps tools python clickhouse.py --csv /data/test.csv
```

Запрос находит категорию максимальной транзакции каждого штата. Пустая категория сохраняется; при равных суммах выбирается последняя строка CSV.

Сортировка и `LowCardinality` ускорили запрос **в 1,21 раза** и уменьшили хранение **на 18,39%** относительно таблицы без сортировки и `LowCardinality`. Проверено на 262144 транзакциях: 9 запусков каждого запроса. [Результаты измерений](results/benchmark.json).

Остановка: `docker compose stop`. Перед повторной загрузкой выполните `docker compose down -v`, затем `docker compose up -d --wait`: сброс удаляет данные проекта и предотвращает дубли от повторной отправки.
