# Команды запуска ДЗ 4

Все команды выполняются из корня существующего проекта:

```bash
cd /Users/artemmeteorolog/data_engineering/hw3_MapReduce/MapReduce_lab
uv sync --locked
docker compose up -d rustfs
docker compose -f compose.zookeeper.yaml up -d --wait --wait-timeout 90
```

Настройки RustFS и ZooKeeper читаются из `.env`. Значения, заданные в командной
строке или экспортированные в окружение, имеют приоритет. Compose публикует
порты `ZK_PORT_1`, `ZK_PORT_2`, `ZK_PORT_3`. Локальные клиенты используют эти же
порты на `127.0.0.1`. Для подключения по другим адресам задайте `ZK_HOSTS`.
В `.env.example` приведены параметры без действующих ключей RustFS.

ZooKeeper 3.9.6 запускается с Java 17 внутри контейнеров; локальные Java и
`JAVA_HOME` для Docker-варианта не нужны. Образ создаёт `/conf/zoo.cfg` и
`/data/myid` из переменных окружения. У каждого сервера свои тома `/data` и
`/datalog`; внутренние порты могут совпадать благодаря отдельным контейнерам.
Наружу доступны только клиентские порты на `127.0.0.1`.

| Подключение | ZooKeeper | RustFS |
| --- | --- | --- |
| С компьютера | `127.0.0.1:22181,127.0.0.1:22182,127.0.0.1:22183` | `http://127.0.0.1:9010` |
| Из сети compose ZooKeeper | `zoo1:2181,zoo2:2181,zoo3:2181` | Другая compose-сеть |
| Из контейнера на Docker Desktop | `host.docker.internal:22181,host.docker.internal:22182,host.docker.internal:22183` | `http://host.docker.internal:9010` |

Координаторы в этих сценариях запускаются на компьютере. Их общий получатель —
**один файл** `results/hw4/experiments/<RUN_ID>/shared.sqlite3`. Он хранит эпоху,
чекпоинт и принятые публикации. Отдельные копии этой базы использовать нельзя.
Решение рассчитано на два процесса на одном компьютере; запуск на разных
компьютерах потребует сетевого получателя и общей СУБД. SQLite-базу не следует
переносить на сетевую файловую систему.

## Ансамбль и эфемерный узел

```bash
uv run scripts/hw4/check_ensemble.py
docker compose -f compose.zookeeper.yaml exec zoo1 java -version
docker compose -f compose.zookeeper.yaml exec zoo1 cat /conf/zoo.cfg /data/myid
docker compose -f compose.zookeeper.yaml exec zoo2 cat /conf/zoo.cfg /data/myid
docker compose -f compose.zookeeper.yaml exec zoo3 cat /conf/zoo.cfg /data/myid
```

Сценарий проверяет роли через `srvr`, запись при трёх и двух серверах,
недоступность записи при одном сервере, возвращает `zoo2` и `zoo3` в работу
и проверяет исчезновение ephemeral-узла после закрытия сессии.
Он временно останавливает только ZooKeeper этого проекта.

Для последующей демонстрации в двух окнах `zkCli`:

```bash
docker compose -f compose.zookeeper.yaml exec zoo1 zkCli.sh -server zoo1:2181,zoo2:2181,zoo3:2181
```

В первом клиенте: `create -e /hw4-demo temporary`. Во втором: `get /hw4-demo`.
Затем завершите первый клиент командой `quit` или закройте его окно.
Повторный `get /hw4-demo` во втором клиенте должен вернуть `Node does not exist`;
после обрыва соединения дождитесь истечения сессии.

## Пауза лидера

Каждый запуск требует нового `RUN_ID`. Сценарий не перезаписывает существующие
каталоги результатов или журналов. Для обычных 6100 строк подходят параметры
по умолчанию: чанк 250 строк, задержка 0.5 секунды после чекпоинта, пауза 40 секунд.
Задержка нужна, чтобы короткий расчёт можно было приостановить посередине.

```bash
RUN_ID=manual-no-fencing-01 FENCING=0 uv run scripts/hw4/run_experiment.py
RUN_ID=manual-fencing-01 FENCING=1 uv run scripts/hw4/run_experiment.py
```

Сценарий запускает `c1`, ждёт первого непустого чекпоинта, посылает ему настоящий
`SIGSTOP`, запускает `c2`, ждёт восстановления и завершения расчёта, затем
возобновляет `c1` через `SIGCONT`. Пауза длится не менее `PAUSE_SECONDS` и всегда
до завершения запасного координатора. Время переключения считается от отправки
`SIGSTOP` до регистрации новой эпохи в получателе, без времени завершения расчёта.

Оба режима используют одинаковые расчёты и защиту чекпоинтов от старой эпохи.
Старому процессу намеренно разрешено закончить оставшуюся работу в памяти и
попытаться опубликовать результат. При `FENCING=0` получатель принимает две
публикации; при `FENCING=1` отклоняет старую эпоху. Проверка эпохи и запись
результата выполняются в одной транзакции SQLite. Повтор запроса с той же
эпохой идемпотентен. Новая эпоха берётся из `czxid` ephemeral sequential узла;
чекпоинт содержит следующий номер строки и накопленные частоты.

При восстановлении CSV снова читается из S3, но уже сохранённые строки
пропускаются без повторного расчёта. ETag передаётся в `GetObject` как `IfMatch`;
объект, параметры чанков и путь выборов привязаны к общей базе. Расчёт пар
использует маппер и формат CSV ДЗ 3, выполняя чанки последовательно для простого
упорядоченного чекпоинта. Команды ДЗ 3 и их параллельный расчёт сохранены.

Ручной запуск двух процессов вместо сценария (в двух терминалах):

```bash
RUN_ID=manual-pair-01 COORDINATOR_ID=c1 FENCING=1 uv run python -m map_reduce.coordination.coordinator
RUN_ID=manual-pair-01 COORDINATOR_ID=c2 FENCING=1 uv run python -m map_reduce.coordination.coordinator
```

PID лидера и прогресс видны в `LEADER` и `CHECKPOINT` в журнале соответствующего
координатора. Для ручной паузы применяются `kill -STOP PID` и `kill -CONT PID`,
где `PID` заменяется фактическим номером процесса из журнала.

## Параметры

| Переменная | Значение по умолчанию / назначение |
| --- | --- |
| `RUN_ID` | Обязательное уникальное имя эксперимента: буквы, цифры, `_`, `-` |
| `COORDINATOR_ID` | Обязательное имя процесса; сценарий задаёт `c1`, `c2` |
| `FENCING` | `1`; допустимы только `0` и `1`, одинаково для обоих процессов |
| `ZK_HOSTS` | Необязательная строка подключения для переопределения локальных адресов |
| `ZK_NAMESPACE` | `/mapreduce-hw4` |
| `ZK_PORT_1`, `ZK_PORT_2`, `ZK_PORT_3` | `22181`, `22182`, `22183`; порты Compose и локальных клиентов |
| `CHUNK_SIZE` | `250`; одинаково для обоих процессов |
| `CHUNK_DELAY` | `0.5` секунды; для сценария паузы должен быть больше нуля |
| `PAUSE_SECONDS` | `40`; используется сценарием, допустимо от 30 до 300 |
| `S3_ENDPOINT` | Из существующего `.env`, обычно `http://127.0.0.1:9010` |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | Существующие секреты RustFS, не выводятся в журналы |

Тайм-аут сессии координаторов — 10 секунд. Серверные `minSessionTimeout` и
`maxSessionTimeout` также равны 10000 мс. После полного сброса состояния
ZooKeeper используйте новый `RUN_ID`, чтобы не смешивать историю эпох.

## Результаты и проверки

* `logs/hw4/<RUN_ID>/c1.jsonl`, `c2.jsonl` — события, эпохи, PID, чекпоинты и публикации.
* `results/hw4/experiments/<RUN_ID>/experiment.json` — измерения и исход сценария.
* `publication-NN/counts_k2.csv`, `run.json`, `verification.json` внутри каталога
  эксперимента — принятый результат, контрольные числа и точная сверка с ДЗ 3.
* `results/hw4/command_outputs/` — сохранённый вывод фактических запусков.

Сценарий проверяет каждую принятую публикацию по всем ключам и частотам, а также
числу строк и вакансий с навыками. CSV и эталон ДЗ 3 не перезаписываются.
База SQLite — рабочее состояние, исключённое из Git; для сдачи используются
экспортированные результаты, сверки и журналы.

```bash
uv run python -m unittest discover -s tests -v
uv run ruff check .
uv run ruff format --check src/map_reduce/coordination src/map_reduce/processing/mapreduce.py src/map_reduce/infrastructure/s3.py scripts/hw4
```

Остановка без удаления данных:

```bash
docker compose -f compose.zookeeper.yaml stop
docker compose stop rustfs
```

Параметры образа: [официальная документация Docker ZooKeeper](https://github.com/docker-library/docs/tree/master/zookeeper).
API клиента: [Kazoo](https://kazoo.readthedocs.io/en/stable/api/client.html).

Отчёт, скриншоты и публикация на GitHub на этом этапе не выполняются.
