# Команды

## 1. Кластер

```bash
kind create cluster --config kubernetes/manifests/kind.yaml --wait 180s
kubectl --context kind-de-k8s-lab apply -f kubernetes/manifests/namespace.yaml
```

## 2. Образ

```bash
docker build -t mapreduce-lab:k8s \
  --build-arg SOURCE_ID=8afa5252bc95a489feac6f85fc4e1bf0158d38a1ca20671792319f7316bcc79d .

kind load docker-image mapreduce-lab:k8s --name de-k8s-lab
```

### Проверка входа

```bash
docker compose up -d rustfs # запустить RustFS
kubectl --context kind-de-k8s-lab create -f kubernetes/manifests/input-check.yaml # запустить проверку
kubectl --context kind-de-k8s-lab -n de-k8s-lab wait --for=condition=complete job/input-check-run-00-0001 --timeout=120s # дождаться Job
kubectl --context kind-de-k8s-lab -n de-k8s-lab logs job/input-check-run-00-0001
```

## 3. Память

```bash
kubectl --context kind-de-k8s-lab create -f kubernetes/results/memory/01-memory/run-01-0001/job.json # запустить Job
kubectl --context kind-de-k8s-lab -n de-k8s-lab logs job/pairs-run-01-0001

kubectl --context kind-de-k8s-lab create -f kubernetes/results/memory/01-memory/run-01-0002/job.json # запустить Job
kubectl --context kind-de-k8s-lab -n de-k8s-lab logs job/pairs-run-01-0002
kubectl --context kind-de-k8s-lab -n de-k8s-lab get pods -l job-name=pairs-run-01-0002 -o json # сохранить статус Pod
kubectl --context kind-de-k8s-lab -n de-k8s-lab describe pod -l job-name=pairs-run-01-0002 # сохранить причину остановки

kubectl --context kind-de-k8s-lab create -f kubernetes/results/memory/01-memory/run-01-0003/job.json # запустить Job
kubectl --context kind-de-k8s-lab -n de-k8s-lab logs job/pairs-run-01-0003
```

## 4. Перенос RustFS

```bash
kubectl --context kind-de-k8s-lab apply -f kubernetes/manifests/rustfs.yaml # развернуть RustFS
kubectl --context kind-de-k8s-lab create -f kubernetes/manifests/storage-client.yaml # запустить клиент

kubectl --context kind-de-k8s-lab -n de-k8s-lab get pod/rustfs-7c4ddb58b4-l6mpk pvc/rustfs-data -o json # снять состояние до
kubectl --context kind-de-k8s-lab -n de-k8s-lab exec -i rustfs-client -- python - copy run-02-0001 a900fbbf01d5f64bdf3719261ecbdcccfafcf24002b48e8a29f14e9f709ec04c < kubernetes/scripts/storage_transfer.py # перенести CSV
kubectl --context kind-de-k8s-lab -n de-k8s-lab rollout restart deployment/rustfs # пересоздать Pod
kubectl --context kind-de-k8s-lab -n de-k8s-lab rollout status deployment/rustfs --timeout=90s # дождаться нового Pod
kubectl --context kind-de-k8s-lab -n de-k8s-lab get pod/rustfs-58fb58687c-jxsfc pvc/rustfs-data -o json # снять состояние после
kubectl --context kind-de-k8s-lab -n de-k8s-lab exec -i rustfs-client -- python - readback run-02-0001 a900fbbf01d5f64bdf3719261ecbdcccfafcf24002b48e8a29f14e9f709ec04c < kubernetes/scripts/storage_transfer.py # проверить данные
kubectl --context kind-de-k8s-lab -n de-k8s-lab delete pod rustfs-client # удалить клиент
```

## 5. Параллельный расчёт

```bash
kubectl --context kind-de-k8s-lab create -f kubernetes/results/parallel/03-parallel/run-03-0001/split-job.json # разделить CSV
kubectl --context kind-de-k8s-lab -n de-k8s-lab logs job/split-run-03-0001

kubectl --context kind-de-k8s-lab create -f kubernetes/results/parallel/03-parallel/run-03-0001/workers-job.json # посчитать части
kubectl --context kind-de-k8s-lab -n de-k8s-lab logs -l job-name=workers-run-03-0001 --tail=-1 # прочитать лог
kubectl --context kind-de-k8s-lab -n de-k8s-lab get pods -l job-name=workers-run-03-0001 -o json # сохранить статусы worker

kubectl --context kind-de-k8s-lab create -f kubernetes/results/parallel/03-parallel/run-03-0001/reduce-job.json # объединить частоты
kubectl --context kind-de-k8s-lab -n de-k8s-lab wait --for=condition=complete job/reduce-run-03-0001 --timeout=90s # дождаться Job
kubectl --context kind-de-k8s-lab -n de-k8s-lab logs job/reduce-run-03-0001

kubectl --context kind-de-k8s-lab create -f kubernetes/results/parallel/03-parallel/run-03-0001/retry-job.json # повторить часть 0
kubectl --context kind-de-k8s-lab -n de-k8s-lab wait --for=condition=complete job/retry-run-03-0001 --timeout=90s # дождаться Job
kubectl --context kind-de-k8s-lab -n de-k8s-lab logs job/retry-run-03-0001

kubectl --context kind-de-k8s-lab create -f kubernetes/results/parallel/03-parallel/run-03-0001/reduce-repeat-job.json # повторить объединение
kubectl --context kind-de-k8s-lab -n de-k8s-lab wait --for=condition=complete job/reduce-repeat-run-03-0001 --timeout=90s # дождаться Job
kubectl --context kind-de-k8s-lab -n de-k8s-lab logs job/reduce-repeat-run-03-0001

kubectl --context kind-de-k8s-lab -n de-k8s-lab port-forward service/rustfs 19000:9000 # открыть порт в отдельном терминале
S3_ENDPOINT=http://127.0.0.1:19000 .venv/bin/python kubernetes/scripts/download_results.py kubernetes/parallel/run-03-0001/final kubernetes/results/parallel/03-parallel/run-03-0001 # скачать CSV
```

## 6. Ожидание CPU

```bash
kubectl --context kind-de-k8s-lab describe node de-k8s-lab-control-plane # снять заявки узла
kubectl --context kind-de-k8s-lab create -f kubernetes/results/scheduling/04-cpu-pending/run-04-0002/reserve.yaml # зарезервировать CPU
kubectl --context kind-de-k8s-lab -n de-k8s-scheduling wait --for=condition=Ready pod/cpu-reserve-run-04-0002 --timeout=60s # дождаться резерва
kubectl --context kind-de-k8s-lab create -f kubernetes/results/scheduling/04-cpu-pending/run-04-0002/job.json # запустить Job
kubectl --context kind-de-k8s-lab -n de-k8s-scheduling wait --for=create --for=condition=PodScheduled=false pod -l job-name=cpu-run-04-0002 --timeout=60s # дождаться Pending
kubectl --context kind-de-k8s-lab -n de-k8s-scheduling get pods -l job-name=cpu-run-04-0002 -o json # сохранить Pending
kubectl --context kind-de-k8s-lab -n de-k8s-scheduling delete pod cpu-reserve-run-04-0002 # освободить CPU
kubectl --context kind-de-k8s-lab -n de-k8s-scheduling wait --for=condition=Complete job/cpu-run-04-0002 --timeout=60s # дождаться расчёта
kubectl --context kind-de-k8s-lab -n de-k8s-scheduling get pods -l job-name=cpu-run-04-0002 -o json # сверить UID
kubectl --context kind-de-k8s-lab -n de-k8s-scheduling get events --field-selector involvedObject.uid=60596ad8-2a6a-4bc4-ba3d-a7ecb647e523 -o json # сохранить события
kubectl --context kind-de-k8s-lab -n de-k8s-scheduling logs job/cpu-run-04-0002 # сверить SHA-256
kubectl --context kind-de-k8s-lab -n de-k8s-scheduling delete job cpu-run-04-0002 # очистить Job
```

## 7. Вытеснение

```bash
kubectl --context kind-de-k8s-lab create -f kubernetes/results/scheduling/05-preemption/run-05-0001/priorities.json # создать приоритеты
kubectl --context kind-de-k8s-lab create -f kubernetes/results/scheduling/05-preemption/run-05-0001/low-job.json # запустить low
kubectl --context kind-de-k8s-lab -n de-k8s-scheduling wait --for=create --for=condition=Ready pod -l job-name=low-run-05-0001 --timeout=60s # дождаться low
kubectl --context kind-de-k8s-lab -n de-k8s-scheduling get pods -l job-name=low-run-05-0001 -o json # снять состояние low
kubectl --context kind-de-k8s-lab create -f kubernetes/results/scheduling/05-preemption/run-05-0001/high-job.json # запустить high
kubectl --context kind-de-k8s-lab -n de-k8s-scheduling wait --for=condition=Complete job/high-run-05-0001 --timeout=60s # дождаться результата
kubectl --context kind-de-k8s-lab -n de-k8s-scheduling get pods -l job-name=high-run-05-0001 -o json # сохранить итог high
kubectl --context kind-de-k8s-lab -n de-k8s-scheduling get pods -l job-name=low-run-05-0001 -o json # найти замену low
kubectl --context kind-de-k8s-lab -n de-k8s-scheduling get events --field-selector involvedObject.uid=283ed3be-fde5-4f74-914b-6640e4025f74 -o json # зафиксировать Preempted
kubectl --context kind-de-k8s-lab -n de-k8s-scheduling logs job/high-run-05-0001 # сверить SHA-256
kubectl --context kind-de-k8s-lab -n de-k8s-scheduling delete job high-run-05-0001 low-run-05-0001 # очистить Job
kubectl --context kind-de-k8s-lab delete priorityclass high-run-05-0001 low-run-05-0001 # удалить приоритеты
```

## 8. Квота ресурсов

```bash
kubectl --context kind-de-k8s-lab create namespace de-k8s-quota # создать namespace

kubectl --context kind-de-k8s-lab -n de-k8s-lab get secret rustfs-external -o json |
  jq '{apiVersion,kind,type,data,metadata:{name:.metadata.name,namespace:"de-k8s-quota"}}' |
  kubectl --context kind-de-k8s-lab apply -f - # подключить RustFS

kubectl --context kind-de-k8s-lab create -f kubernetes/results/scheduling/06-quota/run-06-0001/quota.json # создать квоту
kubectl --context kind-de-k8s-lab create -f kubernetes/results/scheduling/06-quota/run-06-0001/allowed.json # занять 1000m CPU
kubectl --context kind-de-k8s-lab -n de-k8s-quota wait --for=condition=Ready pod/allowed-run-06-0001 --timeout=60s # дождаться первого Pod
kubectl --context kind-de-k8s-lab -n de-k8s-quota get quota quota-run-06-0001 -o json # снять занятую квоту
kubectl --context kind-de-k8s-lab create -f kubernetes/results/scheduling/06-quota/run-06-0001/extra.json # получить отказ
kubectl --context kind-de-k8s-lab -n de-k8s-quota delete pod allowed-run-06-0001 # освободить квоту
kubectl --context kind-de-k8s-lab -n de-k8s-quota wait --for=jsonpath='{.status.used.requests\.cpu}'=0 quota/quota-run-06-0001 --timeout=60s # дождаться used=0
kubectl --context kind-de-k8s-lab -n de-k8s-quota get quota quota-run-06-0001 -o json # снять свободную квоту
kubectl --context kind-de-k8s-lab create -f kubernetes/results/scheduling/06-quota/run-06-0001/extra.json # повторить запрос
kubectl --context kind-de-k8s-lab -n de-k8s-quota wait --for=jsonpath='{.status.phase}'=Succeeded pod/extra-run-06-0001 --timeout=60s # дождаться расчёта
kubectl --context kind-de-k8s-lab -n de-k8s-quota get pod extra-run-06-0001 -o json # сохранить статус
kubectl --context kind-de-k8s-lab -n de-k8s-quota logs extra-run-06-0001 # сверить результат
kubectl --context kind-de-k8s-lab -n de-k8s-quota delete pod extra-run-06-0001 # удалить Pod
kubectl --context kind-de-k8s-lab -n de-k8s-quota delete quota quota-run-06-0001 # удалить квоту
```

## 9. Проверка

```bash
cmp results/mapreduce/counts_k2.csv kubernetes/results/final/counts_k2.csv # частоты
head -n 11 results/mapreduce/counts_k2.csv | cmp - kubernetes/results/final/top10.csv # топ-10
```
