# Общие функции экспериментов. Подключается из скриптов N-*.sh.
set -euo pipefail

cd "$(dirname "$0")/.."
RESULTS=experiments/results
mkdir -p "$RESULTS"

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

# Отметка времени для выборки графиков в Grafana (from/to в мс).
mark() { echo "$1=$(date +%s)000" >>"$RESULTS/$EXP-times.env"; }

# Чистый стек: down -v, up, ожидание kafka-init и прогрева нагрузки.
fresh() {
  log "fresh stack"
  docker compose down -v --remove-orphans >/dev/null 2>&1
  docker compose up -d --build >/dev/null 2>&1
  local code
  code=$(docker wait "$(docker compose ps -aq kafka-init)")
  [ "$code" = 0 ] || { docker compose logs kafka-init; exit 1; }
  : >"$RESULTS/$EXP-times.env"
  mark start
  log "warm-up ${1:-60}s"
  sleep "${1:-60}"
}

# Утилита Kafka внутри брокера $1: kt kafka-3 kafka-topics.sh --describe --topic events
kt() {
  local broker=$1 tool=$2
  shift 2
  docker exec -e KAFKA_HEAP_OPTS=-Xmx128m "hw2-$broker-1" \
    timeout 30 "/opt/kafka/bin/$tool" --bootstrap-server "$broker:19092" "$@" </dev/null
}

describe() { kt "$1" kafka-topics.sh --describe --topic events 2>&1 | grep -v '^WARNING' || true; }
groups() { kt "$1" kafka-consumer-groups.sh --describe --group events-processor 2>&1 || true; }

# Останавливает продюсеров (на SIGTERM они досылают буфер) и запускает verify,
# дождавшись нулевого lag. Вывод - в $1.
stop_and_verify() {
  log "stop producers"
  docker compose stop producer-1 producer-2 producer-3 >/dev/null 2>&1
  mark producers_stopped
  log "verify -> $1"
  docker compose run --rm -T verify verify -wait="${2:-3m}" </dev/null | tee "$1" || true
  mark end
}

# Выдержки из логов сервиса в файл: только сообщения, подходящие под regex.
logs_grep() {
  local regex=$1 out=$2
  shift 2
  docker compose logs --no-color --no-log-prefix "$@" | grep -E "$regex" >"$out" || true
}

sql() { docker exec hw2-postgres-1 psql -U events -d events -c "$1" </dev/null; }
