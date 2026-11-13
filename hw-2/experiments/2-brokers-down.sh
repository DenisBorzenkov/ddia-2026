#!/usr/bin/env bash
# Эксперимент 2: стоп kafka-1, затем kafka-2 (остаётся один брокер при
# min.insync.replicas=2), затем возврат обоих.
EXP=2
. "$(dirname "$0")/lib.sh"

fresh 60

describe kafka-1 >"$RESULTS/2-describe-0-before.txt"

log "stop kafka-1"
mark stop_kafka1
docker compose stop kafka-1 >/dev/null 2>&1
sleep 45
describe kafka-2 >"$RESULTS/2-describe-1-kafka1-down.txt"

log "stop kafka-2"
mark stop_kafka2
docker compose stop kafka-2 >/dev/null 2>&1
sleep 60
describe kafka-3 >"$RESULTS/2-describe-2-one-broker.txt"

log "start kafka-1 kafka-2"
mark start_brokers
docker compose start kafka-1 kafka-2 >/dev/null 2>&1
sleep 90
describe kafka-1 >"$RESULTS/2-describe-3-after.txt"
mark recovered

stop_and_verify "$RESULTS/2.txt" 5m

logs_grep 'batch in a produce request failed|produce failed|"stats"|"stopped"' "$RESULTS/2-producers.log" \
  producer-1 producer-2 producer-3
logs_grep '"level":"(WARN|ERROR)"|partitions (assigned|revoked|lost)|partition resumed' "$RESULTS/2-consumers.log" \
  consumer-1 consumer-2 consumer-3
log "done"
