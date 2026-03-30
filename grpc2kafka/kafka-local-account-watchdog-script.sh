#!/bin/bash

GRPC_KAFKA_SERVICE_NAME="kafka-local-account"

STATE_FILE_PATH="/dragon/watchdogs/acc-failover/kafka-local-account-watchdog.state"

# format: TopicName|Host|Port
REPLICA_MAP=(
    "dragon-account-v2|172.18.104.127|28003"
    "dragon-account-v2_backup|172.18.104.127|29003"
)

LAG_THRESHOLD=200
ABOVE_THRESHOLD_MORE_THAN_SECONDS=600

# format: SecretName|KafkaTopicEnvKey
SECRET_AND_TOPIC_KEY_LIST=(
    "VYBE_TRADE_PRODUCER-STAGE-GKE-ACCOUNT-WSS|KAFKA_TOPIC"
    "VYBE_TRADE_PRODUCER-PROD-GKE-ACCOUNT-WSS|KAFKA_TOPIC"
)

SLACK_WEBHOOK_URL=https://hooks.slack.com/services/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
SLACK_MENTIONS="<@U08NNBY8DLJ> <@U038AJBE4CX>"

# Logic for extracting slot from logs may be different for other grpc-kafka servcies
extract_slot_from_log_file() {
  local log_file_path="$1"
  slot=$(tail -n 100 "$log_file_path" 2>/dev/null | grep "kafka send message with key:" | tail -1 | sed -n 's/.*key: \([0-9]*\)_.*/\1/p')
  echo $slot
}

extract_slot_with_hash_from_log_file() {
  local log_file_path="$1"
  slot=$(tail -n 100 "$log_file_path" 2>/dev/null | grep "kafka send message with key:" | tail -1 | sed -n 's/.*key: \([0-9]*_[a-f0-9]*\),.*/\1/p')
  echo $slot
}

source watchdog-lib-v2.sh

source watchdog-run-shared.sh

