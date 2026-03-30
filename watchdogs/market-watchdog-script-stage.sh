#!/bin/bash

GRPC_KAFKA_SERVICE_NAME="market-staging"

STATE_FILE_PATH="/dragon/watchdogs/acc-failover/market-watchdog-stage.state"

# format: TopicName|Host|Port
REPLICA_MAP=(
    "market_accounts|172.18.96.19|39004"
    "market_accounts_backup|172.18.96.19|49004"
)

LAG_THRESHOLD=200
ABOVE_THRESHOLD_MORE_THAN_SECONDS=600

# format: SecretName|KafkaTopicEnvKey
SECRET_AND_TOPIC_KEY_LIST=(
    "VYBE_TRADE_PRODUCER-STAGE-GKE-MARKET-ACCOUNT|KAFKA_TOPIC"
    "VYBE_LIVE_AGGREGATOR_STAGE|KAFKA_ACCOUNT_TOPIC"
    "VYBE_TRADE_PRODUCER-STAGE-GKE-MARKET-LIQUIDITY-ACCOUNT|KAFKA_TOPIC"
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

