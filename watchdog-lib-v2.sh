log_info() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') [$GRPC_KAFKA_SERVICE_NAME] INFO: $1"
}

log_warning() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') [$GRPC_KAFKA_SERVICE_NAME] WARN: $1"
}

log_error() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') [$GRPC_KAFKA_SERVICE_NAME] ERROR: $1"
}

get_solana_slot() {
  local max_attempts=3
  local attempt=1
  local slot=""

  while [[ $attempt -le $max_attempts ]]; do
    # Get the LAST slot number from kafka send message lines
    slot=$(solana slot)

    if [[ -n "$slot" ]] && [[ "$slot" =~ ^[0-9]+$ ]]; then
      echo "$slot"
      return 0
    fi

    log_warning "Attempt $attempt: no slot found (via solana slots), falling back to RPC 1 metrics..." >&2

    # Get the LAST slot number from RPC 1 server
    slot=$(curl -s http://72.46.85.23:8999/metrics | grep 'slot_status{status="confirmed"}' | awk '{print $2}')

    if [[ -n "$slot" ]] && [[ "$slot" =~ ^[0-9]+$ ]]; then
      echo "$slot"
      return 0
    fi

    log_warning "Attempt $attempt: no slot found (via RPC 1 metrics), falling back to RPC 2 metrics..." >&2

    # Get the LAST slot number from RPC 2 server
    slot=$(curl -s http://67.213.116.39:8999/metrics | grep 'slot_status{status="confirmed"}' | awk '{print $2}')

    if [[ -n "$slot" ]] && [[ "$slot" =~ ^[0-9]+$ ]]; then
      echo "$slot"
      return 0
    fi

    log_warning "Attempt $attempt: no slot found (via RPC 2 metrics), no more fallbacks" >&2

    if [[ $attempt -lt $max_attempts ]]; then
      log_warning "Attempt $attempt: get_solana_slot no slot found, retrying in 1 second..." >&2
      sleep 1
    fi

    ((attempt++))
  done
}

extract_slot_from_metrics() {
  local metrics_host="$1"
  local metrics_port="$2"
  local max_attempts=3
  local attempt=1
  local slot=""

  while [[ $attempt -le $max_attempts ]]; do
    # Get the LAST slot number from kafka send message lines
    slot=$(curl -s http://${metrics_host}:${metrics_port}/metrics | grep 'kafka_slot{slot_type="pushed_to_kafka"}' | awk '{print $2}')

    if [[ -n "$slot" ]] && [[ "$slot" =~ ^[0-9]+$ ]]; then
      echo "$slot"
      return 0
    fi

    if [[ $attempt -lt $max_attempts ]]; then
      echo "Attempt $attempt: extract_slot_from_metrics - no slot found, retrying in 1 second..." >&2
      sleep 1
    fi

    ((attempt++))
  done
}

extract_slot_with_hash() {
  local log_file_path="$1"
  local max_attempts=3
  local attempt=1
  local slot=""

  while [[ $attempt -le $max_attempts ]]; do
    # Get the LAST slot number from kafka send message lines
    slot=$(extract_slot_with_hash_from_log_file $log_file_path)

    if [[ -n "$slot" ]] && [[ "$slot" =~ ^[0-9]+_[a-f0-9]+$ ]]; then
      echo "$slot"
      return 0
    fi

    if [[ $attempt -lt $max_attempts ]]; then
      echo "Attempt $attempt: no slot found, retrying in 1 second..." >&2
      sleep 1
    fi

    ((attempt++))
  done
}

update_state() {
  local key="$1"
  local value="$2"

  # Create state file if it doesn't exist
  if [[ ! -f "$STATE_FILE_PATH" ]]; then
    touch "$STATE_FILE_PATH"
  fi

  # Check if key exists
  if grep -q "^${key}=" "$STATE_FILE_PATH"; then
    # Update existing key
    if [[ "$OSTYPE" == "darwin"* ]]; then
      # macOS
      sed -i '' "s|^${key}=.*|${key}=${value}|" "$STATE_FILE_PATH"
    else
      # Linux
      sed -i "s|^${key}=.*|${key}=${value}|" "$STATE_FILE_PATH"
    fi
  else
    # Append new key
    echo "${key}=${value}" >> "$STATE_FILE_PATH"
  fi
}

get_state() {
  local key="$1"
  local default="${2:-}" # Empty string if not provided

  if [[ ! -f "$STATE_FILE_PATH" ]]; then
    echo "$default"
    return
  fi

  local value=$(grep "^${key}=" "$STATE_FILE_PATH" 2>/dev/null | cut -d'=' -f2-)

  if [[ -n "$value" ]]; then
    echo "$value"
  else
    echo "$default"
  fi
}

is_topic_unhealthy() {
  UNHEALTHY_TOPICS=$(get_state "UNHEALTHY_TOPICS" "")
  local topic=$1
  [[ ",$UNHEALTHY_TOPICS," == *",$topic,"* ]]
}

mark_topic_unhealthy() {
  UNHEALTHY_TOPICS=$(get_state "UNHEALTHY_TOPICS" "")
  local topic=$1
  if ! is_topic_unhealthy "$topic"; then
    if [ -z "$UNHEALTHY_TOPICS" ]; then
        UNHEALTHY_TOPICS="$topic"
    else
        UNHEALTHY_TOPICS="${UNHEALTHY_TOPICS},${topic}"
    fi

    # Persist to state file
    update_state "UNHEALTHY_TOPICS" "$UNHEALTHY_TOPICS"
  fi
}

mark_topic_healthy() {
  UNHEALTHY_TOPICS=$(get_state "UNHEALTHY_TOPICS" "")
  local topic=$1
  
  if is_topic_unhealthy "$topic"; then
    # Split into array
    IFS=',' read -ra topics_array <<< "$UNHEALTHY_TOPICS"
    
    # Build new list without the topic
    local new_topics=""
    for t in "${topics_array[@]}"; do
      if [ "$t" != "$topic" ]; then
        if [ -z "$new_topics" ]; then
          new_topics="$t"
        else
          new_topics="${new_topics},${t}"
        fi
      fi
    done
    
    # Update the variable and persist to state file
    UNHEALTHY_TOPICS="$new_topics"
    update_state "UNHEALTHY_TOPICS" "$UNHEALTHY_TOPICS"
  fi
}

update_secret_env() {
  local secret_name="$1"
  local env_key="$2"
  local env_value="$3"

  if [ -z "$secret_name" ] || [ -z "$env_key" ] || [ -z "$env_value" ]; then
    log_error "Usage: update_secret_env SECRET_NAME ENV_KEY ENV_VALUE"
    return 1
  fi

  log_info "Fetching current secret: $secret_name"

  local current_secret=$(gcloud secrets versions access latest --project="vybe-devops" --secret="$secret_name" 2>/dev/null)

  if [ $? -ne 0 ]; then
    log_error "Failed to fetch secret '$secret_name'"
    return 1
  fi

  local updated_secret=$(echo "$current_secret" | awk -v key="$env_key" -v value="$env_value" '
    BEGIN { found=0 }
    $0 ~ "^"key"=" { print key"="value; found=1; next }
    { print }
    END { if (!found) print key"="value }
  ')

  log_info "Updating secret with new value for: $env_key - diff result:"
  diff <(echo "$current_secret") <(echo "$updated_secret")

  # Create new version of the secret
  printf "%s" "$updated_secret" | gcloud secrets versions add "$secret_name" --data-file=- --project="vybe-devops"

  #log_info "DEBUGGING - Not actually updating the secret! Just echoing it"
}

send_slack_message() {
    local message="$1"

    # remove below in prod
    #log_error "(Not actually sending Slack notification) - $1"
    #return 0

    message=$(echo "$message" | sed 's/"/\\"/g' | sed "s/'/\\'/g")
    curl -X POST -H 'Content-type: application/json' --data "{\"text\":\"$message\"}" "$SLACK_WEBHOOK_URL" -s -o /dev/null -w "%{http_code}"
    # Check if successful (HTTP 200)
    if [ $? -eq 0 ]; then
        return 0
    else
        return 1
    fi
}

is_log_file_growing() {
    local log_file="$1"
    local check_interval=5
    local total_duration=90
    local elapsed=0
    
    # Check if file exists
    if [[ ! -f "$log_file" ]]; then
        return 1
    fi

    while [[ $elapsed -lt $total_duration ]]; do
      local initial=$(extract_slot_with_hash $log_file_path)
      log_info "is_log_file_growing - initial - $initial"

      if [[ -z "$initial" ]]; then
        continue
      fi

      # Wait for the check interval
      sleep "$check_interval"
      elapsed=$((elapsed + check_interval))

      local final=$(extract_slot_with_hash "$log_file")

      log_info "is_log_file_growing - final (at ${elapsed}s) - $final"

      # If file grew, return success immediately
      if [[ "$final" != "$initial" ]]; then
        log_info "is_log_file_growing - File is growing (detected at ${elapsed}s)"
        return 0
      fi

      log_info "is_log_file_growing - No growth detected, continuing check..."
    done

    # If we've exhausted the total duration without detecting growth
    log_info "is_log_file_growing - No growth detected after ${total_duration}s"
    return 1
}

switch() {

  log_info "Not actually switching. This is just a Test"
  return 1

  local current_topic="$1"

  log_info "Marking the current service replica as unhealthy ($current_topic)."
  mark_topic_unhealthy $current_topic

  log_info "Finding a backup to rotate."
  local new_topic=""
  for item in "${REPLICA_MAP[@]}"; do
    local topic_name="${item%%|*}"

    # skip if it's the same as the current topic
    if [[ "$topic_name" == "$current_topic" ]]; then
      continue
    fi

    # skip if unhealthy
    if is_topic_unhealthy "$topic_name"; then
      continue
    fi

    new_topic="$topic_name"
    break
  done

  if [ -z "$new_topic" ]; then
    log_error "All backups are unhealthy. Cannot rotate to any backups!"

    send_slack_message "$SLACK_MENTIONS - [acc-prdcr-failover][$GRPC_KAFKA_SERVICE_NAME] CRITICAL - all backups are unhealthy! grpc-service is disrupted. Staying with ($current_topic) kafka topic now."

    update_state "WATCHDOG_ENABLED" "false"

    return 1
  fi

  log_info "Switching from topic $current_topic to $new_topic"

  for secret_entry in "${SECRET_AND_TOPIC_KEY_LIST[@]}"; do
    IFS='|' read -r secret_name topic_key <<< "$secret_entry"

    log_info "Updating $secret_name - $topic_key to use topic $new_topic"

    update_secret_env $secret_name $topic_key $new_topic
  done

  log_info "Updating state CURRENT_TOPIC to $new_topic"
  update_state "CURRENT_TOPIC" "$new_topic"

  # Reset the ABOVE_THRESHOLD_STARTED_AT to null 
  update_state "ABOVE_THRESHOLD_STARTED_AT" ""

  send_slack_message "$SLACK_MENTIONS - [acc-prdcr-failover][$GRPC_KAFKA_SERVICE_NAME] - switched to a backup topic ($new_topic) - the ($current_topic) topic is marked as unhealthy."
}


main() {
  log_info "Calling main"

  local enabled=$(get_state "WATCHDOG_ENABLED" "true")
  if [[ $enabled == "false" ]]; then
    log_error "Watchdog has self-disabled because all grpc-kafka services are unhealthy. Exiting."
    return 1
  fi

  # get current topic - gives us current log file
  local current_topic=$(get_state "CURRENT_TOPIC" "")
  if [[ -z "$current_topic" ]]; then
    log_error "CURRENT_TOPIC is not found or empty. Update the state with the current topic"
    return 1
  fi

  log_info "Current topic: $current_topic"

  metrics_host=""
  metrics_port=""
  for item in "${REPLICA_MAP[@]}"; do
    IFS='|' read -r item_topic item_host item_port <<< "$item"
    if [[ "$item_topic" == "$current_topic" ]]; then
      metrics_host="$item_host"
      metrics_port="$item_port"
      break
    fi
  done

  if [[ -z "$metrics_port" ]]; then
    log_error "No metrics port found for topic ($current_topic). Make sure this topic is declared properly"
    return 1
  fi

  log_info "found metrics_host=$metrics_host metrics_port=$metrics_port"

  # Check if the log file isn't growing, switch immediately
  #is_log_file_growing "$log_file_path"
  #result=$?
  #if [ $result -ne 0 ]; then
  #  log_error "Log file is not growing"

  #  send_slack_message "$SLACK_MENTIONS - [acc-prdcr-failover][$GRPC_KAFKA_SERVICE_NAME] WARNING - Log file ($log_file_path) has not been growing for >90s. Please advise. Current topic is: ($current_topic)"

    #switch "$current_topic"

  #  return 1
  #else
  #  log_info "Log file is growing"
  #fi

  # get the last processed slot.
  local latest_processed_slot=$(extract_slot_from_metrics $metrics_host $metrics_port)
  local solana_slot=$(get_solana_slot "")

  log_info "solana slot: $solana_slot - latest: $latest_processed_slot"

  local lag=-99

  if [ -n "$latest_processed_slot" ] && [ -n "$solana_slot" ]; then
    lag=$((solana_slot - latest_processed_slot))
  fi

  # handle -99 case
  if [ "$lag" -eq -99 ]; then
    echo "WARNING: Could not calculate lag (missing slot data)"
    log_error "Could not calculate lag (missing slot data). Switching topic immediately."

    switch "$current_topic"
    return 0
  fi

  # if lag is acceptable, don't do anything
  if [ "$lag" -lt "$LAG_THRESHOLD" ]; then
    log_info "Lag of $lag slots is below the threshold of $LAG_THRESHOLD slots. No action taken."

    # Reset the ABOVE_THRESHOLD_STARTED_AT to null 
    update_state "ABOVE_THRESHOLD_STARTED_AT" ""

    return 0
  fi

  log_error "Lag of $lag slots is above the threshold of $LAG_THRESHOLD slots."

  # check the started at. if null, set it to now. if not null, if more than X min, then switch. if not, just leave it.
  local above_threshold_started_at=$(get_state "ABOVE_THRESHOLD_STARTED_AT" "")
  local now=$(date +%s)

  # if null, just set it and return 0
  if [ -z "$above_threshold_started_at" ]; then
    update_state "ABOVE_THRESHOLD_STARTED_AT" "$now"

    log_info "Record the timestamp when the lag threshold began: $now"

    send_slack_message "$SLACK_MENTIONS - [acc-prdcr-failover][$GRPC_KAFKA_SERVICE_NAME] WARNING - Lag of >= $LAG_THRESHOLD slots detected on ($current_topic) kafka topic. Will automatically try switching to a backup topic if persist for more than $ABOVE_THRESHOLD_MORE_THAN_SECONDS seconds."

    return 0
  fi

  local diff_seconds=$((now - above_threshold_started_at))

  # if not null, if not older than X minutes, just return 0
  if [ $diff_seconds -lt $ABOVE_THRESHOLD_MORE_THAN_SECONDS ]; then
    log_info "Lag has been above threshold for ($diff_seconds) seconds. Will switch when goes above ($ABOVE_THRESHOLD_MORE_THAN_SECONDS) seconds"

    return 0
  fi
  
  # if not null, and if older than X minutes, then switch

  switch "$current_topic"
}

set_topic() {
  log_info "Calling set_topic"

  local topic=$1
  local should_create_new_secret=$2

  if [[ -z "$topic" ]]; then
    log_error "set_topic expects a topic as its first argument"
    exit 0
  fi
  if [[ -z "$should_create_new_secret" ]]; then
    log_error "set_topic expects \"true\" or \"false\" as its second argument"
    exit 0
  fi

  log_info "Resetting the state file ($STATE_FILE_PATH)"
  > $STATE_FILE_PATH

  local current_topic=$(get_state "CURRENT_TOPIC" "")

  log_info "Updating state CURRENT_TOPIC to $topic"
  update_state "CURRENT_TOPIC" "$topic"

  if [[ "$should_create_new_secret" == "true" ]]; then
    #log_info "should_create_new_secret is true."
    log_info "Switching from topic $current_topic to $topic"

    for secret_entry in "${SECRET_AND_TOPIC_KEY_LIST[@]}"; do
      IFS='|' read -r secret_name topic_key <<< "$secret_entry"

      log_info "Updating $secret_name - $topic_key to use topic $topic"

      update_secret_env $secret_name $topic_key $topic
    done
  fi
}

check_unhealthy_topics() {
  UNHEALTHY_TOPICS=$(get_state "UNHEALTHY_TOPICS" "")
  IFS=',' read -ra unhealthy_topics <<< "$UNHEALTHY_TOPICS"
  for topic in "${unhealthy_topics[@]}"; do
    log_info "Checking unhealthy topic $topic to see if it's healthy again"

    metrics_host=""
    metrics_port=""
    for item in "${REPLICA_MAP[@]}"; do
      IFS='|' read -r item_topic item_host item_port <<< "$item"
      if [[ "$item_topic" == "$topic" ]]; then
        metrics_host="$item_host"
        metrics_port="$item_port"
        break
      fi
    done

    if [[ -z "$metrics_port" ]]; then
      log_error "No metrics port found for topic ($topic). Make sure this topic if declared properly"
      return 1
    fi

    log_info "found metrics_host=$metrics_host metrics_port=$metrics_port"

    # get the last slot in the log file.
    local latest_processed_slot=$(extract_slot_from_metrics $metrics_host $metrics_port)
    local solana_slot=$(get_solana_slot "")

    log_info "solana slot: $solana_slot - latest: $latest_processed_slot"

    local lag=-99

    if [ -n "$latest_processed_slot" ] && [ -n "$solana_slot" ]; then
      lag=$((solana_slot - latest_processed_slot))
    fi

    # todo: handle -99 case

    # if lag is acceptable, remove from unhealthy topics
    if [ "$lag" -lt "$LAG_THRESHOLD" ]; then
      log_info "Lag of $lag slots is below the threshold of $LAG_THRESHOLD slots. Removing topic ($topic) from unhealthy list."

      mark_topic_healthy $topic

      update_state "WATCHDOG_ENABLED" "true"

      continue
    fi

    log_error "Lag of $lag slots is above the threshold of $LAG_THRESHOLD slots. Topic ($topic) is still unhealthy"
  done
}

