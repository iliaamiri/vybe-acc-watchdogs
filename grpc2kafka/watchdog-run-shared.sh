command=$1
if [[ -z "$command" ]]; then
  log_error "Please enter a valid command: \`run\`, \`set\`"
  log_info "Usage (run the regular watchdog flow): \`run\`"
  log_info "Usage (manually switching to a topic): \`set <topic_name>\`"

  exit 1
fi

if [[ "$command" == "run" ]]; then
  main

  check_unhealthy_topics

  exit 0
fi

if [[ "$command" == "set" ]]; then
  topic_name=$2
  if [[ -z "$topic_name" ]]; then
    log_error "Expected a second argument <topic_name>. Please pass the topic name that you want to set."
    exit 1
  fi

  log_warning "You're about to manually change the topic of this service! This will reset the watchdog's state."
  read -p "Are you sure you want to proceed? (y/n):" confirmation
  if [[ "$confirmation" != "y" && "$confirmation" != "Y" ]]; then
    log_info "Aborting."
    exit 0
  fi

  read -p "[$GRPC_KAFKA_SERVICE_NAME] Do you want to create a new secret version with this topic ($topic_name)? (y/n) default is no: " create_new_secret
  should_create_new_secret="false"
  if [[ "$create_new_secret" == "y" || "$create_new_secret" == "Y" ]]; then
    should_create_new_secret="true"
  fi

  set_topic $topic_name $should_create_new_secret

  exit 0
fi
