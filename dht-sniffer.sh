ipfs log tail | jq -r 'if .event == "dhtSentMessage" and .message.type == "ADD_PROVIDER"  then .message.key else empty end'
