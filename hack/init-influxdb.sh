#!/bin/bash

# This script creates three buckets in InfluxDB with the names "A", "B", and "C". 
# It uses the InfluxDB HTTP API to create the buckets with an optional retention period.
# If no retention period is provided, the buckets are created with an infinite retention period ("never").
# The script accepts the following arguments:
#   - INFLUXDB_TOKEN (required): The InfluxDB access token used for authentication.
#   - INFLUXDB_ORG (required): The InfluxDB organization ID where the buckets will be created.
#
# The script will create the bucket des_event_count automatically and handle the response to confirm successful creation.
# If the bucket creation fails, the script will exit with an error message and display the InfluxDB response.

if ! command -v jq &> /dev/null; then
  echo "jq could not be found, installing..."
  sudo apt-get update
  sudo apt-get install -y jq
fi

INFLUXDB_URL="http://localhost:8086"
INFLUXDB_TOKEN=$1
INFLUXDB_ORG=$2
RETENTION_PERIOD=0

echo "$INFLUXDB_TOKEN"
echo "$INFLUXDB_ORG"

if [[ -z "$INFLUXDB_TOKEN" || -z "$INFLUXDB_ORG" ]]; then
  echo "Error: InfluxDB Token and Organization ID are required."
  echo "Usage: $0 <INFLUXDB_TOKEN> <INFLUXDB_ORG>"
  exit 1
fi

ORG_ID=$(curl -s -X GET "$INFLUXDB_URL/api/v2/orgs?name=$INFLUXDB_ORG" \
  -H "Authorization: Token $INFLUXDB_TOKEN" | jq -r '.orgs[0].id')

if [ "$ORG_ID" == "null" ] || [ -z "$ORG_ID" ]; then
  echo "Error: Could not find orgID for organization '$INFLUXDB_ORG'."
  exit 1
fi

BUCKET_NAMES=(
    "des_event_count"
)

for BUCKET_NAME in "${BUCKET_NAMES[@]}"; do
  echo "Creating bucket '$BUCKET_NAME' in InfluxDB at $INFLUXDB_URL..."

  if [ "$RETENTION_PERIOD" -eq 0 ]; then
    response=$(curl -s -X POST "$INFLUXDB_URL/api/v2/buckets" \
      -H "Authorization: Token $INFLUXDB_TOKEN" \
      -H "Content-Type: application/json" \
      -d '{
            "orgID": "'$ORG_ID'",
            "name": "'$BUCKET_NAME'",
            "retentionRules": [{
              "type": "never"
            }]
          }')
  else
    response=$(curl -s -X POST "$INFLUXDB_URL/api/v2/buckets" \
      -H "Authorization: Token $INFLUXDB_TOKEN" \
      -H "Content-Type: application/json" \
      -d '{
            "orgID": "'$ORG_ID'",
            "name": "'$BUCKET_NAME'",
            "retentionRules": [{
              "type": "expire",
              "everySeconds": '$RETENTION_PERIOD'
            }]
          }')
  fi

  if echo "$response" | grep -q '"id"'; then
    echo "Bucket '$BUCKET_NAME' created successfully."
  else
    echo "Failed to create the bucket '$BUCKET_NAME'."
    echo "Response from InfluxDB: $response"
    exit 1
  fi
done
