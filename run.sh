#!/bin/bash
# =============================================================================
# Kafka → Hive Streaming Framework — run script
#
# Usage:
#   ./run.sh <environment> <conf-file> <truststore-password> <keystore-password> <key-password>
#
# Environments:
#   dev   → development
#   sit   → system integration testing
#   uat   → user acceptance testing
#   prod  → production
#
# Example:
#   ./run.sh dev  /opt/pipelines/orders/application.conf TrustPass KeyPass KeyPass
#   ./run.sh uat  /opt/pipelines/orders/application.conf TrustPass KeyPass KeyPass
#   ./run.sh prod /opt/pipelines/orders/application.conf TrustPass KeyPass KeyPass
# =============================================================================

ENV=$1
CONF_FILE=$2
TRUSTSTORE_PASSWORD=$3
KEYSTORE_PASSWORD=$4
KEY_PASSWORD=$5

# =============================================================================
# SSL file locations per environment — update paths to match your setup
# =============================================================================
case "$ENV" in
    dev)
        TRUSTSTORE_LOCATION="/etc/kafka/ssl/dev/truststore.jks"
        KEYSTORE_LOCATION="/etc/kafka/ssl/dev/keystore.jks"
        KAFKA_BROKERS="dev-broker1:9093,dev-broker2:9093"
        YARN_QUEUE="default"
        ;;
    sit)
        TRUSTSTORE_LOCATION="/etc/kafka/ssl/sit/truststore.jks"
        KEYSTORE_LOCATION="/etc/kafka/ssl/sit/keystore.jks"
        KAFKA_BROKERS="sit-broker1:9093,sit-broker2:9093"
        YARN_QUEUE="sit"
        ;;
    uat)
        TRUSTSTORE_LOCATION="/etc/kafka/ssl/uat/UAT_cacerts.jks"
        KEYSTORE_LOCATION="/etc/kafka/ssl/uat/UAT.jks"
        KAFKA_BROKERS="uat-broker1:9093,uat-broker2:9093"
        YARN_QUEUE="uat"
        ;;
    prod)
        TRUSTSTORE_LOCATION="/etc/kafka/ssl/prod/prod_cacerts.jks"
        KEYSTORE_LOCATION="/etc/kafka/ssl/prod/prod_keystore.jks"
        KAFKA_BROKERS="prod-broker1:9093,prod-broker2:9093,prod-broker3:9093"
        YARN_QUEUE="production"
        ;;
    *)
        echo "ERROR: Unknown environment '$ENV'"
        echo "Valid environments: dev | sit | uat | prod"
        exit 1
        ;;
esac

# =============================================================================
# Validate inputs
# =============================================================================
if [ -z "$CONF_FILE" ]; then
    echo "ERROR: conf file path is required"
    echo "Usage: ./run.sh <env> <conf-file> <truststore-password> <keystore-password> <key-password>"
    exit 1
fi

if [ ! -f "$CONF_FILE" ]; then
    echo "ERROR: conf file not found: $CONF_FILE"
    exit 1
fi

if [ -z "$TRUSTSTORE_PASSWORD" ] || [ -z "$KEYSTORE_PASSWORD" ] || [ -z "$KEY_PASSWORD" ]; then
    echo "ERROR: all three passwords are required"
    echo "Usage: ./run.sh <env> <conf-file> <truststore-password> <keystore-password> <key-password>"
    exit 1
fi

# =============================================================================
# Clear checkpoint
# =============================================================================
CHECKPOINT=$(grep "checkpoint-location" "$CONF_FILE" | awk -F '"' '{print $2}')
if [ -n "$CHECKPOINT" ]; then
    echo "Clearing checkpoint: $CHECKPOINT"
    rm -rf "$CHECKPOINT"
fi

# =============================================================================
# Run
# =============================================================================
echo "========================================"
echo " Environment : $ENV"
echo " Conf file   : $CONF_FILE"
echo " Brokers     : $KAFKA_BROKERS"
echo " Truststore  : $TRUSTSTORE_LOCATION"
echo " Keystore    : $KEYSTORE_LOCATION"
echo " Queue       : $YARN_QUEUE"
echo "========================================"

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --queue "$YARN_QUEUE" \
  --class com.yourcompany.streaming.app.StreamingApp \
  --driver-java-options " \
    -Dconfig.file=$CONF_FILE \
    -Dssl.truststore.location=$TRUSTSTORE_LOCATION \
    -Dssl.keystore.location=$KEYSTORE_LOCATION \
    -Dssl.truststore.password=$TRUSTSTORE_PASSWORD \
    -Dssl.keystore.password=$KEYSTORE_PASSWORD \
    -Dssl.key.password=$KEY_PASSWORD" \
  --executor-java-options " \
    -Dssl.truststore.location=$TRUSTSTORE_LOCATION \
    -Dssl.keystore.location=$KEYSTORE_LOCATION \
    -Dssl.truststore.password=$TRUSTSTORE_PASSWORD \
    -Dssl.keystore.password=$KEYSTORE_PASSWORD \
    -Dssl.key.password=$KEY_PASSWORD" \
  kafka-hive-streaming-1.0.0.jar

EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo "ERROR: Job failed with exit code $EXIT_CODE"
    exit $EXIT_CODE
fi
echo "Job submitted successfully"