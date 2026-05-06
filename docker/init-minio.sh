#!/bin/bash
# Wait for MinIO to be ready
until mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null; do
  echo "Waiting for MinIO..."
  sleep 2
done

# Create buckets (ignore errors if they already exist)
mc mb --ignore-existing local/marketpulse-raw
mc mb --ignore-existing local/marketpulse-bronze
mc mb --ignore-existing local/marketpulse-silver
mc mb --ignore-existing local/marketpulse-gold

echo "MinIO buckets created successfully"
exit 0
