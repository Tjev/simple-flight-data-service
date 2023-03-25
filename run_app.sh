#!/usr/bin/env sh

if [ ! -d "faa_data" ]; then
  mkdir faa_data
  echo "Info: creating faa_data directory..."
fi

if [ -z "$(ls -A faa_data)" ]; then
  echo "Error: required data not found in the faa_data directory."
  echo "Please move the extracted parquet files into the faa_data directory."
  exit 1
fi

echo "Info: required data found."
echo "Info: launching docker container..."
echo ""

dataDir="faa_data/"

docker run --rm -d \
    -p 80:80 \
    -v `pwd`/$dataDir:/code/$dataDir:ro \
    sfd_service
