#!/bin/bash
bucket="your-own-bucket"
key="log/filelist-20220329_050947.log"
#keyword="d0006"
keyword="dir0009/file0441"
limitNum="100"
tmpfile="/tmp/temp-s3select.log"

#result=$(aws s3api select-object-content \
aws s3api select-object-content \
    --bucket $bucket \
    --key $key \
    --expression "SELECT * FROM s3object s where Lower(s._2) like '%${keyword}%' limit $limitNum" \
    --expression-type 'SQL' \
    --input-serialization '{"CSV": {"FieldDelimiter": ","}}' \
    --output-serialization '{"CSV": {"FieldDelimiter": ","}}' /tmp/temp-s3select.log

cat /tmp/temp-s3select.log
echo ""
echo "===== TAR Files containing $keyword ====="
cat /tmp/temp-s3select.log | awk '{print $1}' | sort | uniq
