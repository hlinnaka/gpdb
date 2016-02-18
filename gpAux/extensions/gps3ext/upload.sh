#!/bin/bash
#
# Modified from the script at:
# http://geek.co.il/2014/05/26/script-day-upload-files-to-amazon-s3-using-bash
#
file="$1"
bucket="$2"
path="$3"

endpoint="localhost:1234"
#endpoint="s3.amazonaws.com"
key_id="$AWS_ACCESSID"
key_secret="$AWS_SECRET"
content_type="application/octet-stream"
date="$(LC_ALL=C date -u +"%a, %d %b %Y %X %z")"
md5="$(openssl md5 -binary < "$file" | base64)"

sig="$(printf "PUT\n$md5\n$content_type\n$date\n/$bucket/$path" | openssl sha1 -binary -hmac "$key_secret" | base64)"

curl -T $file http://$endpoint/$path \
    -H "Host: $bucket.s3.amazonaws.com" \
    -H "Date: $date" \
    -H "Authorization: AWS $key_id:$sig" \
    -H "Content-Type: $content_type" \
    -H "Content-MD5: $md5"
