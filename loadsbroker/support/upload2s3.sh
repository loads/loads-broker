#!/bin/sh

if [ $# != 3 ]; then
    echo "usage $0: archive s3-bucket destdir"
    exit 1
fi
if [ -z "${AWS_ACCESS_KEY_ID}" ]; then
    echo "No AWS_ACCESS_KEY_ID defined"
    exit 1
fi
if [ -z "${AWS_SECRET_ACCESS_KEY}" ]; then
    echo "No AWS_SECRET_ACCESS_KEY defined"
    exit 1
fi

upload2s3()
{
    FULLPATH=$1
    BUCKET=$2
    DESTDIR=$3

    FILE=`basename "${FULLPATH}"`
    FILE=`escape "${FILE}"`
    DESTDIR=`escape "${DESTDIR}"`

    DATE=`date -R`
    RESOURCE="/${BUCKET}/${DESTDIR}/${FILE}"
    CONTENT_TYPE="application/x-compressed-tar" # XXX: and really sha1 below?
    TO_SIGN="PUT\n\n${CONTENT_TYPE}\n${DATE}\n${RESOURCE}"
    SIG=`echo -en ${TO_SIGN} | \
         openssl sha1 -hmac ${AWS_SECRET_ACCESS_KEY} -binary | \
         base64`
    curl -X PUT -T "${FULLPATH}" \
         -H "Host: ${BUCKET}.s3.amazonaws.com" \
         -H "Date: ${DATE}" \
         -H "Content-Type: ${CONTENT_TYPE}" \
         -H "Authorization: AWS ${AWS_ACCESS_KEY_ID}:${SIG}" \
         https://${BUCKET}.s3.amazonaws.com/${DESTDIR}/${FILE}
}

escape()
{
    echo $1 | sed 's/ /%20/g'
}

upload2s3 "$1" "$2" "$3"
