#!/bin/bash

JAVA_YES_WEEK=`date +"%u"`
DIRRM=$JAVA_YES_WEEK
USER="ttmp3/ttmp3"
PASS=changba9527
SERVER=v0.ftp.upyun.com
LFTP=/usr/bin/lftp

echo "Del files via FTP... ${DIRRM}"
$LFTP -e "rm -rf /${DIRRM}; bye" -u $USER,$PASS $SERVER
echo "Done."
