#!/bin/bash
mkdir -p out
for f in `ls -1 *.txt`; do
 iconv -f utf-16 -t utf-8 $f > out/$f
 mv -f out/* .
done
