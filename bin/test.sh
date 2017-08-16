#!/usr/bin/env bash

sizes=(16000 16000 32000)
for (( i = 0 ; i < ${#sizes[@]} ; i=$i+1 ));
do
    echo $i
    echo ${sizes[${i}]}
done