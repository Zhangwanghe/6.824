#!/usr/bin/env bash

rm outputDir -rf
mkdir outputDir

SC=0
for i in $(seq 1 100); do
    output="outputDir/output_${i}.txt"
    go test -race -run TestStaticShards  >> ${output}

    NT=`cat ${output} | grep FAIL | wc -l`
    if [ "$NT" -eq "0" ]
    then
    rm ${output} -rf
    let SC++
    fi
done

echo "$SC" success