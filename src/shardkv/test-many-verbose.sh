#!/usr/bin/env bash

rm outputDir -rf
mkdir outputDir

SC=0
for i in $(seq 1 500); do
    output="outputDir/output_${i}.txt"
    go test -race >> ${output}
    sleep 2

    NT=`cat ${output} | grep FAIL | wc -l`
    if [ "$NT" -eq "0" ]
    then
    rm ${output} -rf
    let SC++
    fi
done

echo "$SC" success