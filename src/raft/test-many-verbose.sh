#!/usr/bin/env bash

rm outputDir -rf
mkdir outputDir

SC=0
for i in $(seq 1 500); do
    output="outputDir/output_${i}.txt"
    VERBOSE=1 go test -run 2A -race  >> ${output}
    sleep 2
    VERBOSE=1 go test -run 2B -race  >> ${output}
    sleep 2
    VERBOSE=1 go test -run 2C -race  >> ${output}
    sleep 5
    VERBOSE=1 go test -run 2D -race  >> ${output}
    sleep 5
    

    NT=`cat ${output} | grep FAIL | wc -l`
    if [ "$NT" -eq "0" ]
    then
    rm ${output} -rf
    let SC++
    fi
done

echo "$SC" success