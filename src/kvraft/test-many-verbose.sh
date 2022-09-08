#!/usr/bin/env bash

rm outputDir -rf
mkdir outputDir

for i in $(seq 1 100); do
    output="outputDir/output_${i}.txt"
    go test -run TestConcurrent3A -race  >> ${output}
done