#!/usr/bin/env bash
output="output_many.log"

rm ${output}
touch ${output}

for i in $(seq 1 100); do
    go test -race -run TestStaticShards >> ${output}
done

NT=`cat ${output} | grep FAIL | wc -l`
if [ "$NT" -eq "0" ]
then
echo "Passed All"
fi