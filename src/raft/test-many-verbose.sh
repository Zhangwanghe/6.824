rm outputDir -rf
mkdir outputDir

for i in $(seq 1 100); do
    output="outputDir/output_${i}.txt"
    VERBOSE=1 go test -run TestManyElections2A -race  >> ${output}
done