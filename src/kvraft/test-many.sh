rm output_many.log
touch output_many.log

for i in $(seq 1 100); do
    go test -run 3B  -race  >> output_many.log
done