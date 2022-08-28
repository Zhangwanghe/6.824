rm output_many.txt
touch output_many.txt

for i in $(seq 1 100); do
    go test -run 2A -race  >> output_many.txt
done