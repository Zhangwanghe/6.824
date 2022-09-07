rm output_many.log
touch output_many.log

for i in $(seq 1 100); do
    go test -run TestBasic3A -race  >> output_many.log
done