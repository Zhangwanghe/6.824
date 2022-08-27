VERBOSE=1 go test -run InitialElection2A  > output.log
./dslogs output.log -c 3 # -i ignoretopic -j justtopic