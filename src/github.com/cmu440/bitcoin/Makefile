install:
	go install ./server
	go install ./miner
	go install ./client
run:
	~/go/bin/server 6060 &
	~/go/bin/miner localhost:6060 &
	~/go/bin/miner localhost:6060 &
	~/go/bin/client localhost:6060 bradfitz 99999 &

kill:
	echo "" > serverLog.txt
	sudo killall -9 server
	sudo killall -9 miner
	sudo killall -9 client
