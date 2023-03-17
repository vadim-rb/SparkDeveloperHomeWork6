./kafka-topics.sh -create -topic books -bootstrap-server localhost:29092 --partitions 3

./kafka-topics.sh --bootstrap-server=localhost:29092 --describe --topic books

./kafka-run-class.sh  kafka.tools.GetOffsetShell \
  --broker-list localhost:29092 \
  --topic books

./kafka-topics.sh --bootstrap-server=localhost:29092 --list

./kafka-topics.sh --bootstrap-server localhost:29092 --delete --topic books