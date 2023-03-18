# SparkDeveloperHomeWork6
- Вычитывать из CSV-файла, который можно скачать по ссылке - https://www.kaggle.com/sootersaalu/amazon-top-50-bestselling-books-2009-2019,  
данные, сериализовывать их в JSON, и записывать в топик books локально развернутого сервиса Apache Kafka.
- Вычитать из топика books данные и распечатать в stdout последние 5 записей (c максимальным значением offset) из каждой партиции.  
При чтении топика одновременно можно хранить в памяти только 15 записей.
***
 - Kafka через docker-compose (https://github.com/Gorini4/kafka_scala_example) 
 - ./kafka-topics.sh -create -topic books -bootstrap-server localhost:29092 --partitions 3
 
