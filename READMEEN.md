# A *pipeline* to make a fraud detection in bank transactions using Apache Flink
**This repository was created to make a Scala implementation of a proposed exercise on the course [ Apache Flink | A Real Time & Hands-On course on Flink ](https://www.udemy.com/course/apache-flink-a-real-time-hands-on-course-on-flink/).**

---
#### Para ver esse ReadMe em português [clique aqui](https://github.com/thiagobeppe/flinkFraudDetection/blob/master/README.md)
---
# Techs
* Scala
* Apache Flink
* Apache Kafka
* Docker
* Shell

---
# Necessary resources
* Java (11)
* [Docker](https://www.docker.com/) and [Docker-Compose](https://docs.docker.com/compose/install/)
* [Scala](https://www.scala-lang.org/download/) (2.12.11)
* [SBT](https://www.scala-sbt.org/download.html) (1.3.10)

---
# Attention
* Check if don't have any service running on the ports 2181 and 9092, otherwise your ```docker-compose``` will not build the cointainer. If you can't kill the service you can change the port in docker-compose.yml [1].
  
---

# Project structure
```
├── analisysResult
├── datasets
│   ├── alarmed_cust.txt
│   ├── bank_data.txt
│   └── lost_cards.txt
├── docker-compose.yml
├── fraudDetection
│   ├── build.sbt
│   └── src
│       ├── main
│       │   └── scala
│       │       └── com
│       │           └── github
│       │               └── example
│       │                   ├── filters
│       │                   │   ├── alarmedCustomCheck.scala
│       │                   │   ├── cityChangeCheck.scala
│       │                   │   ├── excessiveTransactionCheck.scala
│       │                   │   └── lostCardsCheck.scala
│       │                   ├── fraudDetection.scala
│       │                   └── kafka
│       │                       └── producerKafka.scala
│       └── test
│           └── scala
└── logs
    └── data
```
* **analisysResult**  - *The results of your code will be placed here*
* **datasets**  - *all the datasets required for the analisys*
* **docker-compose.yml**  - *file with the Kafka and Zookeeper configurations build on docker*
* **fraudDetection/../filters**  - *All of the business rules to make the fraud detection*
* **fraudDetection/../kafka**  - *Kafka's producer to simulate a insert of data by API*
* **logs/data**  - *The kafka's logs will be put here*

---
# Business rules

* **AlarmedCustomer** - A client who has your ID tagged like a suspicious customer.
* **LostCards** - Lost or stolen credit cards.
* **excessiveTransactions** - If the same client was made more than 10 transactions in a interval of 10 secondes, the transaction will be flagged like a suspicious transaction.
* **cityChange** - If the same credit card was used in two or more cities the transaction will be flagged like a suspicious transaction.


---
# How to run (Linux - Ubuntu)
* In your terminal go to the root of the project and run the command "```./run.sh```" to start the application.
* In "real-time", the results will be archived in the folder ```analisysResult```, just open the file to see them.
* Wait at least 1 minute after you start the Consumer, because there is a filter with 1 minute of execution.
* For finalize your application, close the Kafka and Flink terminals and, in a root of the project, open another terminal and run the command ```docker-compose down --volumes```.


---
[1]  In the tag ```ports```, change the left value to another port and run ```docker-compose up -d``` again. If you change the kafka's port you need to change the value in "```producerKafka.scala```" at the line ```  props.put("bootstrap.servers", "localhost:9092")```
```
zookeeper:
    container_name: zookeeper
    image: wurstmeister/zookeeper
    restart: "always"
    ports:
      -  "2181:2181"
```
