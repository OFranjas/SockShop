# E-Commerce Sock Shop Data Processing with Kafka

## Project Overview
This project is part of an educational assignment focused on building a simulated E-Commerce Sock Shop. The core of the project revolves around using Apache Kafka, particularly Kafka Streams, to handle and process data streams related to sock sales, purchases, and inventory management.

### Key Features
- **Data Streaming with Kafka:** Utilizes Kafka topics to manage data streams about sock suppliers, purchases, and sales.
- **Real-Time Data Processing:** Implements Kafka Streams for real-time data processing, calculating metrics like revenue, expenses, and profit.
- **Database Integration:** Uses Kafka Connectors for seamless interaction with the database, storing and retrieving necessary data.
- **Fault Tolerance:** Ensures system robustness with a multi-broker Kafka setup.

### Components
- **Customers Application:** Simulates customer interactions and sales data generation.
- **Purchase Orders Application:** Manages the creation of purchase orders based on inventory data.
- **Kafka Streams Application:** Processes streams for business intelligence and analytics.
- **Kafka Connectors:** Configured for database interactions.

### Technologies Used
- Apache Kafka
- Kafka Streams
- Java
- Docker
- Maven

### Setup and Running
Detailed instructions for setting up and running the project will be provided separately.

### Contribution
This project is developed collaboratively by Gonçalo Ferreira and Rafael Ferreira. It is part of an academic assignment and is not intended for commercial use.

### License
This project is licensed under the [MIT License](LICENSE).

## Source.json

### post connector to kafka connect 

curl -X POST -H "Content-Type: application/json" --data @SockShop/config/sourceSocks.json http://localhost:8083/connectors ; curl -X POST -H "Content-Type: application/json" --data @SockShop/config/SourcePurchases.json http://localhost:8083/connectors 



curl -X DELETE http://localhost:8083/connectors/connect-jdbc-source-purchases ; curl -X DELETE http://localhost:8083/connectors/connect-jdbc-source-socks




### read from DBinfo_topic using consumer

kafka_2.13-3.6.0/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic DBInfo_topic --from-beginning

### steps que me lembro

Adicionar os 2 ficheiros que o gameiro deu no lib do kafka