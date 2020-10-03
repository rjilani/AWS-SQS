AWS-SQS example with JMS and SQS API

How to Run

mvn clean package will create uber jar

change this block "\<mainClass>com.driver.Driver\</mainClass>" in pom.xml 
to switch b/w jms vs simple sqs client

e.g. to pick jms client "\<mainClass>com.driver.JMSDriver\</mainClass>"

java -jar target/driver.jar
