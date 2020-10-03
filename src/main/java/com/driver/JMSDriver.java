package com.driver;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import javax.jms.*;

public class JMSDriver {

    private static String QUEUE_NAME="order_processing";

    public static void main(String[] args) {
        System.out.println("SQS JMS Example");

//        createQueue();
//        sendMessage();
//        recieveMessage();
        aysnchRecieveMessage();
    }

    private static void createQueue() {
        // Create a new connection factory with all defaults (credentials and region) set automatically
        SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
                new ProviderConfiguration(),
                AmazonSQSClientBuilder.defaultClient()
        );


        try {
            SQSConnection connection = connectionFactory.createConnection();
            AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();

            if (!client.queueExists(QUEUE_NAME)) {
                client.createQueue(QUEUE_NAME);
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    private static void sendMessage() {

        // Create a new connection factory with all defaults (credentials and region) set automatically
        SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
                new ProviderConfiguration(),
                AmazonSQSClientBuilder.defaultClient()
        );


        try {
            SQSConnection connection = connectionFactory.createConnection();
            // Create the nontransacted session with AUTO_ACKNOWLEDGE mode
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // Create a queue identity and specify the queue name to the session
            Queue queue = session.createQueue(QUEUE_NAME);

            // Create a producer for the 'MyQueue'
            MessageProducer producer = session.createProducer(queue);
            // Create the text message
            TextMessage message = session.createTextMessage("Hello World!");

            // Send the message
            producer.send(message);
            System.out.println("JMS Message " + message.getJMSMessageID());
            connection.stop();
            connection.close();

        } catch (JMSException e) {
            e.printStackTrace();
        }

    }

    private static void recieveMessage() {

        // Create a new connection factory with all defaults (credentials and region) set automatically
        SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
                new ProviderConfiguration(),
                AmazonSQSClientBuilder.defaultClient()
        );


        try {
            SQSConnection connection = connectionFactory.createConnection();
            // Create the nontransacted session with AUTO_ACKNOWLEDGE mode
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // Create a queue identity and specify the queue name to the session
            // Create a consumer for the 'MyQueue'
            Queue queue = session.createQueue(QUEUE_NAME);
            MessageConsumer consumer = session.createConsumer(queue);
            // Start receiving incoming messages
            connection.start();
            // Receive a message from 'MyQueue' and wait up to 1 second
            Message receivedMessage = consumer.receive(1000);

            // Cast the received message as TextMessage and display the text
            if (receivedMessage != null) {
                System.out.println("Received: " + ((TextMessage) receivedMessage).getText());
            }

            connection.stop();
            connection.close();

        } catch (JMSException e) {
            e.printStackTrace();
        }

    }

    private static void aysnchRecieveMessage() {

        // Create a new connection factory with all defaults (credentials and region) set automatically
        SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
                new ProviderConfiguration(),
                AmazonSQSClientBuilder.defaultClient()
        );


        try {
            SQSConnection connection = connectionFactory.createConnection();
            // Create the nontransacted session with AUTO_ACKNOWLEDGE mode
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // Create a queue identity and specify the queue name to the session
            // Create a consumer for the 'MyQueue'
            Queue queue = session.createQueue(QUEUE_NAME);
            MessageConsumer consumer = session.createConsumer(queue);
            consumer.setMessageListener(new MyListener());
            // Start receiving incoming messages
            connection.start();
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


//            connection.stop();
//            connection.close();

        } catch (JMSException e) {
            e.printStackTrace();
        }

    }
}

class MyListener implements MessageListener {

    @Override
    public void onMessage(Message message) {
        try {
            // Cast the received message as TextMessage and print the text to screen.
            System.out.println("Received: " + ((TextMessage) message).getText());
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}