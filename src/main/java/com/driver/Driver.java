package com.driver;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;

import java.util.List;


public class Driver {
    private static String QUEUE_NAME="order_processing";

    public static void main(String[] args) {
        System.out.println("SQS Example");

//        createQueue();
//        listQueue();
        while (true) {
            sendMessage();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
//        sendMessage();
//        receiveMessage();
//        deleteQueue();
    }

    private static void createQueue() {
        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        CreateQueueRequest create_request = new CreateQueueRequest(QUEUE_NAME)
                .addAttributesEntry("DelaySeconds", "60")
                .addAttributesEntry("MessageRetentionPeriod", "86400");

        try {
            CreateQueueResult result = sqs.createQueue(create_request);
            System.out.println("queu url is: " + result.getQueueUrl());
        } catch (AmazonSQSException e) {
            if (!e.getErrorCode().equals("QueueAlreadyExists")) {
                throw e;
            }
        }
    }

    private static void listQueue() {
        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        ListQueuesResult lq_result = sqs.listQueues();
        System.out.println("Your SQS Queue URLs:");
        for (String url : lq_result.getQueueUrls()) {
            System.out.println(url);
        }
    }

    private static void sendMessage() {
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/xxxxxxxxxxx/order_processing";
        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        System.out.println("seinf message to queue: " + queueUrl);
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody("hello world")
                .withDelaySeconds(5);
        sqs.sendMessage(send_msg_request);
    }

    private static void receiveMessage()  {
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/xxxxxxxxxxx/MyQueue";
        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        List<Message> messages = sqs.receiveMessage(queueUrl).getMessages();
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (Message m:messages) {
            System.out.println(m.getBody());
            System.out.println(m.getReceiptHandle());
            sqs.deleteMessage(queueUrl, m.getReceiptHandle());
        }

    }

    private static void deleteQueue() {

        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        sqs.deleteQueue("https://sqs.us-east-1.amazonaws.com/xxxxxxxxxxx/order_processing");

    }


}
