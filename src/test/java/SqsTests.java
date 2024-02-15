import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@QuarkusTest
public class SqsTests {
    @Inject
    SqsClient sqs;

    @Inject
    SqsAsyncClient sqsAsync;

    @ConfigProperty(name = "sqs.queue.url")
    String queueUrl;

    @Test
    void testSend() {
        Long start = System.currentTimeMillis();
        SendMessageResponse response = sqs.sendMessage(m -> m
                .queueUrl(queueUrl)
                .messageBody("message" + UUID.randomUUID())
                .messageGroupId("a")
        );


        // Prepare messages to send

        // messageGroupId will be only relevant if multiple 10-messages-batches are actually processed in parallel threads
        String messageGroup = "a";

        List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            var id = UUID.randomUUID().toString();
            entries.add(SendMessageBatchRequestEntry.builder().messageBody("message" + id).messageGroupId(messageGroup).id(id).build());

            if(entries.size() == 10){
                Long startSend = System.currentTimeMillis();
                SendMessageBatchRequest request = SendMessageBatchRequest.builder().queueUrl(queueUrl).entries(entries).build();
                sqs.sendMessageBatch(request);
                Log.info("%d messages sent in: %d ms".formatted(entries.size(), System.currentTimeMillis()-startSend));
                entries.clear();
                messageGroup += "a";
            }
        }
//        sent 100 messages in 974 ms (test-perfo.fifo)
//        sent 100 messages in 874 ms (test.fifo)
        Log.info("all messages sent in: %d ms".formatted(System.currentTimeMillis()-start));
    }

    @Test
    void testReceive() {
        Long start = System.currentTimeMillis();
        int waitTimeSeconds = 0;

        while (true) {
            int finalWaitTimeSeconds = waitTimeSeconds;
            List<Message> messageList = sqs.receiveMessage(m -> m.maxNumberOfMessages(10).waitTimeSeconds(finalWaitTimeSeconds).messageAttributeNames("MessageGroupId").queueUrl(queueUrl)).messages();
            Log.info("Received %d messages".formatted(messageList.size()));
            if (messageList.isEmpty()) {
                Log.info("no further messages were received. Switching to long polling");
                waitTimeSeconds = 3;
//                Log.info("all messages were received. Terminating.");
//                break;
            }
            messageList.forEach(m -> {
                Log.info("message received:\tID=%s for messageGroupId %s".formatted(m.messageId(),m.messageAttributes().get("MessageGroupId")));
                Log.info("message body:\t%s".formatted(m.toString()));
//                sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(m.receiptHandle()).build());
            });

        }
//        received 100 message in 5s (test-perfo.fifo)
//        received 100 message in 5s (test.fifo)
//        Log.info("Receive finished in: %d ms".formatted(System.currentTimeMillis()-start));
    }


    @Test
    void testSendAndReceive() {
        sqs.sendMessage(m -> m.queueUrl(queueUrl).messageBody("message1").messageGroupId("a"));
        sqs.sendMessage(m -> m.queueUrl(queueUrl).messageBody("message2").messageGroupId("a"));

        List<Message> messageList = sqs.receiveMessage(m -> m.maxNumberOfMessages(1).queueUrl(queueUrl)).messages();
        //from here, message1 changes from 'available' to 'in-flight'
        //before visibilityTimeout(30s), sqs.receiveMessage returns no further message of the same messageGroup
        //after visibilityTimeout(30s),
        //  - message1 changes from 'in-flight' to 'available'
        //  - sqs.receiveMessage return message1 again
        Log.info("Received %d messages".formatted(messageList.size()));
        Log.info("message body:\t%s".formatted(messageList.get(0).toString()));

//        sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(messageList.get(0).receiptHandle()).build());
        //ONLY after deleting message1, sqs.receiveMessage will return message2
        List<Message> messageList2 = sqs.receiveMessage(m -> m.maxNumberOfMessages(1).queueUrl(queueUrl)).messages();
        Log.info("Received %d messages".formatted(messageList.size()));
        Log.info("message body:\t%s".formatted(messageList.get(0).toString()));
    }

    @Test
    void testSendAndReceiveMessageGroup() {
        sqs.sendMessage(m -> m.queueUrl(queueUrl).messageBody("message1").messageGroupId("a"));
        sqs.sendMessage(m -> m.queueUrl(queueUrl).messageBody("message2").messageGroupId("b"));

        List<Message> messageList = sqs.receiveMessage(m -> m.maxNumberOfMessages(1).queueUrl(queueUrl)).messages();
        Log.info("Received %d messages".formatted(messageList.size()));
        Log.info("message body:\t%s".formatted(messageList.get(0).toString()));

        List<Message> messageList2 = sqs.receiveMessage(m -> m.maxNumberOfMessages(1).queueUrl(queueUrl)).messages();
        Log.info("Received %d messages".formatted(messageList2.size()));
        Log.info("message body:\t%s".formatted(messageList2.get(0).toString()));
    }

    @Test
    void testSendAndReceiveMessageGroupIdentical() {
        sqs.sendMessage(m -> m.queueUrl(queueUrl).messageBody("message1").messageGroupId("a"));
        sqs.sendMessage(m -> m.queueUrl(queueUrl).messageBody("message2").messageGroupId("a"));

        List<Message> messageList = sqs.receiveMessage(m -> m.maxNumberOfMessages(1).queueUrl(queueUrl)).messages();
        Log.info("Received %d messages".formatted(messageList.size()));
        Log.info("message body:\t%s".formatted(messageList.get(0).toString()));

        List<Message> messageList2 = sqs.receiveMessage(m -> m.maxNumberOfMessages(1).queueUrl(queueUrl)).messages();
        Log.info("Received %d messages".formatted(messageList2.size()));
        Log.info("message body:\t%s".formatted(messageList2.get(0).toString()));
    }

    @Test
    void testSendAndReceiveMessageGroupIdenticalBatched() {
        sqs.sendMessage(m -> m.queueUrl(queueUrl).messageBody("message1").messageGroupId("a"));
        sqs.sendMessage(m -> m.queueUrl(queueUrl).messageBody("message2").messageGroupId("a"));

        List<Message> messageList = sqs.receiveMessage(m -> m.maxNumberOfMessages(2).queueUrl(queueUrl)).messages();
        Log.info("Received %d messages".formatted(messageList.size()));
        Log.info("message1 body:\t%s".formatted(messageList.get(0).toString()));
        Log.info("message2 body:\t%s".formatted(messageList.get(1).toString()));

        //simulate that only second message was successfully processed and thus deleted
        sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(messageList.get(1).receiptHandle()).build());

        List<Message> messageList2 = sqs.receiveMessage(m -> m.maxNumberOfMessages(1).queueUrl(queueUrl)).messages();
        //sqs.receiveMessage returns message1: on batch receive, fail on first error, don't continue with next message or order will not be preserved
        Log.info("Received %d messages".formatted(messageList2.size()));
        Log.info("message body:\t%s".formatted(messageList2.get(0).toString()));
    }


    @Test
    void dummyTest(){
        Log.info("in dummy test");
        testWorker();
        Log.info("test worker scheduled, working in background");
    }

    void testWorker() {
        final ExecutorService executor = Executors.newFixedThreadPool(5);
            executor.submit(() -> {
                Log.info("in test worker now");
                try {
                    Thread.sleep(10000);
                    Log.info("test worker finished");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
    }
}
