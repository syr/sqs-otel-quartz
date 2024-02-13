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
}
