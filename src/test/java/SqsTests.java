import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

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
        List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            var id = UUID.randomUUID().toString();
            entries.add(SendMessageBatchRequestEntry.builder().messageBody("message" + id).messageGroupId("a").id(id).build());

            if(entries.size() == 10){
                Long startSend = System.currentTimeMillis();
                SendMessageBatchRequest request = SendMessageBatchRequest.builder().queueUrl(queueUrl).entries(entries).build();
                sqs.sendMessageBatch(request);
                Log.info("%d messages sent in: %d ms".formatted(entries.size(), System.currentTimeMillis()-startSend));
                entries.clear();
            }
        }
//        sent 100 messages in 974 ms (test-perfo.fifo)
//        sent 100 messages in 874 ms (test.fifo)
        Log.info("all messages sent in: %d ms".formatted(System.currentTimeMillis()-start));
    }

    @Test
    void testReceive() {
        Long start = System.currentTimeMillis();

        while (true) {
            sqs.receiveMessage(m -> m.maxNumberOfMessages(10).queueUrl(queueUrl)).messages().forEach(m -> {
                Log.info("message received:\tID=%s".formatted(m.messageId()));
                Log.info("message body:\t%s".formatted(m.toString()));
                sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(m.receiptHandle()).build());
            });
        }
//        received 100 message in 5s (test-perfo.fifo)
//        received 100 message in 5s (test.fifo)
//        Log.info("Receive finished in: %d ms".formatted(System.currentTimeMillis()-start));
    }
}
