import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import javax.inject.Inject;
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

        SendMessageResponse response = sqs.sendMessage(m -> m
                .queueUrl(queueUrl)
                .messageBody("message" + UUID.randomUUID())
                .messageGroupId("a")
        );

        //sqs.sendMessageBatch() TODO try later
        Log.info("message sent\t\tID=%s".formatted(response.messageId()));
    }
}
