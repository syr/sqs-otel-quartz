package org.acme.quartz;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.MDC;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class SqsSendReceive {
    @Inject
    SqsClient sqs;

    @Inject
    SqsAsyncClient sqsAsync;

    @ConfigProperty(name = "sqs.queue.url")
    String queueUrl;

    @ConfigProperty(name = "sqs.client.async")
    Boolean async;


    @Scheduled(every = "3s", identity = "send-job")
    public void send(){
        //FIXME How to get MDCs updated in a less cumbersome/invasive way?
        MDC.put("traceId", Span.current().getSpanContext().getTraceId());
        MDC.put("spanId", Span.current().getSpanContext().getSpanId());
        if(async) sendAsync(); else sendSync();
    }

    private void sendSync() {
        SendMessageResponse response = sqs.sendMessage(m -> m
                .queueUrl(queueUrl)
                .messageBody("message" + UUID.randomUUID())
                .messageGroupId("a")
        );
        Log.info("message sent\t\tID=%s".formatted(response.messageId()));
    }

    private void sendAsync() {
        Uni.createFrom()
                .completionStage(sqsAsync.sendMessage(m -> m
                        .queueUrl(queueUrl)
                        .messageBody("message" + UUID.randomUUID())
                        .messageGroupId("a"))
                )
                .onItem().transform(SendMessageResponse::messageId)
                .onItem().invoke(messageId -> Log.info("message sent\t\tID=%s".formatted(messageId)))
                .subscribe().with(ignord -> {})
        ;
    }

    @Scheduled(every = "2s", identity = "receive-job")
    public void receive(){
        //FIXME How to get MDCs updated in a less cumbersome/invasive way?
        MDC.put("traceId", Span.current().getSpanContext().getTraceId());
        MDC.put("spanId", Span.current().getSpanContext().getSpanId());
        if(async) receiveAsync(); else receiveSync();
        postReceive();
    }

    private void receiveSync() {
        sqs.receiveMessage(m -> m.maxNumberOfMessages(1).queueUrl(queueUrl)).messages().forEach(m -> {
            //FIXME How to get MDCs updated in a less cumbersome/invasive way?
            MDC.put("traceId", Span.current().getSpanContext().getTraceId());
            MDC.put("spanId", Span.current().getSpanContext().getSpanId());
            Log.info("message received\tID=%s".formatted(m.messageId()));
            Log.info("message system attributes: %s".formatted(m.attributes().entrySet()
                    .stream()
                    .map(e -> e.getKey() + "=\"" + e.getValue() + "\"")
                    .collect(Collectors.joining(", "))));
            sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(m.receiptHandle()).build());
        });
    }

    private void receiveAsync() {
        Uni.createFrom()
                .completionStage(sqsAsync.receiveMessage(m -> m.maxNumberOfMessages(1).queueUrl(queueUrl)))
                .onItem().transform(ReceiveMessageResponse::messages)
                .onItem().invoke(msgList -> msgList.forEach(m -> {
                    Log.info("message received\tID=%s".formatted(m.messageId()));
                    Log.info("message system attributes: %s".formatted(m.attributes().entrySet()
                            .stream()
                            .map(e -> e.getKey() + "=\"" + e.getValue() + "\"")
                            .collect(Collectors.joining(", "))));
                    //FIXME not instrumented by otel agent -> no span created in trace
                    sqsAsync.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(m.receiptHandle()).build());
                }))
                .subscribe().with(ignord -> {});
    }

    @WithSpan("postReceive")
    public void postReceive(){
        //FIXME How to get MDCs updated in a less cumbersome/invasive way?
        MDC.put("traceId", Span.current().getSpanContext().getTraceId());
        MDC.put("spanId", Span.current().getSpanContext().getSpanId());
        Log.info("postReceive");
    }
}