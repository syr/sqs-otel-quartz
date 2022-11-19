package org.acme.quartz;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.MDC;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Map;
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

    private static final String TRACE_ID_KEY = "traceId";
    private static final String SPAN_ID_KEY = "spanId";


    @Scheduled(every = "3s", identity = "send-job")
    public void send(){
        //FIXME How to get MDCs updated in a less cumbersome/invasive way?
        MDC.put("traceId", Span.current().getSpanContext().getTraceId());
        MDC.put("spanId", Span.current().getSpanContext().getSpanId());
        if(async) sendAsync(); else sendSync();
    }

    private void sendSync() {
        SpanContext spanContext = Span.current().getSpanContext();
        String traceId = spanContext.getTraceId();
        String spanId = spanContext.getSpanId();
        Map<String, MessageAttributeValue> messageAttributes = Map.of(
                TRACE_ID_KEY, MessageAttributeValue.builder().dataType("String").stringValue(traceId).build(),
                SPAN_ID_KEY, MessageAttributeValue.builder().dataType("String").stringValue(spanId).build()
        );

        SendMessageResponse response = sqs.sendMessage(m -> m
                .queueUrl(queueUrl)
                .messageBody("message" + UUID.randomUUID())
                .messageAttributes(messageAttributes)
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
        postReceive(); //with the traceId from this method we can see the original Sqs.ReceiveMessage span
    }

    private void receiveSync() {
        sqs.receiveMessage(m -> m
                .maxNumberOfMessages(1)
                .queueUrl(queueUrl)
                .messageAttributeNames(TRACE_ID_KEY, SPAN_ID_KEY)
        ).messages().forEach(m -> {
            String traceId = m.messageAttributes().get(TRACE_ID_KEY).stringValue();
            String spanId = m.messageAttributes().get(SPAN_ID_KEY).stringValue();

            //restore remote trace
            Span span = createSpanLinkedToParent(traceId, spanId);
            try (Scope scope = span.makeCurrent()) {
                //FIXME How to get MDCs updated in a less cumbersome/invasive way?
                MDC.put("traceId", Span.current().getSpanContext().getTraceId());
                MDC.put("spanId", Span.current().getSpanContext().getSpanId());
                Log.info("message received\tID=%s".formatted(m.messageId()));
                Log.info("message system attributes: %s".formatted(m.attributes().entrySet()
                        .stream()
                        .map(e -> e.getKey() + "=\"" + e.getValue() + "\"")
                        .collect(Collectors.joining(", "))));

                deleteMessage(m);
                postReceive();
            } finally {
                span.end();
            }
        });
    }

    @WithSpan("deleteMessage")
    private void deleteMessage(Message m) {
        //FIXME How to get MDCs updated in a less cumbersome/invasive way?
        MDC.put("traceId", Span.current().getSpanContext().getTraceId());
        MDC.put("spanId", Span.current().getSpanContext().getSpanId());
        sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(m.receiptHandle()).build());
    }

    private static Span createSpanLinkedToParent(String traceId, String spanId) {
        // Fetch the trace and span IDs from wherever you've stored them
        SpanContext remoteContext = SpanContext.createFromRemoteParent(
                traceId,
                spanId,
                TraceFlags.getSampled(),
                TraceState.getDefault());

        return GlobalOpenTelemetry.getTracer("")
                .spanBuilder("Sqs.ReceiveMessage")
                .setParent(Context.current().with(Span.wrap(remoteContext)))
                .startSpan();
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