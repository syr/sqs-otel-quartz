# Init. AWS profile 'localstack' (if not exists)
```
$ aws configure --profile localstack
AWS Access Key ID [None]: test-key
AWS Secret Access Key [None]: test-secret
Default region name [None]: us-east-1
Default output format [None]:
```

# Start local SQS
```
docker run --rm --name local-sqs -p 8010:4576 -e SERVICES=sqs -e START_WEB=0 -d localstack/localstack:0.11.1
```

# Create SQS queue
```
aws sqs create-queue --queue-name=ColliderQueue --profile localstack --endpoint-url=http://localhost:8010
```

# Run Quarkus App with OpenTelemetry Agent Config
 ```
JAVA_TOOL_OPTIONS=-javaagent:./otel/opentelemetry-javaagent.jar -Dotel.traces.exporter=otlp -Dotel.metrics.exporter=none
```