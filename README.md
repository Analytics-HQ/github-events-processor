## requirements.txt
```
requests==2.32.3
sseclient-py==1.8.0
kafka-python==2.1.5
fastavro==1.10.0
structlog==25.2.0
microsoft-kiota-abstractions==1.9.3
microsoft-kiota-http==1.9.3
apicurioregistryclient==0.6.2
apicurioregistrysdk==3.0.6
```

## Kafka User Spec
```yaml
spec:
  authentication:
    password:
      valueFrom:
        secretKeyRef:
          key: password
          name: kfk-u-github-ccravens
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource:
          name: github-events-processor
          patternType: literal
          type: group
        operations:
          - Read
          - Describe
      - resource:
          name: kfk-t-github-sink
          patternType: literal
          type: topic
        operations:
          # - Create
          # - Describe
          - Read
          - Write
      - resource:
          name: kfk-t-github-events
          patternType: literal
          type: topic
        operations:
          - Write
```

## Sample Trino URL for Superset
```
trino://admin@tno-github.env-45y9fr.svc.dev.ahq:8080/nse-github/default?auth=JWT&password=CyYFo3bpseYr4BiCbDZ3jDOlO4bYOocuis09bdhBYS0TrTf7nOxiiMeBgtLJC3CFU9Ax2UFa6pfeYM4sx6GInM8JepK3vovHVcTddocxmbbWJgbhacpmWxEgQNk9rO0L
```

## Build and Run
```
mvn clean package
KAFKA_BOOTSTRAP_SERVER='kfk-github-kafka-bootstrap.env-g0vgp2.svc.dev.ahq:9092' KAFKA_INPUT_TOPIC='kfk-t-github-sink' KAFKA_OUTPUT_TOPIC='kfk-t-github-events' KAFKA_USERNAME='kfk-u-github-ccravens' KAFKA_PASSWORD='GkugKjwtoTwYFC2OYAbmLjkbLw3oWMuT' java -jar target/github-events-processor-1.0-SNAPSHOT.jar
```

## Schema
```
{
  "type": "record",
  "name": "Event",
  "namespace": "com.analyticshq.github",
  "fields": [
    {
      "name": "id",
      "type": "string"
    },
    {
      "name": "type",
      "type": "string"
    },
    {
      "name": "created_at",
      "type": "string"
    },
    {
      "name": "public",
      "type": "boolean"
    },
    {
      "name": "actor_name",
      "type": "string"
    },
    {
      "name": "repo_name",
      "type": "string"
    },
    {
      "name": "ts",
      "type": "string"
    }
  ]
}
```
