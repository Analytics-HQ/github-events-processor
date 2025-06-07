
## Schema Validation
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

```
{
  "type": "record",
  "name": "User",
  "namespace": "com.analyticshq.github",
  "fields": [
    {
      "name": "id",
      "type": "string"
    },
    {
      "name": "login",
      "type": "string"
    },
    {
      "name": "display_login",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "gravatar_id",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "url",
      "type": "string"
    },
    {
      "name": "avatar_url",
      "type": "string"
    }
  ]
}
```

## Data Validation
`data_validation_rules.json`
```
{
  "expectations": [
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": { "column": "id" }
    },
    {
      "expectation_type": "expect_column_values_to_be_unique",
      "kwargs": { "column": "id" }
    },
    {
      "expectation_type": "expect_column_values_to_be_in_set",
      "kwargs": { "column": "public", "value_set": [true, false] }
    }
  ]
}
```

## requirements.txt
```
## Apicurio Dependencies
requests==2.32.3
sseclient-py==1.8.0

## Apicurio Dependencies
apicurioregistryclient==0.6.2
apicurioregistrysdk==3.0.6
microsoft-kiota-abstractions==1.9.3
microsoft-kiota-http==1.9.3
fastavro==1.10.0
avro==1.12.0

## Kafka Dependencies
kafka-python==2.1.5

## Automated Validation Dependencies
great_expectations==1.3.13
soda_core==3.5.2
pandas==2.1.4
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
      - operations:
          - Read
          - Describe
        resource:
          name: github-events-processor
          patternType: literal
          type: group
      - operations:
          - Read
          - Describe
        resource:
          name: github-users-processor
          patternType: literal
          type: group
      - operations:
          - Write
        resource:
          name: kfk-t-github-events
          patternType: literal
          type: topic
      - operations:
          - Write
        resource:
          name: kfk-t-github-users
          patternType: literal
          type: topic
```

## Sample Trino URL for Superset
```
trino://admin@tno-github.env-1czw4v.svc.dev.ahq:8080/nse-github/default?auth=JWT&password=LOF2cZ6A5HwOKMzHupoMoJOnsiRsQRgWAlOY7XUvPboR1Y9tRqI2fK2rOsYHSIN6TE9975d85tnq3IdcJdVFHuGoMiCqUDvATc04xYfvQbUOtj2H1PSZvicuZWTCKQNc
```

## Build and Run
```
mvn clean package
KAFKA_BOOTSTRAP_SERVER='kfk-github-kafka-bootstrap.env-1czw4v.svc.dev.ahq:9092' KAFKA_INPUT_TOPIC='kfk-t-github-sink' KAFKA_OUTPUT_TOPIC='kfk-t-github-events' KAFKA_USERNAME='kfk-u-github-ccravens' KAFKA_PASSWORD='GkugKjwtoTwYFC2OYAbmLjkbLw3oWMuT' java -jar target/github-events-processor-1.0-SNAPSHOT.jar
```
