# GCP BigQuery to Pub/Sub 

Reads data from BigQuery and publishes it to Pub/Sub.

## Configuration

The following environment variable must be set:
- GOOGLE_APPLICATION_CREDENTIALS

The following system properties must be set to produce data:
- PROJECT_ID
- TOPIC_ID
- QUERY
- 

## Build

```shell script
$ mvn clean package
```

## Example

To read the title from each post from the Hacker News BigQuery public dataset, use the following parameters:

```shell script
$ export GOOGLE_APPLICATION_CREDENTIALS=<path-to-credentials> && \
  mvn compile exec:java -Dexec.mainClass=com.kep.PublisherMain \
  -DPROJECT_ID=<your-project-id> \
  -DTOPIC_ID=hackernews \
  -DQUERY="SELECT title FROM \`bigquery-public-data.hacker_news.full\` WHERE title is not null AND timestamp > timestamp('2019-01-01 00:00:00') AND timestamp < timestamp('2019-12-31 00:00:00')" 
```

Or, if you have the fat/uber/shaded jar:

```shell script
$ export GOOGLE_APPLICATION_CREDENTIALS=<path-to-credentials> && \
  java -DPROJECT_ID=<your-project-id> \
  -DTOPIC_ID=hackernews \
  -DQUERY="SELECT title FROM \`bigquery-public-data.hacker_news.full\` WHERE title is not null AND timestamp > timestamp('2019-01-01 00:00:00') AND timestamp < timestamp('2019-12-31 00:00:00')" \
  -jar ./target/pubsub-bigquery-1.0.0-SNAPSHOT.jar
```

To create the topic and the subscription:

```shell script
$ gcloud pubsub topics create hackernews
$ gcloud pubsub subscriptions create sub-hackernews --topic hackernews
```
