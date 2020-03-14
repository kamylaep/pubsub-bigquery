package com.kep;

import com.kep.bigquery.BigQueryClient;
import com.kep.publisher.PubSubEventPublisher;

public class PublisherMain {

  public static void main(String[] args) {
    String projectId = System.getProperty("PROJECT_ID");
    String topicId = System.getProperty("TOPIC_ID");
    String query = System.getProperty("QUERY");

    PubSubEventPublisher pubSubEventPublisher = new PubSubEventPublisher(projectId, topicId);
    BigQueryClient bigQueryClient = new BigQueryClient(query);
    bigQueryClient.readData(data -> pubSubEventPublisher.send(data));
  }
}
