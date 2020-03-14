package com.kep.publisher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

public class PubSubEventPublisher {

  private Logger logger = LoggerFactory.getLogger(PubSubEventPublisher.class);

  private Publisher publisher;
  private List<ApiFuture<String>> futures;

  public PubSubEventPublisher(String projectId, String topicId) {
    logger.debug("Creating publisher with projectId={} and topicId={}", projectId, topicId);
    publisher = buildPublisher(StringUtils.trim(projectId), StringUtils.trim(topicId));
    futures = new ArrayList<>();
    addShutdownHook();
  }

  public void send(String message) {
    logger.trace("Publishing value={}", message);
    ByteString data = ByteString.copyFromUtf8(message);
    PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
    ApiFuture<String> future = publisher.publish(pubsubMessage);
    addFutureCallback(message, future);
    futures.add(future);
  }

  private Publisher buildPublisher(String projectId, String topicId) {
    ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
    try {
      return Publisher.newBuilder(topicName).build();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  private void addFutureCallback(String message, ApiFuture<String> future) {
    ApiFutures.addCallback(future,
        new ApiFutureCallback<String>() {
          @Override
          public void onFailure(Throwable throwable) {
            logger.error("Error publishing message={}", message);
            if (throwable instanceof ApiException) {
              ApiException apiException = ((ApiException) throwable);
              logger.error("ApiException statusCode={}, isRetryable={}", apiException.getStatusCode().getCode(), apiException.isRetryable());
            }
          }

          @Override
          public void onSuccess(String messageId) {
            logger.debug("Published messageId={}", messageId);
          }
        },
        MoreExecutors.directExecutor());
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Shutting down publisher ");

      try {
        logger.info("Finishing publish messages");
        ApiFutures.allAsList(futures).get();
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }

      publisher.shutdown();
      logger.info("Done shutting down publisher");
    }));
  }

}
