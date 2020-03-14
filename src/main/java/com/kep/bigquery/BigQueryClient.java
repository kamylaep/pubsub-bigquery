package com.kep.bigquery;

import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;
import org.threeten.bp.temporal.ChronoUnit;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

public class BigQueryClient {

  private Logger logger = LoggerFactory.getLogger(BigQueryClient.class);
  private BigQuery bigquery;
  private QueryJobConfiguration queryJobConfiguration;

  public BigQueryClient(String query) {
    logger.debug("Creating BigQueryClient");
    bigquery = BigQueryOptions.getDefaultInstance().getService();

    logger.debug("Creating QueryJobConfiguration");
    queryJobConfiguration = buildQuery(query);
  }

  public void readData(Consumer<String> consumer) {
    try {
      Job queryJob = buildJob(queryJobConfiguration);
      checkErrors(queryJob);
      processResult(consumer, queryJob.getQueryResults());
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    } finally {
      System.exit(0);
    }
  }

  private QueryJobConfiguration buildQuery(String query) {
    return QueryJobConfiguration.newBuilder(query)
        .setUseLegacySql(false)
        .build();
  }

  private Job buildJob(QueryJobConfiguration queryConfig) throws InterruptedException {
    String jobUUID = UUID.randomUUID().toString();
    logger.debug("Creating job={}", jobUUID);
    JobId jobId = JobId.of(jobUUID);
    return bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())
        .waitFor(RetryOption.initialRetryDelay(Duration.of(20000, ChronoUnit.MILLIS)));
  }

  private void checkErrors(Job queryJob) {
    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    }

    if (queryJob.getStatus().getError() != null) {
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }
  }

  private void processResult(Consumer<String> consumer, TableResult result) {
    logger.debug("Processing result");

    for (FieldValueList row : result.iterateAll()) {
      for (Field field : result.getSchema().getFields()) {
        if (Objects.isNull(field)) {
          return;
        }
        String value = row.get(field.getName()).getStringValue();
        logger.trace("Processing value={}", value);
        consumer.accept(value);
      }
    }

    logger.debug("Finished processing result");
  }
}
