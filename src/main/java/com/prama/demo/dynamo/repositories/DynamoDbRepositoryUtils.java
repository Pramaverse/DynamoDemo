package com.prama.demo.dynamo.repositories;

import com.google.common.collect.Lists;
import com.prama.demo.dynamo.models.Customer;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.Document;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchGetResultPageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.ReadBatch;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public final class DynamoDbRepositoryUtils {

    private static final int MAX_BATCH_WRITE_SIZE = 25;
    private static final int MAX_BATCH_READ_SIZE = 100;

    private static final ExecutorService executorService = Executors.newFixedThreadPool(20);

    private DynamoDbRepositoryUtils() {
    }

    public static <T> T getItem(String keyValue, DynamoDbTable<T> table) {
        try {
            // Get the item by using the key.
            return table.getItem(r -> r.key(
                Key.builder()
                    .partitionValue(keyValue)
                    .build()));
        } catch (DynamoDbException e) {
            log.error("Failed while getting item.", e);
            throw e;
        }
    }

    public static <T> List<T> transactGetItems(DynamoDbEnhancedClient enhancedClient,
                                               DynamoDbTable<T> table,
                                               List<String> keys) {
        final List<Document> documents = enhancedClient.transactGetItems(r -> {
            for (String key : keys) {
                r.addGetItem(table, buildKey(key));
            }
        });

        return documents.stream()
            .map(doc -> doc.getItem(table))
            .collect(Collectors.toList());
    }

    public static <T> void batchPutRecords(DynamoDbEnhancedClient enhancedClient,
                                           DynamoDbTable<T> table,
                                           List<T> items,
                                           Class<T> clazz) {
        try {
            partitionedWrite(clazz, items, enhancedClient, table);
            System.out.println("done");

        } catch (DynamoDbException e) {
            log.error("Failed while batch pushing items.", e);
        }
    }

    /**
     * Reads a list of keys from the specified DynamoDB table.
     */
    public static <T> List<T> partitionedRead(Class<T> itemType,
                                              List<String> keys,
                                              DynamoDbEnhancedClient client,
                                              DynamoDbTable<T> table) {
        final List<T> retrievedItems = Collections.synchronizedList(new ArrayList<>());

        final List<List<String>> batchOfBatches = Lists.partition(keys, MAX_BATCH_READ_SIZE);
        System.out.println("Number of batches " + batchOfBatches.size());
        final List<Future<List<T>>> batchFutures = new ArrayList<>(batchOfBatches.size() + 1);
        batchOfBatches.forEach(chunkOfKeys -> batchFutures.add(executorService.submit(
            () -> batchRead(itemType, chunkOfKeys, client, table))));

        for (Future<List<T>> batchFuture : batchFutures) {
            try {
                final List<T> retrieved = batchFuture.get();
                System.out.println("Number retrieved: " + retrieved.size());
                retrievedItems.addAll(retrieved);
            } catch (Exception e) {
                log.error("Failed to fully process all batches. Response may be incomplete.", e);
            }
        }

        log.debug("Total number of retrieved items: " + retrievedItems.size());

        return retrievedItems;
    }

    /**
     * Reads a list of keys from the specified DynamoDB table.
     */
    public static <T> List<T> partitionedRead2(Class<T> itemType,
                                               List<String> keys,
                                               DynamoDbEnhancedClient client,
                                               DynamoDbTable<T> table) {
        final List<T> retrievedItems = Collections.synchronizedList(new ArrayList<>());

        final List<List<String>> batchOfBatches = Lists.partition(keys, MAX_BATCH_READ_SIZE);
        batchOfBatches.forEach(chunkOfKeys -> retrievedItems.addAll(batchRead(itemType, chunkOfKeys, client, table)));

        log.debug("Total number of retrieved items: " + retrievedItems.size());

        return retrievedItems;
    }

    /**
     * Writes the list of items to the specified DynamoDB table.
     */
    public static <T> void partitionedWrite(Class<T> itemType,
                                            List<T> items,
                                            DynamoDbEnhancedClient client,
                                            DynamoDbTable<T> table) {
        Stream<List<T>> chunksOfItems = Lists.partition(items, MAX_BATCH_WRITE_SIZE).stream();
        chunksOfItems.forEach(chunkOfItems -> executorService.submit(() -> {
            List<T> unprocessedItems = chunkOfItems;
            int timesLooped = 1;
            do {
                // some failed (provisioning problems, etc.), so write those again
                // TODO implement exponential backoff as suggested by AWS?
                unprocessedItems = batchWrite(itemType, unprocessedItems, client, table);
                System.out.println("Times looped: " + timesLooped++);
            } while (!unprocessedItems.isEmpty());
        }));
    }

    /**
     * Reads a single batch of (at most) 100 items from DynamoDB. Note that the overall limit of items in a batch is
     * 100, so you can't have nested batches of 100 each that would exceed that overall limit.
     *
     * @return those items that couldn't be written due to provisioning issues, etc., but were otherwise valid
     */
    public static <T> List<T> batchRead(Class<T> itemType,
                                        List<String> chunkOfKeys,
                                        DynamoDbEnhancedClient client,
                                        DynamoDbTable<T> table) {
        ReadBatch.Builder<T> builder = ReadBatch.builder(itemType).mappedTableResource(table);
        chunkOfKeys.forEach(key -> builder.addGetItem(req -> req.key(buildKey(key))));

        try {
            final BatchGetResultPageIterable result = client.batchGetItem(
                r -> r.addReadBatch(builder.build()));
            final SdkIterable<T> resultsForTable = result.resultsForTable(table);
            System.out.println("Results for table count: " + resultsForTable.stream().count());
            return resultsForTable.stream().collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Failed to process batch.", e);
            return Collections.emptyList();
        }
    }

    /**
     * Writes a single batch of (at most) 25 items to DynamoDB. Note that the overall limit of items in a batch is 25,
     * so you can't have nested batches of 25 each that would exceed that overall limit.
     *
     * @return those items that couldn't be written due to provisioning issues, etc., but were otherwise valid
     */
    public static <T> List<T> batchWrite(Class<T> itemType,
                                         List<T> chunkOfItems,
                                         DynamoDbEnhancedClient client,
                                         DynamoDbTable<T> table) {
        WriteBatch.Builder<T> subBatchBuilder = WriteBatch.builder(itemType).mappedTableResource(table);
        chunkOfItems.forEach(subBatchBuilder::addPutItem);

        try {
            return client.batchWriteItem(b -> b.addWriteBatch(subBatchBuilder.build()))
                .unprocessedPutItemsForTable(table);
        } catch (Exception e) {
            log.error("Failed to process batch.", e);
            return Collections.emptyList();
        }
    }

    public static Key buildKey(String key) {
        return Key.builder().partitionValue(key).build();
    }

    public static <T> List<T> batchGetItem(DynamoDbEnhancedClient client,
                                           DynamoDbTable<T> table,
                                           ReadBatch.Builder<Customer> builder) {
        final BatchGetResultPageIterable batchResult =
            client.batchGetItem(r -> r.addReadBatch(builder.build()));
        final SdkIterable<T> customers = batchResult.resultsForTable(table);

        return customers.stream().collect(Collectors.toList());
    }
}
