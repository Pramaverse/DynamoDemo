package com.prama.demo.dynamo.repositories;

import com.google.common.collect.Lists;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Service for interacting with dynamo db in retrieving or writing data. Also has support for batch calls. Uses the
 * {@link DynamoDbEnhancedClient} to make the calls.
 */
@Slf4j
public class DynamoService {
    static final int MAX_BATCH_WRITE_SIZE = 25;
    static final int MAX_BATCH_READ_SIZE = 100;

    protected final DynamoDbEnhancedClient enhancedClient;

    public DynamoService(DynamoDbEnhancedClient enhancedClient) {
        this.enhancedClient = enhancedClient;
    }

    public <T> T getItem(String keyValue, DynamoDbTable<T> table) {
        try {
            // Get the item by using the key.
            return table.getItem(r -> r.key(buildKey(keyValue)));
        } catch (DynamoDbException e) {
            log.error("Failed while getting item.", e);
            throw e;
        }
    }

    public <T> List<T> transactGetItems(DynamoDbTable<T> table, List<String> keys) {
        final List<Document> documents = enhancedClient.transactGetItems(r -> {
            for (String key : keys) {
                r.addGetItem(table, buildKey(key));
            }
        });

        return documents.stream()
            .map(doc -> doc.getItem(table))
            .collect(Collectors.toList());
    }

    public <T> void batchPutRecords(DynamoDbTable<T> table, List<T> items, Class<T> clazz) {
        try {
            partitionedWrite(table, clazz, items);
        } catch (DynamoDbException e) {
            log.error("Failed while batch putting items.", e);
        }
    }

    /**
     * Reads a list of keys from the specified DynamoDB table.
     */
    public <T> List<T> partitionedRead(Class<T> itemType, List<String> keys, DynamoDbTable<T> table) {
        final List<T> retrievedItems = new ArrayList<>();

        final List<List<String>> batchOfChunksOfKeys = Lists.partition(keys, MAX_BATCH_READ_SIZE);
        batchOfChunksOfKeys.forEach(chunkOfKeys -> retrievedItems.addAll(batchRead(itemType, chunkOfKeys, table)));

        log.debug("Total number of retrieved items: " + retrievedItems.size());

        return retrievedItems;
    }

    /**
     * Writes the list of items to the specified DynamoDB table.
     */
    public <T> void partitionedWrite(DynamoDbTable<T> table, Class<T> itemType, List<T> items) {
        Stream<List<T>> chunksOfItems = Lists.partition(items, MAX_BATCH_WRITE_SIZE).stream();
        chunksOfItems.forEach(chunk -> partitionedWriteForChunk(table, itemType, chunk));
    }

    public <T> void partitionedWriteForChunk(DynamoDbTable<T> table, Class<T> itemType, List<T> chunk) {
        List<T> unprocessedItems = chunk;
        int timesLooped = 1;
        do {
            // some failed (provisioning problems, etc.), so write those again
            // TODO implement exponential backoff as suggested by AWS?
            unprocessedItems = batchWrite(itemType, unprocessedItems, table);
            timesLooped++;

            // We only care if we are looping more than once.
            if (timesLooped > 1) {
                log.debug("Times looped: " + timesLooped);
            }
        } while (!unprocessedItems.isEmpty());
    }

    /**
     * Reads a single batch of (at most) 100 items from DynamoDB. Note that the overall limit of items in a batch is
     * 100, so you can't have nested batches of 100 each that would exceed that overall limit.
     *
     * @return those items that couldn't be written due to provisioning issues, etc., but were otherwise valid
     */
    public <T> List<T> batchRead(Class<T> itemType, List<String> chunkOfKeys, DynamoDbTable<T> table) {
        ReadBatch.Builder<T> builder = ReadBatch.builder(itemType).mappedTableResource(table);
        chunkOfKeys.forEach(key -> builder.addGetItem(req -> req.key(buildKey(key))));

        try {
            final BatchGetResultPageIterable result = enhancedClient.batchGetItem(
                r -> r.addReadBatch(builder.build()));
            final SdkIterable<T> resultsForTable = result.resultsForTable(table);
            log.info("Results for table count: " + resultsForTable.stream().count());
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
    public <T> List<T> batchWrite(Class<T> itemType, List<T> chunkOfItems, DynamoDbTable<T> table) {
        WriteBatch.Builder<T> subBatchBuilder = WriteBatch.builder(itemType).mappedTableResource(table);
        chunkOfItems.forEach(subBatchBuilder::addPutItem);

        try {
            return enhancedClient.batchWriteItem(b -> b.addWriteBatch(subBatchBuilder.build()))
                .unprocessedPutItemsForTable(table);
        } catch (Exception e) {
            log.error("Failed to process batch.", e);
            return Collections.emptyList();
        }
    }

    public <T> List<T> batchGetItem(DynamoDbEnhancedClient client,
                                    DynamoDbTable<T> table,
                                    ReadBatch.Builder<T> builder) {
        final BatchGetResultPageIterable batchResult = client.batchGetItem(r -> r.addReadBatch(builder.build()));
        final SdkIterable<T> customers = batchResult.resultsForTable(table);

        return customers.stream().collect(Collectors.toList());
    }

    public static Key buildKey(String key) {
        return Key.builder().partitionValue(key).build();
    }

}
