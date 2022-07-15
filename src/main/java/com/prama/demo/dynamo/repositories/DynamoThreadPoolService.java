package com.prama.demo.dynamo.repositories;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * Implementation of {@link DynamoService} that uses an executor thread pool to make multiple batch calls with
 * chunks of writes or reads in parallel.
 */
@Slf4j
public class DynamoThreadPoolService extends DynamoService {
    private final ExecutorService executorService;

    /**
     * Use this for testing if you don't want to specifically create an executor thread pool.
     */
    public DynamoThreadPoolService(DynamoDbEnhancedClient enhancedClient) {
        this(enhancedClient, Executors.newFixedThreadPool(20));
    }

    public DynamoThreadPoolService(DynamoDbEnhancedClient enhancedClient, ExecutorService executorService) {
        super(enhancedClient);
        this.executorService = executorService;
    }

    /**
     * Reads a list of rows given keys from the specified DynamoDB table.
     */
    public <T> List<T> partitionedRead(Class<T> itemType, List<String> keys, DynamoDbTable<T> table) {
        final List<T> retrievedItems = Collections.synchronizedList(new ArrayList<>());

        final List<List<String>> batchOfChunksOfKeys = Lists.partition(keys, MAX_BATCH_READ_SIZE);
        log.debug("Number of batches {}", batchOfChunksOfKeys.size());

        final List<Future<List<T>>> batchFutures = new ArrayList<>(batchOfChunksOfKeys.size() + 1);
        batchOfChunksOfKeys.forEach(chunkOfKeys -> batchFutures.add(executorService.submit(
            () -> batchRead(itemType, chunkOfKeys, table))));

        for (Future<List<T>> batchFuture : batchFutures) {
            try {
                final List<T> retrieved = batchFuture.get();
                log.info("Number retrieved: {}", retrieved.size());
                retrievedItems.addAll(retrieved);
            } catch (Exception e) {
                log.error("Failed to fully process all batches. Response may be incomplete.", e);
            }
        }

        log.debug("Total number of retrieved items: " + retrievedItems.size());

        return retrievedItems;
    }

    /**
     * Writes the list of items to the specified DynamoDB table.
     */
    public <T> void partitionedWrite(DynamoDbTable<T> table, Class<T> itemType, List<T> items) {
        Stream<List<T>> chunksOfItems = Lists.partition(items, MAX_BATCH_WRITE_SIZE).stream();
        chunksOfItems.forEach(chunk -> executorService.submit(() -> partitionedWriteForChunk(table, itemType, chunk)));
    }

}
