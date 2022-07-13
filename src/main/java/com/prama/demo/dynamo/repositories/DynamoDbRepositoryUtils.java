package com.prama.demo.dynamo.repositories;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import java.util.List;

@Slf4j
public final class DynamoDbRepositoryUtils {

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

    public static <T> void putBatchRecords(DynamoDbEnhancedClient enhancedClient,
                                           DynamoDbTable<T> mappedTable,
                                           List<T> items,
                                           Class<T> clazz) {
        try {
            final WriteBatch.Builder<T> writeBatchBuilder = WriteBatch.builder(clazz)
                .mappedTableResource(mappedTable);
            for (T item : items) {
                writeBatchBuilder.addPutItem(item);
            }

            enhancedClient.batchWriteItem(request -> request.addWriteBatch(writeBatchBuilder.build()));
            System.out.println("done");

        } catch (DynamoDbException e) {
            log.error("Failed while batch pushing items.", e);
        }
    }

}
