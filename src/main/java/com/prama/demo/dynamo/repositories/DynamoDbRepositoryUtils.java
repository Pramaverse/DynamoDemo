package com.prama.demo.dynamo.repositories;

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

import java.util.List;
import java.util.stream.Collectors;

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
