package com.prama.demo.dynamo.repositories;

import com.prama.demo.dynamo.models.Customer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.ReadBatch;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;

import static com.prama.demo.dynamo.repositories.DynamoDbRepositoryUtils.batchPutRecords;
import static com.prama.demo.dynamo.repositories.DynamoDbRepositoryUtils.buildKey;
import static com.prama.demo.dynamo.repositories.DynamoDbRepositoryUtils.getItem;
import static com.prama.demo.dynamo.repositories.DynamoDbRepositoryUtils.transactGetItems;

@Slf4j
@Component
public class CustomerRepository {

    private static final Class<Customer> CLASS = Customer.class;
    private static final String TABLE_NAME = "bryce_test";

    private final DynamoDbEnhancedClient enhancedClient;
    private final DynamoDbTable<Customer> table;

    public CustomerRepository(DynamoDbEnhancedClient enhancedClient) {
        this.enhancedClient = enhancedClient;
        table = enhancedClient.table(TABLE_NAME, TableSchema.fromBean(CLASS));
    }

    public void batchRegisterCustomers(List<Customer> customers) {
        LocalDateTime localDateTime = LocalDate.now().atStartOfDay();
        Instant instant = localDateTime.toInstant(ZoneOffset.UTC);

        // Using now for registrationDate
        customers = customers.stream()
            .map(c -> Customer.builder()
                .id(c.getId())
                .custName(c.getCustName())
                .email(c.getEmail())
                .registrationDate(instant)
                .build())
            .collect(Collectors.toList());

        // Add these two items to the table.
        batchPutRecords(enhancedClient, table, customers, CLASS);
    }

    public Customer getCustomer(String key) {
        return getItem(key, table);
    }

    public List<Customer> batchGetCustomers(List<String> keys) {
        return transactGetItems(enhancedClient, table, keys);
    }

    public List<Customer> batchGetCustomers2(List<String> keys) {
        final ReadBatch.Builder<Customer> builder = ReadBatch.builder(Customer.class)
            .mappedTableResource(table);
        for (String key : keys) {
            builder.addGetItem(buildKey(key));
        }

        return DynamoDbRepositoryUtils.batchGetItem(enhancedClient, table, builder);
    }

}
