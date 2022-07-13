package com.prama.demo.dynamo.repositories;

import com.prama.demo.dynamo.models.Customer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static com.prama.demo.dynamo.repositories.DynamoDbRepositoryUtils.getItem;
import static com.prama.demo.dynamo.repositories.DynamoDbRepositoryUtils.putBatchRecords;

@Slf4j
@Component
public class CustomerRepository {

    private static final Class<Customer> CLASS = Customer.class;
    private static final String TABLE_NAME = "bryce_test";

    private final DynamoDbEnhancedClient enhancedClient;

    public CustomerRepository(DynamoDbEnhancedClient enhancedClient) {
        this.enhancedClient = enhancedClient;
    }

    public void batchRegisterCustomers(List<Customer> customers) {
        LocalDateTime localDateTime = LocalDate.now().atStartOfDay();
        Instant instant = localDateTime.toInstant(ZoneOffset.UTC);

        for (Customer customer : customers) {
            customer.setRegistrationDate(instant);
        }

        DynamoDbTable<Customer> mappedTable =
            enhancedClient.table(TABLE_NAME, TableSchema.fromBean(CLASS));

        // Add these two items to the table.
        putBatchRecords(enhancedClient, mappedTable, customers, CLASS);
    }

    public Customer getCustomer(String key) {
        final DynamoDbTable<Customer> table = enhancedClient.table(TABLE_NAME, TableSchema.fromBean(CLASS));
        return getItem(key, table);
    }

}
