package com.prama.demo.dynamo.repositories;

import com.prama.demo.dynamo.models.Customer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Slf4j
@Component
public class DynamoDbRepository {

    public String batchGet() {
        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
        Region region = Region.US_WEST_2;
        DynamoDbClient ddb = DynamoDbClient.builder()
            .credentialsProvider(credentialsProvider)
            .region(region)
            .build();

        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
            .dynamoDbClient(ddb)
            .build();

        putBatchRecords(enhancedClient);

        getItem(enhancedClient);
        ddb.close();

        return "test2";
    }

    public static void getItem(DynamoDbEnhancedClient enhancedClient) {

        try {
            DynamoDbTable<Customer> table = enhancedClient.table("bryce_test", TableSchema.fromBean(Customer.class));
            Key key = Key.builder()
                .partitionValue("id120")
                .build();

            // Get the item by using the key.
            Customer result = table.getItem(r -> r.key(key));
            System.out.println("******* The description value is " + result.getCustName());

        } catch (DynamoDbException e) {
            log.error("Failed while getting item.", e);
        }
    }

    public static void putBatchRecords(DynamoDbEnhancedClient enhancedClient) {

        try {
            DynamoDbTable<Customer> mappedTable = enhancedClient.table("bryce_test", TableSchema.fromBean(Customer.class));
            LocalDate localDate = LocalDate.parse("2020-04-07");
            LocalDateTime localDateTime = localDate.atStartOfDay();
            Instant instant = localDateTime.toInstant(ZoneOffset.UTC);

            Customer record2 = new Customer();
            record2.setCustName("Fred Pink");
            record2.setId("id110");
            record2.setEmail("fredp@noserver.com");
            record2.setRegistrationDate(instant) ;

            Customer record3 = new Customer();
            record3.setCustName("Susan Pink");
            record3.setId("id120");
            record3.setEmail("spink@noserver.com");
            record3.setRegistrationDate(instant) ;

            BatchWriteItemEnhancedRequest batchWriteItemEnhancedRequest =
                BatchWriteItemEnhancedRequest.builder()
                    .writeBatches(
                        WriteBatch.builder(Customer.class)
                            .mappedTableResource(mappedTable)
                            .addPutItem(r -> r.item(record2))
                            .addPutItem(r -> r.item(record3))
                            .build())
                    .build();

            // Add these two items to the table.
            enhancedClient.batchWriteItem(batchWriteItemEnhancedRequest);
            System.out.println("done");

        } catch (DynamoDbException e) {
            log.error("Failed while batch pushing items.", e);
        }
    }

}
