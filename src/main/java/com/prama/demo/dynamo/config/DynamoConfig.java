package com.prama.demo.dynamo.config;

import com.prama.demo.dynamo.repositories.DynamoService;
import com.prama.demo.dynamo.repositories.DynamoThreadPoolService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@Slf4j
@Configuration
public class DynamoConfig {

    @Value("${aws.region:us-east-1}") String region;

    @Bean(destroyMethod = "close")
    public DynamoDbClient dynamoDbClient() {
        return DynamoDbClient.builder()
            .credentialsProvider(ProfileCredentialsProvider.create())
            .region(Region.of(region))
            .build();
    }

    @Bean
    public DynamoDbEnhancedClient dynamoDbEnhancedClient(DynamoDbClient ddb) {
        return DynamoDbEnhancedClient.builder()
            .dynamoDbClient(ddb)
            .build();
    }

    @Bean
    public DynamoService dynamoService(DynamoDbEnhancedClient enhancedClient) {
        return new DynamoService(enhancedClient);
    }

    @Bean
    public DynamoThreadPoolService dynamoThreadPoolService(DynamoDbEnhancedClient enhancedClient) {
        return new DynamoThreadPoolService(enhancedClient);
    }

}
