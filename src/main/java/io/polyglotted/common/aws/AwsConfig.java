package io.polyglotted.common.aws;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import io.polyglotted.applauncher.settings.Attribute;
import io.polyglotted.applauncher.settings.Settings;
import io.polyglotted.common.aws.AbaciCredentialsProviderChain.ConfigCredentialsProvider;

@Settings
@SuppressWarnings("unused")
public interface AwsConfig {
    int MAX_MESSAGE_SIZE = 262144; //256KB

    @Attribute(name = "aws.access_key")
    default String accessKey() { return ""; }

    @Attribute(name = "aws.secret_key", encrypted = true)
    default String secretKey() { return ""; }

    @Attribute(name = "aws.region")
    default String region() { return "eu-west-1"; }

    static AmazonSQS createSqsClient(AwsConfig config) {
        return AmazonSQSClientBuilder.standard()
            .withCredentials(new AbaciCredentialsProviderChain(new ConfigCredentialsProvider(config)))
            .withRegion(Regions.fromName(config.region()))
            .build();
    }

    static AmazonSNS createSnsClient(AwsConfig config) {
        return AmazonSNSClientBuilder.standard()
            .withCredentials(new AbaciCredentialsProviderChain(new ConfigCredentialsProvider(config)))
            .withRegion(Regions.fromName(config.region()))
            .build();
    }
}