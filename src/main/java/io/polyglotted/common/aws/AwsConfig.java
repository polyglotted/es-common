package io.polyglotted.common.aws;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3Encryption;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.model.CryptoConfiguration;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import io.polyglotted.common.config.Attribute;
import io.polyglotted.common.config.Settings;

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

    @Attribute(name = "aws.provider")
    default String provider() { return "environment"; }

    @Attribute(name = "aws.role_arn")
    default String roleArn() { return ""; }

    @Attribute(name = "aws.role_session_name")
    default String roleSessionName() { return "defaultSession"; }

    static AmazonS3 createS3Client(AwsConfig config) {
        return AmazonS3ClientBuilder.standard()
            .withCredentials(CredsProvider.getProvider(config))
            .withRegion(Regions.fromName(config.region()))
            .build();
    }

    static AmazonS3Encryption s3EncryptionClient(AwsConfig config, String cmkId) {
        return AmazonS3EncryptionClient.encryptionBuilder()
            .withCredentials(CredsProvider.getProvider(config))
            .withEncryptionMaterials(new KMSEncryptionMaterialsProvider(cmkId))
            .withCryptoConfiguration(new CryptoConfiguration().withAwsKmsRegion(RegionUtils.getRegion(config.region())))
            .withRegion(config.region()).build();
    }

    static AmazonSQS createSqsClient(AwsConfig config) {
        return AmazonSQSClientBuilder.standard()
            .withCredentials(CredsProvider.getProvider(config))
            .withRegion(Regions.fromName(config.region()))
            .build();
    }

    static AmazonSNS createSnsClient(AwsConfig config) {
        return AmazonSNSClientBuilder.standard()
            .withCredentials(CredsProvider.getProvider(config))
            .withRegion(Regions.fromName(config.region()))
            .build();
    }

    static AWSSimpleSystemsManagement createSsmClient(AwsConfig config) {
        return AWSSimpleSystemsManagementClientBuilder.standard()
            .withCredentials(CredsProvider.getProvider(config))
            .withRegion(Regions.fromName(config.region()))
            .build();
    }
}