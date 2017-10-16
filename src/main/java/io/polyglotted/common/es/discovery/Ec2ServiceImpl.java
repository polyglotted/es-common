package io.polyglotted.common.es.discovery;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.*;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;

import java.util.Random;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
final class Ec2ServiceImpl implements Ec2Service {

    static AmazonEC2 client(Settings settings) {
        Regions region = Regions.EU_WEST_1;
        if (REGION_SETTING.exists(settings) || CLOUD_EC2.REGION_SETTING.exists(settings)) {
            region = Regions.fromName(CLOUD_EC2.REGION_SETTING.get(settings));
        }
        return AmazonEC2Client.builder().withCredentials(buildCredentials(settings))
            .withClientConfiguration(buildConfiguration(settings)).withRegion(region).build();
    }

    private static AWSCredentialsProvider buildCredentials(Settings settings) {
        AWSCredentialsProvider credentials;
        try (SecureString key = DISCOVERY_EC2.ACCESS_KEY_SETTING.get(settings);
             SecureString secret = DISCOVERY_EC2.SECRET_KEY_SETTING.get(settings)) {
            if (key.length() == 0 && secret.length() == 0) {
                log.debug("Using either environment variables, system properties or instance profile credentials");
                credentials = new DefaultAWSCredentialsProviderChain();
            }
            else {
                log.debug("Using basic key/secret credentials");
                credentials = new AWSStaticCredentialsProvider(new BasicAWSCredentials(key.toString(), secret.toString()));
            }
        }
        return credentials;
    }

    private static ClientConfiguration buildConfiguration(Settings settings) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setResponseMetadataCacheSize(0);
        clientConfiguration.setProtocol(DISCOVERY_EC2.PROTOCOL_SETTING.get(settings));
        if (PROXY_HOST_SETTING.exists(settings) || DISCOVERY_EC2.PROXY_HOST_SETTING.exists(settings)) {
            String proxyHost = DISCOVERY_EC2.PROXY_HOST_SETTING.get(settings);
            Integer proxyPort = DISCOVERY_EC2.PROXY_PORT_SETTING.get(settings);
            try (SecureString proxyUsername = DISCOVERY_EC2.PROXY_USERNAME_SETTING.get(settings);
                 SecureString proxyPassword = DISCOVERY_EC2.PROXY_PASSWORD_SETTING.get(settings)) {
                clientConfiguration.withProxyHost(proxyHost).withProxyPort(proxyPort)
                    .withProxyUsername(proxyUsername.toString()).withProxyPassword(proxyPassword.toString());
            }
        }

        String awsSigner = CLOUD_EC2.SIGNER_SETTING.get(settings);
        if (Strings.hasText(awsSigner)) {
            log.debug("using AWS API signer [{}]", awsSigner);
            configureSigner(awsSigner, clientConfiguration);
        }

        final Random rand = Randomness.get();
        RetryPolicy retryPolicy = new RetryPolicy(RetryPolicy.RetryCondition.NO_RETRY_CONDITION,
            (originalRequest, exception, retriesAttempted) -> {
                log.warn("EC2 API request failed, retry again. Reason was:", exception);
                return 1000L * (long) (10d * Math.pow(2, retriesAttempted / 2.0d) * (1.0d + rand.nextDouble()));
            },
            10, false);
        clientConfiguration.setRetryPolicy(retryPolicy);
        clientConfiguration.setSocketTimeout((int) DISCOVERY_EC2.READ_TIMEOUT_SETTING.get(settings).millis());
        return clientConfiguration;
    }

    private static void configureSigner(String signer, ClientConfiguration configuration) {
        try {
            SignerFactory.getSignerByTypeAndService(checkNotNull(signer, "[null] signer set"), null);
        } catch (IllegalArgumentException e) {
            log.warn("wrong signer set {} {}", signer, e.getMessage());
        }
        configuration.setSignerOverride(signer);
    }
}