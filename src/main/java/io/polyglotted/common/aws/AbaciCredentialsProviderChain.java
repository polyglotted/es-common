package io.polyglotted.common.aws;

import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import lombok.RequiredArgsConstructor;

@SuppressWarnings("WeakerAccess")
public class AbaciCredentialsProviderChain extends AWSCredentialsProviderChain {
    public AbaciCredentialsProviderChain(AWSCredentialsProvider provider) {
        super(provider, new STSSessionCredentialsProvider(new EnvironmentVariableCredentialsProvider()), new EnvironmentVariableCredentialsProvider(),
            new SystemPropertiesCredentialsProvider(), new ProfileCredentialsProvider(), new EC2ContainerCredentialsProviderWrapper());
    }

    @RequiredArgsConstructor
    public static class ConfigCredentialsProvider implements AWSCredentialsProvider {
        private final AwsConfig config;

        @Override public AWSCredentials getCredentials() { return new BasicAWSCredentials(config.accessKey(), config.secretKey()); }

        @Override public void refresh() { }
    }
}