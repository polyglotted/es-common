package io.polyglotted.common.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ConfigCredentialsProvider implements AWSCredentialsProvider {
    private final AwsConfig config;

    @Override public AWSCredentials getCredentials() { return new BasicAWSCredentials(config.accessKey(), config.secretKey()); }

    @Override public void refresh() { }
}