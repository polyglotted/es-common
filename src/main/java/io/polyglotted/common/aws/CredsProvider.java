package io.polyglotted.common.aws;

import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.google.common.collect.ImmutableMap;

import static com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder.standard;

public enum CredsProvider {
    CONFIG {
        @Override AWSCredentialsProvider provider(AwsConfig config) { return new ConfigCredentialsProvider(config); }
    },
    ENVIRONMENT {
        @Override AWSCredentialsProvider provider(AwsConfig config) { return new EnvironmentVariableCredentialsProvider(); }
    },
    SYSTEM_PROPERTIES {
        @Override AWSCredentialsProvider provider(AwsConfig config) { return new SystemPropertiesCredentialsProvider(); }
    },
    PROFILE {
        @Override AWSCredentialsProvider provider(AwsConfig config) { return new ProfileCredentialsProvider(); }
    },
    CONTAINER {
        @Override AWSCredentialsProvider provider(AwsConfig config) { return new EC2ContainerCredentialsProviderWrapper(); }
    },
    STS_ROLE {
        @Override AWSCredentialsProvider provider(AwsConfig config) {
            return new STSAssumeRoleSessionCredentialsProvider.Builder(config.roleArn(), config.roleSessionName())
                .withStsClient(standard().build()).build();
        }
    },
    STS_ROLE_PROXY {
        @Override AWSCredentialsProvider provider(AwsConfig config) {
            return new STSAssumeRoleSessionCredentialsProvider.Builder(config.roleArn(), config.roleSessionName())
                .withStsClient(standard().withCredentials(new ConfigCredentialsProvider(config)).build()).build();
        }
    },
    STS_SESSION {
        @Override AWSCredentialsProvider provider(AwsConfig config) {
            return new STSSessionCredentialsProvider(new EnvironmentVariableCredentialsProvider());
        }
    },
    STS_SESSION_PROXY {
        @Override AWSCredentialsProvider provider(AwsConfig config) {
            return new STSSessionCredentialsProvider(new ConfigCredentialsProvider(config));
        }
    };

    private static final ImmutableMap<String, CredsProvider> INSTANCES;

    static {
        ImmutableMap.Builder<String, CredsProvider> builder = ImmutableMap.builder();
        for (CredsProvider value : values()) { builder.put(value.name(), value).put(value.name().toLowerCase(), value); }
        INSTANCES = builder.build();
    }

    abstract AWSCredentialsProvider provider(AwsConfig config);

    public static AWSCredentialsProvider getProvider(AwsConfig config) { return INSTANCES.get(config.provider()).provider(config); }
}