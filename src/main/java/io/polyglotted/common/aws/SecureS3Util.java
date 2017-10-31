package io.polyglotted.common.aws;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3Encryption;
import com.amazonaws.services.s3.model.CryptoConfiguration;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static com.amazonaws.services.s3.AmazonS3ClientBuilder.standard;
import static com.amazonaws.services.s3.AmazonS3EncryptionClient.encryptionBuilder;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.fasterxml.jackson.databind.DeserializationFeature.*;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.polyglotted.common.util.MapperUtil.readToMap;
import static io.polyglotted.common.util.MapperUtil.reqdStr;

@SuppressWarnings({"unused", "WeakerAccess"})
public abstract class SecureS3Util {
    private static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(READ_ENUMS_USING_TO_STRING, true).configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(FAIL_ON_NULL_FOR_PRIMITIVES, true).configure(ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
        .configure(ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true).configure(ADJUST_DATES_TO_CONTEXT_TIME_ZONE, false)
        .setSerializationInclusion(NON_NULL).setSerializationInclusion(NON_EMPTY)
        .setVisibility(new VisibilityChecker.Std(NONE, NONE, NONE, ANY, ANY));
    @SuppressWarnings("unchecked")
    private static final Class<Map<String, Object>> MAP_CLASS = (Class<Map<String, Object>>) new TypeToken<Map<String, Object>>() {}.getRawType();

    public static InputStream decryptAndGetStream(AwsConfig config, Map<String, Object> file) throws IOException {
        return decryptAndGet(config, file).getObjectContent();
    }

    public static S3Object decryptAndGet(AwsConfig config, Map<String, Object> file) throws IOException {
        String bucket = reqdStr(file, "bucket"), key = reqdStr(file, "key");
        AmazonS3Encryption encryptionClient = encryptionClient(config, fetchCmkId(config, bucket, key));
        return encryptionClient.getObject(bucket, key);
    }

    private static AmazonS3Encryption encryptionClient(AwsConfig config, String cmkId) {
        return encryptionBuilder()
            .withCredentials(CredsProvider.getProvider(config))
            .withEncryptionMaterials(new KMSEncryptionMaterialsProvider(cmkId))
            .withCryptoConfiguration(new CryptoConfiguration().withAwsKmsRegion(RegionUtils.getRegion(config.region())))
            .withRegion(config.region()).build();
    }

    private static String fetchCmkId(AwsConfig config, String bucket, String key) throws IOException {
        ObjectMetadata metadata = standard()
            .withCredentials(CredsProvider.getProvider(config))
            .withRegion(config.region()).build().getObjectMetadata(bucket, key);
        String matDesc = checkNotNull(metadata.getUserMetaDataOf("x-amz-matdesc"), "missing material description");
        return reqdStr(readToMap(matDesc), "kms_cmk_id");
    }
}