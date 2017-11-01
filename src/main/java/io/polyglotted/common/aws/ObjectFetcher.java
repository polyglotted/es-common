package io.polyglotted.common.aws;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.polyglotted.common.aws.AwsConfig.createS3Client;
import static io.polyglotted.common.aws.AwsConfig.s3EncryptionClient;
import static io.polyglotted.common.util.MapperUtil.readToMap;
import static io.polyglotted.common.util.MapperUtil.reqdStr;

@SuppressWarnings({"unused", "WeakerAccess"})
public abstract class ObjectFetcher {
    public static ObjectMetadata fetchObjectMetadata(AwsConfig config, String bucket, String key) {
        return createS3Client(config).getObjectMetadata(bucket, key);
    }

    public static S3Object fetchObject(AmazonS3 client, GetObjectRequest request) { return client.getObject(request); }

    public static InputStream fetchPartial(AwsConfig config, String bucket, String key, long[] range) {
        GetObjectRequest request = new GetObjectRequest(bucket, key);
        if (range != null) { request.setRange(range[0], range[1]); }
        return fetchObject(createS3Client(config), request).getObjectContent();
    }

    public static InputStream fetchSecure(AwsConfig config, String bucket, String key) throws IOException {
        return fetchObject(s3EncryptionClient(config, fetchCmkId(config, bucket, key)), new GetObjectRequest(bucket, key)).getObjectContent();
    }

    private static String fetchCmkId(AwsConfig config, String bucket, String key) throws IOException {
        ObjectMetadata metadata = fetchObjectMetadata(config, bucket, key);
        String matDesc = checkNotNull(metadata.getUserMetaDataOf("x-amz-matdesc"), "missing material description");
        return reqdStr(readToMap(matDesc), "kms_cmk_id");
    }
}