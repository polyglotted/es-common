package io.polyglotted.common.aws;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;

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

    public static InputStream fetchMayBeSecure(AwsConfig config, String bucket, String key) throws IOException {
        String cmkId = fetchCmkId(config, bucket, key);
        AmazonS3 client = (cmkId == null) ? createS3Client(config) : s3EncryptionClient(config, cmkId);
        return fetchObject(client, new GetObjectRequest(bucket, key)).getObjectContent();
    }

    private static String fetchCmkId(AwsConfig config, String bucket, String key) throws IOException {
        ObjectMetadata metadata = fetchObjectMetadata(config, bucket, key);
        String matDesc = metadata.getUserMetaDataOf("x-amz-matdesc");
        return matDesc == null ? null : reqdStr(readToMap(matDesc), "kms_cmk_id");
    }
}