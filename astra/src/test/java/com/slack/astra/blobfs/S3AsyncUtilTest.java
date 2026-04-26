package com.slack.astra.blobfs;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.proto.config.AstraConfigs;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;

class S3AsyncUtilTest {

  @Test
  void createS3CrtBuilderAppliesForcePathStyleFlag() throws Exception {
    AstraConfigs.S3Config s3Config =
        AstraConfigs.S3Config.newBuilder()
            .setS3Region("us-east-1")
            .setS3Bucket("test-bucket")
            .setS3TargetThroughputGbps(10D)
            .setS3ForcePathStyle(true)
            .build();

    S3CrtAsyncClientBuilder builder = invokeCreateS3CrtBuilder(s3Config);

    assertThat(getBuilderField(builder, "forcePathStyle")).isEqualTo(Boolean.TRUE);
  }

  @Test
  void createS3CrtBuilderDefaultsForcePathStyleToFalse() throws Exception {
    AstraConfigs.S3Config s3Config =
        AstraConfigs.S3Config.newBuilder()
            .setS3Region("us-east-1")
            .setS3Bucket("test-bucket")
            .setS3TargetThroughputGbps(10D)
            .build();

    S3CrtAsyncClientBuilder builder = invokeCreateS3CrtBuilder(s3Config);

    assertThat(getBuilderField(builder, "forcePathStyle")).isEqualTo(Boolean.FALSE);
  }

  private static S3CrtAsyncClientBuilder invokeCreateS3CrtBuilder(AstraConfigs.S3Config s3Config)
      throws Exception {
    Method method =
        S3AsyncUtil.class.getDeclaredMethod(
            "createS3CrtAsyncClientBuilder",
            AstraConfigs.S3Config.class,
            software.amazon.awssdk.auth.credentials.AwsCredentialsProvider.class,
            long.class);
    method.setAccessible(true);
    return (S3CrtAsyncClientBuilder)
        method.invoke(
            null,
            s3Config,
            StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")),
            1073741824L);
  }

  private static Object getBuilderField(S3CrtAsyncClientBuilder builder, String fieldName)
      throws Exception {
    Field field = builder.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(builder);
  }
}
