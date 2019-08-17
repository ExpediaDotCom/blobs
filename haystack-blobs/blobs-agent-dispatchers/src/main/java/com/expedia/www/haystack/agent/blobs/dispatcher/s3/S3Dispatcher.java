/*
 *  Copyright 2019 Expedia, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.expedia.www.haystack.agent.blobs.dispatcher.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.internal.securitytoken.RoleInfo;
import com.amazonaws.auth.profile.internal.securitytoken.STSProfileCredentialsServiceProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.expedia.blobs.core.BlobReadWriteException;
import com.expedia.blobs.core.io.BlobInputStream;
import com.expedia.blobs.core.support.CompressDecompressService;
import com.expedia.www.blobs.model.Blob;
import com.expedia.www.haystack.agent.blobs.dispatcher.core.BlobDispatcher;
import com.expedia.www.haystack.agent.blobs.dispatcher.core.RateLimitException;
import com.expedia.www.haystack.agent.core.metrics.SharedMetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.typesafe.config.Config;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public class S3Dispatcher implements BlobDispatcher, AutoCloseable {
    private final static Logger LOGGER = LoggerFactory.getLogger(S3Dispatcher.class);

    private final static String BUCKET_NAME_PROPERTY = "bucket.name";
    private final static String REGION_PROPERTY = "region";
    private final static String RETRY_COUNT = "retry.count";
    private final static String AWS_ACCESS_KEY = "aws.access.key";
    private final static String AWS_SECRET_KEY = "aws.secret.key";
    private final static String MAX_CONNECTIONS = "max.connections";
    private final static String KEEP_ALIVE = "keep.alive";
    private final static String AWS_SERVICE_ENDPOINT = "service.endpoint";
    private final static String AWS_PATH_STYLE_ACCESS_ENABLED = "path.style.access.enabled";
    private final static String AWS_DISABLE_CHUNKED_ENCODING = "disable.chunked.encoding";
    private final static String AWS_USE_STS_ARN = "use.sts.arn";
    private final static String AWS_STS_ARN_ROLE = "sts.arn.role";

    private final static String SHOULD_WAIT_FOR_UPLOAD = "should.wait.for.upload";

    private final static Long MULTIPART_UPLOAD_THRESHOLD = 5L * 1024 * 1024;

    private final static String MAX_OUTSTANDING_REQUESTS = "max.outstanding.requests";

    private final static String COMPRESSION_TYPE_CONFIG_KEY = "compression.type";
    private final static String COMPRESSION_TYPE_META_KEY = "compressionType";

    private TransferManager transferManager;
    private String bucketName;
    private Boolean shouldWaitForUpload;
    private int maxOutstandingRequests;
    private Semaphore parallelUploadSemaphore;
    private Executor executor;
    private CompressDecompressService compressDecompressService;
    private Meter dispatchFailureMeter;
    private Timer dispatchTimer;

    @VisibleForTesting
    S3Dispatcher(TransferManager transferManager, String bucketName, Boolean shouldWaitForUpload, int maxOutstandingRequests, CompressDecompressService compressDecompressService) {
        this.transferManager = transferManager;
        this.bucketName = bucketName;
        this.shouldWaitForUpload = shouldWaitForUpload;
        this.maxOutstandingRequests = maxOutstandingRequests;
        this.parallelUploadSemaphore = new Semaphore(maxOutstandingRequests);
        this.executor = directExecutor();
        this.dispatchFailureMeter = SharedMetricRegistry.newMeter("s3.dispatch.failure");
        this.dispatchTimer = SharedMetricRegistry.newTimer("s3.dispatch.timer");
        this.compressDecompressService = compressDecompressService;
    }

    @VisibleForTesting
    public S3Dispatcher() {
    }

    @Override
    public String getName() {
        return "s3";
    }

    @Override
    public void dispatch(Blob blob) {
        if (parallelUploadSemaphore.tryAcquire()) {
            CompletableFuture.runAsync(() -> dispatchInternal(blob), executor)
                    .whenComplete((v, t) -> {
                        if (t != null) {
                            LOGGER.error(this.getClass().getSimpleName() + " failed to store blob ", t);
                        }
                        parallelUploadSemaphore.release();
                    });
        } else {
            throw new RateLimitException("RateLimit is hit with outstanding(pending) requests=" + maxOutstandingRequests);
        }
    }

    void dispatchInternal(Blob blob) {

        try {
            final ObjectMetadata metadata = new ObjectMetadata();
            blob.getMetadataMap().forEach(metadata::addUserMetadata);

            final BlobInputStream blobInputStream = compressDecompressService.compressData(blob.getContent().toByteArray());

            metadata.setContentLength(blobInputStream.getLength());
            metadata.addUserMetadata(COMPRESSION_TYPE_META_KEY, compressDecompressService.getCompressionType());

            final PutObjectRequest putRequest =
                    new PutObjectRequest(bucketName, blob.getKey(), blobInputStream.getStream(), metadata)
                            .withCannedAcl(CannedAccessControlList.BucketOwnerFullControl)
                            .withGeneralProgressListener(new UploadProgressListener(LOGGER, blob.getKey(), dispatchFailureMeter, dispatchTimer.time()));

            final Upload upload = transferManager.upload(putRequest);

            if (shouldWaitForUpload) {
                upload.waitForUploadResult();
            }
        } catch (Exception e) {
            final String message = String.format("Unable to upload blob to S3 for  key %s : %s",
                    blob.getKey(),
                    e.getMessage());
            throw new BlobReadWriteException(message, e);
        }
    }

    @Override
    public Optional<Blob> read(String key) {
        try {
            final S3Object s3Object = transferManager.getAmazonS3Client().getObject(bucketName, key);
            final Map<String, String> objectMetadata = s3Object.getObjectMetadata().getUserMetadata();

            Map<String, String> metadata = objectMetadata == null ? Collections.emptyMap() : objectMetadata;

            final CompressDecompressService.CompressionType compressionType = getCompressionType(metadata);

            final InputStream is = s3Object.getObjectContent();

            try (final InputStream uncompressedStream = CompressDecompressService.uncompressData(compressionType, is)) {
                Blob blob = Blob.newBuilder()
                        .setKey(key)
                        .putAllMetadata(metadata)
                        .setContent(ByteString.copyFrom(readInputStream(uncompressedStream)))
                        .build();

                return Optional.of(blob);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to read the blob from name={}", getName(), e);
            return Optional.empty();
        }
    }

    protected byte[] readInputStream(InputStream is) throws IOException {
        return IOUtils.toByteArray(is);
    }

    @Override
    public void initialize(final Config s3Config) {
        Validate.isTrue(s3Config.hasPath(BUCKET_NAME_PROPERTY), "s3 bucket name should be present");
        bucketName = s3Config.getString(BUCKET_NAME_PROPERTY);
        Validate.notEmpty(bucketName, "s3 bucket name can't be empty");

        Validate.isTrue(s3Config.hasPath(MAX_OUTSTANDING_REQUESTS), "number of max parallel uploads should be present");
        maxOutstandingRequests = s3Config.getInt(MAX_OUTSTANDING_REQUESTS);
        Validate.isTrue(maxOutstandingRequests > 0, "max parallel uploads has to be greater than 0");

        shouldWaitForUpload = s3Config.hasPath(SHOULD_WAIT_FOR_UPLOAD) && s3Config.getBoolean(SHOULD_WAIT_FOR_UPLOAD);
        this.parallelUploadSemaphore = new Semaphore(maxOutstandingRequests);

        transferManager = createTransferManager(s3Config);

        executor = directExecutor();

        CompressDecompressService.CompressionType compressionType = findCompressionType(s3Config);
        compressDecompressService = new CompressDecompressService(compressionType);

        dispatchFailureMeter = SharedMetricRegistry.newMeter("s3.dispatch.failure");
        dispatchTimer = SharedMetricRegistry.newTimer("s3.dispatch.timer");

        LOGGER.info("Successfully initialized the S3 dispatcher with config={}", s3Config);
    }

    private static TransferManager createTransferManager(final Config config) {
        Validate.isTrue(config.hasPath(REGION_PROPERTY), "s3 bucket region can't be empty");
        final int maxConnections = config.hasPath(MAX_CONNECTIONS) ? config.getInt(MAX_CONNECTIONS) : 50;
        final boolean keepAlive = config.hasPath(KEEP_ALIVE) && config.getBoolean(KEEP_ALIVE);
        final int retryCount = config.hasPath(RETRY_COUNT) ? config.getInt(RETRY_COUNT) : -1;

        final ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withMaxConnections(maxConnections)
                .withTcpKeepAlive(keepAlive);

        if (retryCount > 0) {
            clientConfiguration.setMaxErrorRetry(retryCount);
        }

        final AmazonS3 s3 = getS3Client(config, clientConfiguration);

        return TransferManagerBuilder.standard().withS3Client(s3)
                .withMultipartUploadThreshold(MULTIPART_UPLOAD_THRESHOLD).build();
    }

    private static AmazonS3 getS3Client(Config config, ClientConfiguration clientConfiguration) {
        final String region = config.getString(REGION_PROPERTY);
        final boolean pathStyleAccessEnabled = config.hasPath(AWS_PATH_STYLE_ACCESS_ENABLED) ? config.getBoolean(AWS_PATH_STYLE_ACCESS_ENABLED) : false;
        final boolean disableChunkedEncoding = config.hasPath(AWS_DISABLE_CHUNKED_ENCODING) ? config.getBoolean(AWS_DISABLE_CHUNKED_ENCODING) : false;

        AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard()
                .withCredentials(buildCredentialProvider(config))
                .withClientConfiguration(clientConfiguration)
                .withPathStyleAccessEnabled(pathStyleAccessEnabled);

        if (config.hasPath(AWS_SERVICE_ENDPOINT)) {
            s3ClientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(config.getString(AWS_SERVICE_ENDPOINT), region));
        } else {
            s3ClientBuilder.withRegion(region);
        }

        if (disableChunkedEncoding) {
            s3ClientBuilder.disableChunkedEncoding();
        }

        return s3ClientBuilder.build();
    }

    @VisibleForTesting
    static AWSCredentialsProvider buildCredentialProvider(final Config config) {
        if (config.hasPath(AWS_USE_STS_ARN) && config.getBoolean(AWS_USE_STS_ARN)) {
            LOGGER.info("using STS profile credential service provider");
            Validate.isTrue(config.hasPath(AWS_STS_ARN_ROLE), "AWS STS Assume-Role should be present when enabled");

            return new STSProfileCredentialsServiceProvider(
                    new RoleInfo().withRoleArn(config.getString(AWS_STS_ARN_ROLE))
                            .withRoleSessionName("haystack-monitoring-blobs-agent"));

        } else if (config.hasPath(AWS_ACCESS_KEY) && config.hasPath(AWS_SECRET_KEY)) {
            LOGGER.info("using static aws credential provider with access and secret key for s3 dispatcher");
            return new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(config.getString(AWS_ACCESS_KEY), config.getString(AWS_SECRET_KEY)));
        } else {
            LOGGER.info("using default credential provider chain for s3 dispatcher");
            return DefaultAWSCredentialsProviderChain.getInstance();
        }
    }

    CompressDecompressService.CompressionType findCompressionType(Config config) {
        final String compressionType = config.hasPath(COMPRESSION_TYPE_CONFIG_KEY) ? config.getString(COMPRESSION_TYPE_CONFIG_KEY).toUpperCase() : "NONE";
        return CompressDecompressService.CompressionType.valueOf(compressionType);
    }

    CompressDecompressService.CompressionType getCompressionType(Map<String, String> metadata) {
        String compressionType = metadata.get(COMPRESSION_TYPE_META_KEY);
        compressionType = StringUtils.isEmpty(compressionType) ? "NONE" : compressionType;
        return CompressDecompressService.CompressionType.valueOf(compressionType.toUpperCase());
    }

    @Override
    public void close() {
        if (transferManager != null) {
            LOGGER.info("shutting down the s3 dispatcher now..");
            transferManager.shutdownNow();
        }
    }

}
