package com.expedia.www.haystack.agent.blobs.dispatcher.s3

import java.io.{ByteArrayInputStream, InputStream}
import java.util.Optional

import com.amazonaws.auth.profile.internal.securitytoken.STSProfileCredentialsServiceProvider
import com.amazonaws.auth.{AWSStaticCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest, S3Object, S3ObjectInputStream}
import com.amazonaws.services.s3.transfer.model.UploadResult
import com.amazonaws.services.s3.transfer.{TransferManager, Upload}
import com.expedia.blobs.core.io.BlobInputStream
import com.expedia.blobs.core.support.CompressDecompressService
import com.expedia.www.blobs.model.Blob
import com.expedia.www.haystack.agent.blobs.dispatcher.core.RateLimitException
import com.google.protobuf.ByteString
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.easymock.EasyMock
import org.easymock.EasyMock.{replay, verify}
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{BeforeAndAfter, FunSpec, GivenWhenThen, Matchers}

import scala.collection.JavaConverters._

class ErrorHandlingS3Dispatcher(transferManager: TransferManager,
                                bucketName: String,
                                shouldWaitForUpload: Boolean,
                                maxOutstandingRequests: Int,
                                compressDecompressService: CompressDecompressService)
  extends S3Dispatcher(transferManager, bucketName, shouldWaitForUpload, maxOutstandingRequests, compressDecompressService) {

  var error: Throwable = _

  def getError: Throwable = error

  def throwError(): Unit = error = new RuntimeException

  override protected def readInputStream(is: InputStream): Array[Byte] = {
    if (error != null) throw error
    super.readInputStream(new ByteArrayInputStream("""{"key":"value"}""".getBytes))
  }
}

class S3DispatcherSpec extends FunSpec with GivenWhenThen with BeforeAndAfter with Matchers with EasyMockSugar {
  private val metadata = Map[String, String]("compressionType" -> "none", "content-type" -> "application/json", "blob-type" -> "request", "a" -> "b", "c" -> "d").asJava
  private val blobKey = "key1"
  private val blob = Blob.newBuilder()
    .setKey(blobKey)
    .setContent(ByteString.copyFromUtf8(StringUtils.repeat("request-data", 5)))
    .putAllMetadata(metadata)
    .build()

  describe("S3 Dispatcher") {

    it("should return it's name as 's3'") {
      val transferManager = mock[TransferManager]
      val dispatcher = new S3Dispatcher(transferManager, "haystack", false, 50, mock[CompressDecompressService])

      When("getName method of dispatcher is called")
      Then("It should return 's3' as name")
      dispatcher.getName shouldEqual "s3"
    }

    it("should check if bucket name is not empty while initializing the dispatcher") {
      val config = ConfigFactory.parseString(
        """
          |bucket.name = ""
          |max.outstanding.requests = 50
          |should.wait.for.upload = false
          |aws.access.key = "my-access-key"
          |aws.secret.key = "my-secret-key"
          |compression.type = "null"
        """.stripMargin)

      val dispatcher = new S3Dispatcher()

      val exception = intercept[Exception] {
        dispatcher.initialize(config)
      }

      exception.getMessage shouldEqual "s3 bucket name can't be empty"
    }

    it("should check if bucket name is present while initializing the dispatcher") {
      val config = ConfigFactory.parseString(
        """
          |max.outstanding.requests = 50
          |should.wait.for.upload = false
          |aws.access.key = "my-access-key"
          |aws.secret.key = "my-secret-key"
          |compression.type = "null"
        """.stripMargin)

      val dispatcher = new S3Dispatcher()

      val exception = intercept[Exception] {
        dispatcher.initialize(config)
      }

      exception.getMessage shouldEqual "s3 bucket name should be present"
    }

    it("should check if maxOutstandingRequests is present while initializing the dispatcher") {
      val config = ConfigFactory.parseString(
        """
          |bucket.name = "haystack"
          |should.wait.for.upload = false
          |aws.access.key = "my-access-key"
          |aws.secret.key = "my-secret-key"
          |compression.type = "null"
        """.stripMargin)

      val dispatcher = new S3Dispatcher()

      val exception = intercept[Exception] {
        dispatcher.initialize(config)
      }

      exception.getMessage shouldEqual "number of max parallel uploads should be present"
    }

    it("should check if maxOutstandingRequests is positive while initializing the dispatcher") {
      val config = ConfigFactory.parseString(
        """
          |bucket.name = "haystack"
          |max.outstanding.requests = 0
          |should.wait.for.upload = false
          |aws.access.key = "my-access-key"
          |aws.secret.key = "my-secret-key"
          |compression.type = "null"
        """.stripMargin)

      val dispatcher = new S3Dispatcher()

      val exception = intercept[Exception] {
        dispatcher.initialize(config)
      }

      exception.getMessage shouldEqual "max parallel uploads has to be greater than 0"
    }

    it("should check if regionName is present while initializing the dispatcher") {
      val config = ConfigFactory.parseString(
        """
          |bucket.name = "haystack"
          |max.outstanding.requests = 50
          |should.wait.for.upload = false
          |aws.access.key = "my-access-key"
          |aws.secret.key = "my-secret-key"
          |compression.type = "null"
        """.stripMargin)

      val dispatcher = new S3Dispatcher()

      val exception = intercept[Exception] {
        dispatcher.initialize(config)
      }

      exception.getMessage shouldEqual "s3 bucket region can't be empty"
    }

    it("should dispatch the blob") {
      When("given a dispatcher which should call dispatch for a blob")
      val transferManager = mock[TransferManager]
      val upload = mock[Upload]
      val compressDecompressService = mock[CompressDecompressService]
      val dispatcher = new S3Dispatcher(transferManager, "haystack", false, 50, compressDecompressService)

      val putRequest = EasyMock.newCapture[PutObjectRequest]

      expecting {
        compressDecompressService.getCompressionType.andReturn("none")
        compressDecompressService.compressData(EasyMock.anyObject()).andReturn(new BlobInputStream(new ByteArrayInputStream(blob.getContent.toByteArray), blob.getContent.toByteArray.length))
        transferManager.upload(EasyMock.capture(putRequest)).andReturn(upload)
      }

      Then("it should successfully dispatch that blob")
      whenExecuting(transferManager, compressDecompressService) {
        dispatcher.dispatch(blob)
        val requestObject = putRequest.getValue
        requestObject.getBucketName shouldEqual "haystack"
        requestObject.getKey shouldEqual blobKey
        requestObject.getMetadata.getUserMetadata shouldEqual metadata
        IOUtils.readLines(requestObject.getInputStream).get(0) shouldEqual StringUtils.repeat("request-data", 5)
      }
    }

    it("should call wait for upload while uploading blob if stated") {
      When("given the dispatcher with shouldWaitForUpload true is initialized")
      val transferManager = mock[TransferManager]
      val upload = mock[Upload]
      val compressDecompressService = mock[CompressDecompressService]
      val dispatcher = new S3Dispatcher(transferManager, "haystack", true, 50, compressDecompressService)

      expecting {
        compressDecompressService.getCompressionType.andReturn("none")
        compressDecompressService.compressData(EasyMock.anyObject()).andReturn(new BlobInputStream(new ByteArrayInputStream(blob.getContent.toByteArray), blob.getContent.toByteArray.length))
        transferManager.upload(EasyMock.anyObject[PutObjectRequest]).andReturn(upload)
        upload.waitForUploadResult().andReturn(mock[UploadResult]).once()
      }

      And("dispatch of dispatcher is called")
      Then("it should wait for the complete blob to get uploaded")
      whenExecuting(transferManager, upload, compressDecompressService) {
        dispatcher.dispatch(blob)
      }
    }

    it("should throw an error in dispatchInternal while uploading blob") {
      When("given the dispatcher")
      val transferManager = mock[TransferManager]
      val upload = mock[Upload]
      val compressDecompressService = mock[CompressDecompressService]
      val dispatcher = new S3Dispatcher(transferManager, "haystack", false, 50, compressDecompressService)

      When("dispatching the blobs encounter an error")
      expecting {
        compressDecompressService.getCompressionType.andReturn("none")
        compressDecompressService.compressData(EasyMock.anyObject()).andReturn(new BlobInputStream(new ByteArrayInputStream(blob.getContent.toByteArray), blob.getContent.toByteArray.length))
        transferManager.upload(EasyMock.anyObject(classOf[PutObjectRequest])).andThrow(new RuntimeException("some error"))
      }

      Then("it should intercept and show the correct message")
      whenExecuting(transferManager, upload, compressDecompressService) {
        val caught = intercept[Exception] {
          dispatcher.dispatchInternal(blob)
        }
        caught should not be null
        caught.getMessage shouldEqual "Unable to upload blob to S3 for  key key1 : some error"
      }
    }

    it("should be closed correctly") {
      When("closing the dispatcher")
      val transferManager = mock[TransferManager]
      val dispatcher = new S3Dispatcher(transferManager, "haystack", false, 50, mock[CompressDecompressService])

      expecting {
        transferManager.shutdownNow().once()
      }

      Then("it should shutdown the transfer manager")
      whenExecuting(transferManager) {
        dispatcher.close()
      }
    }

    it("should throw rate limit exceeded error") {
      When("dispatching the blobs more than the dispatch rate")
      val transferManager = mock[TransferManager]
      val dispatcher = new S3Dispatcher(transferManager, "haystack", false, 0, mock[CompressDecompressService])

      Then("it should return RateLimitException")
      val caught = intercept[RateLimitException] {
        dispatcher.dispatch(blob)
      }
      caught should not be null
      caught.getMessage should include("RateLimit is hit with outstanding(pending) requests=0")
    }

    it("should throw error while building the credential provider using unavailable STS assume-role") {
      When("given the complete configuration")
      val config = ConfigFactory.parseString(
        """
          |bucket.name = "haystack"
          |max.outstanding.requests = 50
          |should.wait.for.upload = false
          |use.sts.arn = true
        """.stripMargin)

      And("credential provider is build")
      val caught = intercept[Exception]{
        S3Dispatcher.buildCredentialProvider(config)
      }
      caught should not be null
      caught.getMessage should include("AWS STS Assume-Role should be present when enabled")
    }

    it("should build the credential provider using STS assume-role") {
      When("given the complete configuration")
      val config = ConfigFactory.parseString(
        """
          |bucket.name = "haystack"
          |max.outstanding.requests = 50
          |should.wait.for.upload = false
          |use.sts.arn = true
          |sts.arn.role = "role/tempArnRole"
        """.stripMargin)

      And("credential provider is build")
      val provider = S3Dispatcher.buildCredentialProvider(config)

      Then("it should be the instance of STSProfileCredentialsServiceProvider")
      provider.isInstanceOf[STSProfileCredentialsServiceProvider] shouldBe true
    }

    it("should build the credential provider using access and secret key") {
      When("given the complete configuration")
      val config = ConfigFactory.parseString(
        """
          |bucket.name = "haystack"
          |max.outstanding.requests = 50
          |should.wait.for.upload = false
          |aws.access.key = "my-access-key"
          |aws.secret.key = "my-secret-key"
        """.stripMargin)

      And("credential provider is build")
      val provider = S3Dispatcher.buildCredentialProvider(config)

      Then("it should have the elements according to the provided config")
      provider.isInstanceOf[AWSStaticCredentialsProvider] shouldBe true
      val creds = provider.asInstanceOf[AWSStaticCredentialsProvider].getCredentials
      creds.getAWSSecretKey shouldEqual "my-secret-key"
      creds.getAWSAccessKeyId shouldEqual "my-access-key"
    }

    it("should build the credential provider using default credential provider") {
      val config = ConfigFactory.parseString(
        """
          |bucket.name = "haystack"
          |max.outstanding.requests = 50
        """.stripMargin)

      val provider = S3Dispatcher.buildCredentialProvider(config)
      provider.isInstanceOf[DefaultAWSCredentialsProviderChain] shouldBe true
    }

    it("should read a blob from s3 and return the data as expected") {
      Given("a blob store and a mock transfer manager")
      val transferManager = mock[TransferManager]
      val s3Dispatcher = new ErrorHandlingS3Dispatcher(transferManager, "blobs", false, 50, mock[CompressDecompressService])
      val s3Client = mock[AmazonS3Client]
      val s3Object = mock[S3Object]
      val objectMetadata = mock[ObjectMetadata]
      val userMetadata = metadata
      val s3InputStream = mock[S3ObjectInputStream]
      expecting {
        transferManager.getAmazonS3Client.andReturn(s3Client).times(1)
        s3Client.getObject("blobs", blobKey).andReturn(s3Object).once()
        s3Object.getObjectMetadata.andReturn(objectMetadata).once()
        objectMetadata.getUserMetadata.andReturn(userMetadata).once()
        s3Object.getObjectContent.andReturn(s3InputStream).once()
      }
      replay(transferManager, s3Client, s3Object, objectMetadata)
      When("a blob is read from s3")
      val optionalBlob = s3Dispatcher.read(blobKey)
      Then("it should read the blob by key from s3 bucket")
      optionalBlob.isPresent should be(true)
      val blob = optionalBlob.get()
      And("blob data should be as expected")
      blob.getKey should equal("key1")
      blob.getMetadataMap should equal(metadata)
      blob.getContent.toByteArray should equal("""{"key":"value"}""".getBytes)
      verify(transferManager, s3Client, s3Object, objectMetadata)
    }

    it("should handle any exception at read and return empty object") {
      Given("a blob store that fails on read")
      val transferManager = mock[TransferManager]
      val store = new ErrorHandlingS3Dispatcher(transferManager, "blobs", false, 50, mock[CompressDecompressService])
      store.throwError()
      val s3Client = mock[AmazonS3Client]
      val s3Object = mock[S3Object]
      val objectMetadata = mock[ObjectMetadata]
      val userMetadata = Map[String, String]("content-type" -> "application/json", "blob-type" -> "request", "a" -> "b").asJava
      val s3InputStream = mock[S3ObjectInputStream]
      expecting {
        transferManager.getAmazonS3Client.andReturn(s3Client).times(1)
        s3Client.getObject("blobs", blobKey).andReturn(s3Object).once()
        s3Object.getObjectMetadata.andReturn(objectMetadata).once()
        objectMetadata.getUserMetadata.andReturn(userMetadata).once()
        s3Object.getObjectContent.andReturn(s3InputStream).once()
      }

      When("read is called with a key")
      Then("it should not return any blob")
      whenExecuting(transferManager, s3Client, s3Object, objectMetadata) {
        val currentBlob: Optional[Blob] = store.read(blobKey)
        currentBlob shouldEqual Optional.empty()
      }
    }

    it("should return correct compressionType when asked for") {
      val metadata = Map[String, String]("compressionType" -> "gzip", "content-type" -> "application/json", "blob-type" -> "request", "a" -> "b").asJava
      val s3Dispatcher = new S3Dispatcher()
      s3Dispatcher.getCompressionType(metadata) shouldEqual CompressDecompressService.CompressionType.GZIP
    }

    it("should return correct 'none' as compressionType when not present in metadata") {
      val metadata = Map[String, String]("content-type" -> "application/json", "blob-type" -> "request", "a" -> "b").asJava
      val s3Dispatcher = new S3Dispatcher()
      s3Dispatcher.getCompressionType(metadata) shouldEqual CompressDecompressService.CompressionType.NONE
    }

    it("should find the actual compression type(GZIP) from config") {
      val config = ConfigFactory.parseString(
        """
          |compression.type = "SNAPPY"
        """.stripMargin)

      val s3Dispatcher = new S3Dispatcher()
      val compressionType = s3Dispatcher.findCompressionType(config)

      compressionType shouldEqual CompressDecompressService.CompressionType.SNAPPY
    }

    it("should return 'NONE' for compression.type if not specified in config") {
      val config = ConfigFactory.parseString(
        """
        """.stripMargin)

      val s3Dispatcher = new S3Dispatcher()
      val compressionType = s3Dispatcher.findCompressionType(config)

      compressionType shouldEqual CompressDecompressService.CompressionType.NONE
    }
  }
}
