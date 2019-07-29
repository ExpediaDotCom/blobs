package com.expedia.blobs.stores.aws

import java.io.{ByteArrayInputStream, InputStream}
import java.util.Optional

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest, S3Object, S3ObjectInputStream}
import com.amazonaws.services.s3.transfer.internal.UploadImpl
import com.amazonaws.services.s3.transfer.{TransferManager, Upload}
import com.expedia.blobs.core.support.CompressDecompressService.CompressionType
import com.expedia.blobs.core.{BlobReadWriteException, BlobWriterImpl}
import com.expedia.www.blobs.model.Blob
import com.google.protobuf.ByteString
import org.apache.commons.io.IOUtils
import org.easymock.EasyMock
import org.easymock.EasyMock.{anyObject, replay, verify}
import org.scalatest.easymock.EasyMockSugar._
import org.scalatest.{BeforeAndAfter, FunSpec, GivenWhenThen, Matchers}

import scala.collection.JavaConverters._

object Support {
  def newBlob(): Blob = Blob.newBuilder()
    .setKey("key1")
    .setContent(ByteString.copyFrom("""{"key":"value"}""".getBytes))
    .putAllMetadata(Map[String, String]("a" -> "b", "c" -> "d").asJava)
    .build()
}

class ErrorHandlingS3BlobStore(builder: S3BlobStore.Builder) extends S3BlobStore(builder) {
  var error: Throwable = _

  override def store(b: BlobWriterImpl.BlobBuilder): Unit = {
    super.store(b, (t: Void, u: Throwable) => error = u)
  }

  def getError: Throwable = error

  def throwError(): Unit = error = new RuntimeException

  override protected def readInputStream(is: InputStream): Array[Byte] = {
    if (error != null) throw error
    super.readInputStream(new ByteArrayInputStream("""{"key":"value"}""".getBytes))
  }
}

class S3BlobStoreSpec extends FunSpec with GivenWhenThen with BeforeAndAfter with Matchers {
  describe("a class that acts as a blob store backed by s3") {
    it("should require valid bucket name, transfer manager and threadpool size") {
      Given("valid and invalid values")
      val transferManager = mock[TransferManager]
      val poolSz = 1
      val bucketName = "blobs"
      When("an instance of s3 store is created")
      Then("it should fail for invalid values")
      intercept[IllegalArgumentException] {
        new S3BlobStore.Builder(null, transferManager).withThreadPoolSize(poolSz).build()
      }
      intercept[IllegalArgumentException] {
        new S3BlobStore.Builder(bucketName, null).withThreadPoolSize(poolSz).build()
      }
      intercept[IllegalArgumentException] {
        new S3BlobStore.Builder(bucketName, transferManager).withThreadPoolSize(0).build()
      }
      And("create an instance if all arguments are valid")
      new S3BlobStore.Builder(bucketName, transferManager).withThreadPoolSize(poolSz).build() should not be null
    }
  }
  it("should create a put request and submit it to transfer manager") {
    Given("a blob and a valid s3 store")
    val transferManager = mock[TransferManager]
    val poolSz = 1
    val bucketName = "blobs"
    val store = new S3BlobStore.Builder(bucketName, transferManager).withThreadPoolSize(poolSz).disableAutoShutdown().build()
    val blob = Support.newBlob()

    val blobBuilder = mock[BlobWriterImpl.BlobBuilder]

    val result = mock[Upload]
    val capturedRequest = EasyMock.newCapture[PutObjectRequest]()
    expecting {
      transferManager.upload(EasyMock.capture(capturedRequest)).andReturn(result).times(1)
      blobBuilder.build().andReturn(blob)
    }
    replay(transferManager, blobBuilder)
    When("store is requested to store the blob")
    store.store(blobBuilder)
    Thread.sleep(5000)
    capturedRequest.getValue.getBucketName shouldEqual "blobs"
    Then("it should create a put request and send it to the transfer manager")
    verify(transferManager, blobBuilder)
  }
  it("should create a put request and with right bucket, key, metadata and content") {
    Given("a blob and a valid s3 store")
    val transferManager = mock[TransferManager]
    val poolSz = 1
    val bucketName = "blobs"
    val store = new S3BlobStore.Builder(bucketName, transferManager).withThreadPoolSize(poolSz).disableAutoShutdown().build()
    val blob = Support.newBlob()
    val blobBuilder = mock[BlobWriterImpl.BlobBuilder]
    val result = mock[Upload]
    val captured = EasyMock.newCapture[PutObjectRequest]
    expecting {
      transferManager.upload(EasyMock.capture(captured)).andReturn(result).times(1)
      blobBuilder.build().andReturn(blob)
    }
    replay(transferManager, blobBuilder)
    When("store is requested to store the blob")
    store.store(blobBuilder)
    Thread.sleep(10)
    Then("it should create a put request and send it to the transfer manager")
    verify(transferManager, blobBuilder)
    val putObjectRequest = captured.getValue
    putObjectRequest should not be null
    putObjectRequest.getMetadata.getContentLength should equal(blob.getContent.size())
    putObjectRequest.getMetadata.getUserMetadata.asScala should equal(Map[String, String]("a" -> "b", "c" -> "d", "compressionType" -> "NONE"))
    putObjectRequest.getBucketName should equal("blobs")
    putObjectRequest.getKey should equal("key1")
    IOUtils.readFully(putObjectRequest.getInputStream, blob.getContent.size()) should equal("""{"key":"value"}""".getBytes)
  }
  it("should handle errors in store method and invoke the error handler") {
    Given("a blob and a valid s3 store")
    val transferManager = mock[TransferManager]
    val poolSz = 1
    val bucketName = "blobs"
    val store = new ErrorHandlingS3BlobStore(new S3BlobStore.Builder(bucketName, transferManager)
      .disableAutoShutdown()
      .withCompressionType(CompressionType.NONE))

    val blob = Support.newBlob()
    val blobBuilder = mock[BlobWriterImpl.BlobBuilder]
    val error = new RuntimeException
    expecting {
      blobBuilder.build().andReturn(blob)
      transferManager.upload(anyObject[PutObjectRequest]).andThrow(error)
    }
    replay(transferManager, blobBuilder)
    When("store is requested to store the blob and the transfer manager fails")
    store.store(blobBuilder)
    Thread.sleep(10)
    Then("it should handle the error, wrap and send it to error handler")
    verify(transferManager, blobBuilder)
    store.getError.getCause shouldBe a[BlobReadWriteException]
  }
  it("should read a blob from s3 and return the data as expected") {
    Given("a blob store and a mock transfer manager")
    val transferManager = mock[TransferManager]
    val store = new ErrorHandlingS3BlobStore(new S3BlobStore.Builder("blobs", transferManager)
      .disableAutoShutdown()
      .withCompressionType(CompressionType.NONE))
    val s3Client = mock[AmazonS3Client]
    val s3Object = mock[S3Object]
    val objectMetadata = mock[ObjectMetadata]
    val userMetadata = Map[String, String]("content-type" -> "application/json", "blob-type" -> "request", "a" -> "b").asJava
    val s3InputStream = mock[S3ObjectInputStream]
    expecting {
      transferManager.getAmazonS3Client.andReturn(s3Client).times(1)
      s3Client.getObject("blobs", "key1").andReturn(s3Object).once()
      s3Object.getObjectMetadata.andReturn(objectMetadata).once()
      objectMetadata.getUserMetadata.andReturn(userMetadata).once()
      s3Object.getObjectContent.andReturn(s3InputStream).once()
    }
    replay(transferManager, s3Client, s3Object, objectMetadata)
    When("a blob is read from s3")
    val optionalBlob = store.read("key1")
    Then("it should read the blob by key from s3 bucket")
    optionalBlob.isPresent should be(true)
    val blob = optionalBlob.get()
    And("blob data should be as expected")
    blob.getKey should equal("key1")
    blob.getMetadataMap.asScala should equal(Map[String, String]("content-type" -> "application/json", "blob-type" -> "request", "a" -> "b"))
    blob.getContent.toByteArray should equal("""{"key":"value"}""".getBytes)
    verify(transferManager, s3Client, s3Object, objectMetadata)
  }
  it("should handle any exception at read and return empty object") {
    Given("a blob store that fails on read")
    val transferManager = mock[TransferManager]
    val store = new ErrorHandlingS3BlobStore(new S3BlobStore.Builder("blobs", transferManager)
      .disableAutoShutdown()
      .withCompressionType(CompressionType.NONE))
    store.throwError()
    val s3Client = mock[AmazonS3Client]
    val s3Object = mock[S3Object]
    val objectMetadata = mock[ObjectMetadata]
    val userMetadata = Map[String, String]("content-type" -> "application/json", "blob-type" -> "request", "a" -> "b").asJava
    val s3InputStream = mock[S3ObjectInputStream]
    expecting {
      transferManager.getAmazonS3Client.andReturn(s3Client).times(1)
      s3Client.getObject("blobs", "key1").andReturn(s3Object).once()
      s3Object.getObjectMetadata.andReturn(objectMetadata).once()
      objectMetadata.getUserMetadata.andReturn(userMetadata).once()
      s3Object.getObjectContent.andReturn(s3InputStream).once()
    }
    replay(transferManager, s3Client, s3Object, objectMetadata)
    var error: Throwable = null
    When("read is called with a key")
    store.read("key1", (v: Optional[Blob], t: Throwable) => error = t)
    Then("it should handle the error as expected")
    Thread.sleep(10)
    verify(transferManager, s3Client, s3Object, objectMetadata)
    error shouldBe a[RuntimeException]
  }
  it("should have autoShutdownHook when disableShutdown is disabled") {
    Given("disableShutdown as false")
    When("when an instance of S3 store is initialized")
    val transferManager = mock[TransferManager]
    val s3BlobStore: S3BlobStore = new S3BlobStore.Builder("blobs", transferManager).build()
    Then("it should have a shutdown hook")
    s3BlobStore.shutdownHookAdded should equal(true)
  }
  it("should have autoShutdownHook when disableShutdown is enabled") {
    Given("disable shutdown as true")
    When("when an instance of S3 store is initialized")
    val transferManager = mock[TransferManager]
    val s3BlobStore: S3BlobStore = new S3BlobStore.Builder("blobs", transferManager)
      .disableAutoShutdown()
      .build()

    Then("it should not have shutdown hook")
    s3BlobStore.shutdownHookAdded should equal(false)
  }

  it("should return correct compressionType when asked for") {
    val metadata = Map[String, String]("compressionType" -> "gzip", "content-type" -> "application/json", "blob-type" -> "request", "a" -> "b").asJava
    val s3BlobStore: S3BlobStore = new S3BlobStore.Builder("blobs", mock[TransferManager])
      .disableAutoShutdown()
      .withCompressionType(CompressionType.GZIP)
      .build()
    s3BlobStore.getCompressionType(metadata) shouldEqual CompressionType.GZIP
  }

  it("should return 'none' as compressionType when not present in metadata") {
    val metadata = Map[String, String]("content-type" -> "application/json", "blob-type" -> "request", "a" -> "b").asJava
    val s3BlobStore: S3BlobStore = new S3BlobStore.Builder("blobs", mock[TransferManager])
      .disableAutoShutdown()
      .build()
    s3BlobStore.getCompressionType(metadata) shouldEqual CompressionType.NONE
  }
}
