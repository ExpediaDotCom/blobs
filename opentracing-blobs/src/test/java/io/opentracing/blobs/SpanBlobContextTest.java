/*
 *  Copyright 2020 Expedia, Inc.
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

package io.opentracing.blobs;

import com.expedia.blobs.core.BlobType;
import io.opentracing.Span;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

import static org.mockito.Mockito.mock;

public class SpanBlobContextTest {

    @Test
    public void verifyConstructorThrowsExceptionWithoutSpan() {
        try {
            new SpanBlobContext(null, "remote", "operation");
        } catch (Exception e) {
            Assert.assertEquals("span cannot be null in context", e.getMessage());
        }
    }

    @Test
    public void verifyConstructorThrowsExceptionWithoutServiceName() {
        try {
            new SpanBlobContext(mock(Span.class), "", "operation");
        } catch (Exception e) {
            Assert.assertEquals("remoteServiceName name cannot be null or empty in context", e.getMessage());
        }
    }

    @Test
    public void verifyConstructorThrowsExceptionWithoutOperationName() {
        try {
            new SpanBlobContext(mock(Span.class), "remote", "");
        } catch (Exception e) {
            Assert.assertEquals("name cannot be null or empty in context", e.getMessage());
        }
    }

    @Test
    public void verifyTagIsAddedOnBlobCreate() {
        MockTracer tracer = new MockTracer();
        MockSpan span = tracer.buildSpan("span-name").start();
        SpanBlobContext spanBlobContext = new SpanBlobContext(span, "remote-service", "span-name");

        spanBlobContext.onBlobKeyCreate(spanBlobContext.makeKey(BlobType.REQUEST), BlobType.REQUEST);
        Assert.assertEquals(1, span.tags().size());
        Object blobKey = span.tags().get("request-blob");
        Assert.assertEquals("remote-service_span-name_2_request", blobKey);
    }
}
