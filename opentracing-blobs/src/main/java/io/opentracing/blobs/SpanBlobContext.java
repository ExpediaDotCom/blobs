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

import io.opentracing.Span;
import com.expedia.blobs.core.BlobContext;
import com.expedia.blobs.core.BlobType;
import com.expedia.blobs.core.BlobWriter;
import org.apache.commons.lang3.Validate;

/**
 * Class representing a {@link BlobContext} associated with {@link BlobWriter} and uses {@link Span}
 * to save blob key produced to be used again for reading.
 */
public class SpanBlobContext implements BlobContext {

    private final Span span;
    private final String remoteServiceName;
    private final static String PARTIAL_BLOB_KEY = "-blob";
    private final String name;

    /**
     * Creates new SpanBlobContext.
     *
     * @param span open tracing span object
     * @param remoteServiceName for a specific service name, can't be null or empty
     * @param name for a specific operation name
     */
    public SpanBlobContext(Span span, String remoteServiceName, String name) {
        Validate.notNull(span, "span cannot be null in context");
        Validate.notEmpty(remoteServiceName, "remoteServiceName name cannot be null or empty in context");
        Validate.notEmpty(name, "name cannot be null or empty in context");

        this.span = span;
        this.remoteServiceName = remoteServiceName;
        this.name = name;
    }

    @Override
    public String getOperationName() {
        return this.name;
    }

    @Override
    public String getServiceName() {
        return this.remoteServiceName;
    }

    @Override
    public String getOperationId() {
        return span.context().toSpanId();
    }

    /**
     * Creates tag that associates target blob key with the span.
     *
     * @param blobKey created from {@link SpanBlobContext#makeKey(BlobType)}
     * @param blobType value of {@link BlobType}
     */
    @Override
    public void onBlobKeyCreate(String blobKey, BlobType blobType) {
        span.setTag(String.format("%s%s", blobType.getType(), PARTIAL_BLOB_KEY), blobKey);
    }
}
