package com.expedia.haystack.blobs;

import com.expedia.blobs.core.BlobContext;
import com.expedia.blobs.core.BlobType;
import com.expedia.blobs.core.BlobWriter;
import com.expedia.www.haystack.client.Span;
import org.apache.commons.lang3.Validate;

/**
 * Class representing a {@link BlobContext} associated with {@link BlobWriter} and uses {@link Span}
 * to save blob key produced to be used again for reading
 */

public class SpanBlobContext implements BlobContext {

    private final Span span;
    private final static String PARTIAL_BLOB_KEY = "-blob";

    /**
     * constructor
     * @param span span object
     */
    public SpanBlobContext(Span span) {
        Validate.notNull(span, "span cannot be null in context");
        this.span = span;
    }

    @Override
    public String getOperationName() {
        return this.span.getOperationName();
    }

    @Override
    public String getServiceName() {
        return this.span.getServiceName();
    }

    @Override
    public String getOperationId() {
        return this.span.context().getSpanId().toString();
    }

    /**
     * This will be used to add the key produced inside the span
     * for it to be used during the time of reading the blob through the span
     * @param blobKey created from {@link SpanBlobContext#makeKey(BlobType)}
     * @param blobType value of {@link BlobType}
     */
    @Override
    public void onBlobKeyCreate(String blobKey, BlobType blobType) {
        span.setTag(String.format("%s%s", blobType.getType(), PARTIAL_BLOB_KEY), blobKey);
    }
}
