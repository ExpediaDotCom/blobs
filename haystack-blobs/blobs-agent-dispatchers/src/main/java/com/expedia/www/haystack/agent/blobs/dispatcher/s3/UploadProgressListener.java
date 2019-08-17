package com.expedia.www.haystack.agent.blobs.dispatcher.s3;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;

class UploadProgressListener implements ProgressListener {
    private final Logger logger;
    private final String key;
    private final Meter dispatchFailureMeter;
    private final Timer.Context dispatchTimer;

    UploadProgressListener(final Logger logger,
                           final String objectKey,
                           final Meter dispatchFailureMeter,
                           final Timer.Context dispatchTimer) {
        Validate.notNull(logger);
        Validate.notEmpty(objectKey);
        Validate.notNull(dispatchFailureMeter);
        Validate.notNull(dispatchTimer);

        this.logger = logger;
        this.key = objectKey;
        this.dispatchFailureMeter = dispatchFailureMeter;
        this.dispatchTimer = dispatchTimer;
    }

    @Override
    public void progressChanged(final ProgressEvent progressEvent) {
        final String msg = String.format("Progress event=%s file=%s transferred=%d", progressEvent.getEventType(), key,
                progressEvent.getBytesTransferred());
        switch (progressEvent.getEventType()) {
            case TRANSFER_CANCELED_EVENT:
            case TRANSFER_COMPLETED_EVENT:
                logger.info(msg);
                close();
                break;
            case TRANSFER_FAILED_EVENT:
                logger.error(msg);
                dispatchFailureMeter.mark();
                close();
                break;
            case TRANSFER_PART_FAILED_EVENT:
                logger.error(msg);
                close();
                break;
            default:
                logger.info(msg);
                close();
                break;
        }
    }

    private void close() {
        dispatchTimer.close();
    }
}
