package org.apache.activemq.artemis.utils;

/**
 * Special type of exception with stacktrace removed which is thrown when binary payload representing
 * [{@link javax.transaction.xa.Xid}] is invalid.
 */
public class XidPayloadException extends RuntimeException {

    public XidPayloadException(String message) {
        super(message, null, true, false);
    }
}