package org.apache.activemq.artemis.core.protocol.mqtt.exceptions;

import org.apache.activemq.artemis.core.protocol.mqtt.MQTTReasonCodes;

public class ClientIdValidateException extends RuntimeException{
    private byte code = MQTTReasonCodes.CLIENT_IDENTIFIER_NOT_VALID;

    public ClientIdValidateException(String message) {
        super(message);
    }

    public ClientIdValidateException(byte code,String message) {
        super(message);
        this.code = code;
    }
    public byte getCode() {
        return code;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[code=" + code + "]";
    }
}
