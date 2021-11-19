package org.apache.activemq.artemis.util;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.activemq.artemis.utils.XidCodecSupport;
import org.apache.activemq.artemis.utils.XidPayloadException;
import org.junit.Test;

import javax.transaction.xa.Xid;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class XidCodecSupportTest {

    private static final Xid VALID_XID =
            new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

    @Test
    public void testEncodeDecode() {
        final ActiveMQBuffer buffer = ActiveMQBuffers.dynamicBuffer(0);
        XidCodecSupport.encodeXid(VALID_XID, buffer);

        assertThat(buffer.readableBytes(), equalTo(51)); // formatId(4) + branchQualLength(4) + branchQual(3) +
        // globalTxIdLength(4) + globalTx(36)

        final Xid readXid = XidCodecSupport.decodeXid(buffer);
        assertThat(readXid, equalTo(VALID_XID));
    }

    @Test(expected = XidPayloadException.class)
    public void testNegativeLength() {
        final ActiveMQBuffer buffer = ActiveMQBuffers.dynamicBuffer(0);
        XidCodecSupport.encodeXid(VALID_XID, buffer);
        // Alter branchQualifierLength to be negative
        buffer.setByte(4, (byte) 0xFF);

        XidCodecSupport.decodeXid(buffer);
    }

    @Test(expected = XidPayloadException.class)
    public void testOverflowLength() {
        final ActiveMQBuffer buffer = ActiveMQBuffers.dynamicBuffer(0);
        XidCodecSupport.encodeXid(VALID_XID, buffer);
        // Alter globalTxIdLength to be too big
        buffer.setByte(11, (byte) 0x0C);

        XidCodecSupport.decodeXid(buffer);
    }
}