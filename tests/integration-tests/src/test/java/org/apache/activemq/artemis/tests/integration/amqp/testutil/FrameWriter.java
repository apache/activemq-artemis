/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.activemq.artemis.tests.integration.amqp.testutil;

import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.engine.impl.FrameWriterBuffer;
import org.apache.qpid.proton.engine.impl.TransportImpl;

import java.nio.ByteBuffer;

/**
 * Writes Frames to an internal buffer for later processing by the transport.
 *
 * Copied from qpid-proton-j
 */
public class FrameWriter {

    static final int DEFAULT_FRAME_BUFFER_FULL_MARK = 64 * 1024;
    static final int FRAME_HEADER_SIZE = 8;

    public static final byte AMQP_FRAME_TYPE = 0;
    public static final byte SASL_FRAME_TYPE = 1;

    private final TransportImpl transport = null;
    private final EncoderImpl encoder;
    private final FrameWriterBuffer frameBuffer = new FrameWriterBuffer();

    // Configuration of this Frame Writer
    private int maxFrameSize;
    private final byte frameType;
    private int frameBufferMaxBytes = DEFAULT_FRAME_BUFFER_FULL_MARK;

    // State of current write operation, reset on start of each new write
    private int frameStart;

    // Frame Writer metrics
    private long framesOutput;

    public FrameWriter(EncoderImpl encoder, int maxFrameSize, byte frameType) {
        this.encoder = encoder;
        this.maxFrameSize = maxFrameSize;
        this.frameType = frameType;

        encoder.setByteBuffer(frameBuffer);
    }

    boolean isFull() {
        return frameBuffer.position() > frameBufferMaxBytes;
    }

    public int readBytes(ByteBuffer dst) {
        return frameBuffer.transferTo(dst);
    }

    long getFramesOutput() {
        return framesOutput;
    }

    void setMaxFrameSize(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    void setFrameWriterMaxBytes(int maxBytes) {
        this.frameBufferMaxBytes = maxBytes;
    }

    int getFrameWriterMaxBytes() {
        return frameBufferMaxBytes;
    }

    public void writeHeader(byte[] header) {
        frameBuffer.put(header, 0, header.length);
    }

    public void writeFrame(Object frameBody) {
        writeFrame(0, frameBody, null, null);
    }

    void writeFrame(int channel, Object frameBody, ReadableBuffer payload, Runnable onPayloadTooLarge) {
        frameStart = frameBuffer.position();

        final int performativeSize = writePerformative(frameBody, payload, onPayloadTooLarge);
        final int capacity = maxFrameSize > 0 ? maxFrameSize - performativeSize : Integer.MAX_VALUE;
        final int payloadSize = Math.min(payload == null ? 0 : payload.remaining(), capacity);

//        if (transport.isFrameTracingEnabled()) {
//            logFrame(channel, frameBody, payload, payloadSize);
//        }

        if (payloadSize > 0) {
            int oldLimit = payload.limit();
            payload.limit(payload.position() + payloadSize);
            frameBuffer.put(payload);
            payload.limit(oldLimit);
        }

        endFrame(channel);

        framesOutput++;
    }

    private int writePerformative(Object frameBody, ReadableBuffer payload, Runnable onPayloadTooLarge) {
        frameBuffer.position(frameStart + FRAME_HEADER_SIZE);

        if (frameBody != null) {
            encoder.writeObject(frameBody);
        }

        int performativeSize = frameBuffer.position() - frameStart;

        if (onPayloadTooLarge != null && maxFrameSize > 0 && payload != null && (payload.remaining() + performativeSize) > maxFrameSize) {
            // Next iteration will re-encode the frame body again with updates from the <payload-to-large>
            // handler and then we can move onto the body portion.
            onPayloadTooLarge.run();
            performativeSize = writePerformative(frameBody, payload, null);
        }

        return performativeSize;
    }

    private void endFrame(int channel) {
        int frameSize = frameBuffer.position() - frameStart;
        int originalPosition = frameBuffer.position();

        frameBuffer.position(frameStart);
        frameBuffer.putInt(frameSize);
        frameBuffer.put((byte) 2);
        frameBuffer.put(frameType);
        frameBuffer.putShort((short) channel);
        frameBuffer.position(originalPosition);
    }
}
