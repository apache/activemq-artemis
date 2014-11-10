/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.core.message.impl;

import java.io.InputStream;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.message.BodyEncoder;
import org.hornetq.utils.TypedProperties;

/**
 * A MessageInternal
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public interface MessageInternal extends Message
{
   void decodeFromBuffer(HornetQBuffer buffer);

   int getEndOfMessagePosition();

   int getEndOfBodyPosition();

   void checkCopy();

   void bodyChanged();

   void resetCopied();

   boolean isServerMessage();

   HornetQBuffer getEncodedBuffer();

   int getHeadersAndPropertiesEncodeSize();

   HornetQBuffer getWholeBuffer();

   void encodeHeadersAndProperties(HornetQBuffer buffer);

   void decodeHeadersAndProperties(HornetQBuffer buffer);

   BodyEncoder getBodyEncoder() throws HornetQException;

   InputStream getBodyInputStream();

   void setAddressTransient(SimpleString address);

   TypedProperties getTypedProperties();
}
