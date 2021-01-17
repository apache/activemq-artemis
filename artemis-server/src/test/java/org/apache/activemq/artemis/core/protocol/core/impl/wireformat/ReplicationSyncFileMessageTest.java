/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.protocol.core.impl.wireformat;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.HashMap;

import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.RemotingConnectionImpl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnection;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnection;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.DataConstants;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.activemq.artemis.core.persistence.impl.journal.AbstractJournalStorageManager.JournalContent.MESSAGES;

public class ReplicationSyncFileMessageTest extends ActiveMQTestBase {
   @Test
   public void testNettyConnectionEncodeMessage() throws Exception {
      int dataSize = 10;
      NettyConnection conn = new NettyConnection(new HashMap<>(), new EmbeddedChannel(), null, false, false);

      SequentialFileFactory factory = new NIOSequentialFileFactory(temporaryFolder.getRoot(), 100);
      SequentialFile file = factory.createSequentialFile("file1.bin");
      file.open();
      RandomAccessFile raf = new RandomAccessFile(file.getJavaFile(), "r");
      FileChannel fileChannel = raf.getChannel();
      ReplicationSyncFileMessage replicationSyncFileMessage = new ReplicationSyncFileMessage(MESSAGES,
                                                                                             null, 10, raf, fileChannel, 0, dataSize);
      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(null, conn, 10, 10, null, null, null);
      ActiveMQBuffer buffer = replicationSyncFileMessage.encode(remotingConnection);
      Assert.assertEquals(buffer.getInt(0), replicationSyncFileMessage.expectedEncodeSize() - DataConstants.SIZE_INT);
      Assert.assertEquals(buffer.capacity(), replicationSyncFileMessage.expectedEncodeSize() - dataSize);
      file.close();
   }


   @Test
   public void testInVMConnectionEncodeMessage() throws Exception {
      int fileId = 10;
      InVMConnection conn = new InVMConnection(0, null, null, null);

      SequentialFileFactory factory = new NIOSequentialFileFactory(temporaryFolder.getRoot(), 100);
      SequentialFile file = factory.createSequentialFile("file1.bin");
      file.open();
      RandomAccessFile raf = new RandomAccessFile(file.getJavaFile(), "r");
      FileChannel fileChannel = raf.getChannel();
      ReplicationSyncFileMessage replicationSyncFileMessage = new ReplicationSyncFileMessage(MESSAGES,
                                                                                             null, fileId, raf, fileChannel, 0, 0);
      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(null, conn, 10, 10, null, null, null);
      ActiveMQBuffer buffer = replicationSyncFileMessage.encode(remotingConnection);
      Assert.assertEquals(buffer.readInt(), replicationSyncFileMessage.expectedEncodeSize() - DataConstants.SIZE_INT);
      Assert.assertEquals(buffer.capacity(), replicationSyncFileMessage.expectedEncodeSize());

      Assert.assertEquals(buffer.readByte(), PacketImpl.REPLICATION_SYNC_FILE);

      ReplicationSyncFileMessage decodedReplicationSyncFileMessage = new ReplicationSyncFileMessage();
      decodedReplicationSyncFileMessage.decode(buffer);
      Assert.assertEquals(decodedReplicationSyncFileMessage.getJournalContent(), MESSAGES);
      Assert.assertNull(decodedReplicationSyncFileMessage.getData());
      file.close();
   }
}