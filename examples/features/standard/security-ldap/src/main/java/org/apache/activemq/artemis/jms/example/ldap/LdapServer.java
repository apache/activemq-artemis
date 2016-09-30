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
package org.apache.activemq.artemis.jms.example.ldap;

import java.io.IOException;

import org.apache.directory.api.ldap.model.entry.DefaultEntry;
import org.apache.directory.api.ldap.model.ldif.LdifEntry;
import org.apache.directory.api.ldap.model.ldif.LdifReader;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.api.ldap.model.schema.SchemaManager;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.partition.impl.avl.AvlPartition;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;

/**
 * Creates and starts LDAP server(s).
 */
public class LdapServer {

   private final DirectoryService directoryService;
   private final org.apache.directory.server.ldap.LdapServer ldapServer;

   /**
    * Create a single LDAP server.
    *
    * @param ldifFile
    * @throws Exception
    */
   public LdapServer(String ldifFile) throws Exception {
      InMemoryDirectoryServiceFactory dsFactory = new InMemoryDirectoryServiceFactory();
      dsFactory.init("ds");

      directoryService = dsFactory.getDirectoryService();

      final SchemaManager schemaManager = directoryService.getSchemaManager();
      importLdif(directoryService, schemaManager, new LdifReader(ldifFile));

      ldapServer = new org.apache.directory.server.ldap.LdapServer();
      ldapServer.setTransports(new TcpTransport("127.0.0.1", 1024));
      ldapServer.setDirectoryService(directoryService);

      ldapServer.start();
   }

   /**
    * Stops LDAP server and the underlying directory service.
    *
    * @throws Exception
    */
   public void stop() throws Exception {
      ldapServer.stop();
      directoryService.shutdown();
   }

   private void importLdif(DirectoryService directoryService,
                           final SchemaManager schemaManager,
                           LdifReader ldifReader) throws Exception {
      try {
         for (LdifEntry ldifEntry : ldifReader) {
            checkPartition(ldifEntry);
            directoryService.getAdminSession().add(new DefaultEntry(schemaManager, ldifEntry.getEntry()));
         }
      } finally {
         try {
            ldifReader.close();
         } catch (IOException ioe) {
            // ignore
         }
      }
   }

   private void checkPartition(LdifEntry ldifEntry) throws Exception {
      Dn dn = ldifEntry.getDn();
      Dn parent = dn.getParent();
      try {
         directoryService.getAdminSession().exists(parent);
      } catch (Exception e) {
         System.out.println("Creating new partition for DN=" + dn + "\n");
         AvlPartition partition = new AvlPartition(directoryService.getSchemaManager());
         partition.setId(dn.getName());
         partition.setSuffixDn(dn);
         directoryService.addPartition(partition);
      }
   }
}
