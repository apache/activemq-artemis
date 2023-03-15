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
package org.apache.activemq.artemis.quorum.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

/* Users could implement one of these simple connection providers, and they could add decryption to the password
that was passed via configuration or they could use an existing connection pool, etc
* */
public class TestDatabaseConnectionProvider implements DatabaseConnectionProvider {
   private final String url;
   private final String user;
   private final String pwd;

   public TestDatabaseConnectionProvider(Map<String, String> properties) {
      url = properties.get("url");
      user = properties.get("user");
      pwd = properties.get("password");
   }

   @Override
   public Connection getConnection() throws SQLException {
      return DriverManager.getConnection(url, user, pwd);
   }
}
