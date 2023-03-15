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
import java.sql.SQLException;

/**
 * If you are embedding artemis or wish to otherwise provide your own mechanism for getting database connections,
 * you can provide a class that implements this interface and specify it in a property with the key "database-connection-provider-class"
 * in the properties of the manager.
 */

/* The parameter to the constructor for this class are expected to be
    DatabaseConnectionProvider (Map<String, String> properties)
  And the property for the PrimitiveManager will be passed along.
 */
public interface DatabaseConnectionProvider {
   Connection getConnection() throws SQLException;
}
