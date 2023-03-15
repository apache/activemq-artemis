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

import org.apache.activemq.artemis.quorum.DistributedLockTest;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DatabaseDistributedLockTest extends DistributedLockTest {

/*  To run these tests against MSSQL
You will need the microsoft proprietary JDBC driver.  (alternatively you could probably configure and use the open source jTDS driver it will have a different URL and mvn dependency)

Download the jar for the microsoft driver that you wish to use and add it to your maven using a variation of the following:
   mvn install:install-file -DgroupId=com.microsoft -DartifactId=mssql-jdbc -Dversion=12.2.0.jre11 -Dpackaging=jar -Dfile=mssql-jdbc-12.2.0.jre11.jar -DgeneratePom=true
then adjust the pom.xml for artemis-quorum-ri to have a testing dependency on the driver you just added:
      <dependency>
         <groupId>com.microsoft</groupId>
         <artifactId>mssql-jdbc</artifactId>
         <version>12.2.0.jre11</version>
      </dependency>

 Uncomment the below "setConfig" method and comment out the default H2 one.  Adjust the username, password, and URL to fit your environment
 */
//   private static Map<String,String> setConfig(Map<String,String> config)
//   {
//       config.put("adapter-class",  MSSQLDatabaseAdapter.class.getCanonicalName());
//       config.put("user","testuser");
//       config.put("password","testuser");
//       config.put("url","jdbc:sqlserver://localhost:1433;databaseName=TestDB;trustServerCertificate=true");
//       return config;
//   }

/* To run these tests against ORACLE,
Get the latest Oracle driver from Oracle via their download process.  Then add it into mvn using a variation of the following:

  mvn install:install-file -DgroupId=com.oracle -DartifactId=ojdbc8 -Dversion=21.9.0.0.0 -Dpackaging=jar -Dfile=ojdbc8.jar -DgeneratePom=true

match those settings when you update pom.xml
  <dependency>
     <groupId>com.oracle</groupId>
     <artifactId>ojdbc8</artifactId>
     <version>21.9.0.0.0</version>
  </dependency>

  Remember that the Oracle user needs access to DBMS_LOCK, which even SYSTEM doesn't have by default.  So execute the following grant:

  GRANT EXECUTE on DBMS_LOCK to [youruser]

  Uncomment the "setConfig" method below and adjust your settings and environment
 */


//    private static Map<String,String> setConfig(Map<String,String> config)
//    {
//        config.put("adapter-class",  OracleDatabaseAdapter.class.getCanonicalName());
//        config.put("user","testuser");
//        config.put("password","testuser");
//        config.put("url","jdbc:oracle:thin:@localhost:1521/orcl");
//        return config;
//    }


/* To run these tests against POSTGRES
Just add the following dependency to pom.xml. This is the postgres driver:

<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.5.4</version>
</dependency>

Also add the following, which I have added for 64 bit hashing. see the comment in PostgresDatabaseAdapter

<dependency>
    <groupId>com.sangupta</groupId>
    <artifactId>murmur</artifactId>
    <version>1.0.0</version>
</dependency>

    Uncomment the "setConfig" method below and adjust your settings and environment
 */

//    private static Map<String,String> setConfig(Map<String,String> config)
//    {
//        config.put("adapter-class",  PostgresDatabaseAdapter.class.getCanonicalName());
//        config.put("user","testuser");
//        config.put("password","testuser");
//        config.put("url","jdbc:postgresql://localhost:5432/testdb?sslmode=disable");
//        return config;
//    }

/* To run these tests against MySQL

<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>8.0.27</version>
</dependency>
    Uncomment the "setConfig" method below and adjust your settings and environment

 */
//   private static Map<String, String> setConfig(Map<String, String> config) {
//      config.put("adapter-class", MySQLDatabaseAdapter.class.getCanonicalName());
//      config.put("user", "testuser");
//      config.put("password", "testuser");
//      config.put("url", "jdbc:mysql://localhost:3306/testdb");
//      return config;
//   }

   //H2  This configuration is default and is included so that the unit tests can be run by anyone with no fuss
   private static Map<String, String> setConfig(Map<String, String> config) {
      config.put("adapter-class", H2DatabaseAdapter.class.getCanonicalName());
      config.put("user", "sa");
      config.put("password", "sa");
      config.put("url", "jdbc:h2:mem:test");
      return config;
   }

   // we will initialize and hold an H2 database in memory database with the name "test" so that all other tests
   // share a database, as close as possible to if it was an external Oracle/MSSQL/Postgres/etc
   static Connection h2;

   @BeforeClass
   public static void h2Setup() throws SQLException {
      DatabaseConnectionProvider prov = new DefaultDatabaseConnectionProvider(setConfig(new HashMap<>()));
      h2 = prov.getConnection();
   }

   @Before
   @Override
   public void setupEnv() throws Throwable {
      super.setupEnv();
      DatabaseConnectionProvider setup = new DefaultDatabaseConnectionProvider(setConfig(new HashMap<>()));
      try (Connection c = setup.getConnection()) {
         deleteAllLocks(c);
      }
   }

   private void deleteAllLocks(Connection c) throws SQLException {

      try (Statement st = c.createStatement()) {
         runAndSwallow(st, "delete from ARTEMIS_LOCKS");
      }
   }

   private boolean verifyLockExistence(Connection c, String lockid) throws SQLException {
      try (PreparedStatement ps = c.prepareStatement("select count(*) from ARTEMIS_LOCKS where LOCKID=?")) {
         ps.setString(1, lockid);
         ResultSet rs = ps.executeQuery();
         rs.next();
         return rs.getLong(1) > 0;
      }
   }

   private void addSomeDummyOldDataToBePurged(Connection c) throws SQLException {
        /* if your database is running in a different timezone than you are running, this can get interesting here (because we are
        putting a java time stamp into the DB.  MSSQL for instance can be fun.
        */
      try (PreparedStatement insert = c.prepareStatement("insert into ARTEMIS_LOCKS(LOCKID,LONG_VALUE,LAST_ACCESS) values (?,?,?)")) {
         insert.setLong(2, 12L);
         insert.setString(1, "first");
         insert.setTimestamp(3, new Timestamp(631180861L));  // Jan 1, 1990 -- just something really old
         insert.execute();

         insert.setString(1, "second");
         insert.setTimestamp(3, new Timestamp(System.currentTimeMillis() - (4 * 60 * 60 * 1000))); // 4 hours ago
         insert.execute();

         insert.setString(1, "third");
         insert.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
         insert.execute();
      }
   }

   private void runAndSwallow(Statement st, String query) {
      try {
         st.executeUpdate(query);
      } catch (SQLException ex) {
         // let it go
      }
   }

   @Override
   protected void configureManager(Map<String, String> config) {
      setConfig(config);
   }

   @Override
   protected String managerClassName() {
      return DatabasePrimitiveManager.class.getName();
   }

   @Test
   public void reflectiveManagerCreation() throws Exception {
      DistributedPrimitiveManager.newInstanceOf(managerClassName(), setConfig(new HashMap<>()));
   }


   @Test
   public void specifyDatabaseProvider() throws Exception {
      Map<String,String> config = setConfig(new HashMap<>());
      config.put("database-connection-provider-class",TestDatabaseConnectionProvider.class.getCanonicalName());
      DistributedPrimitiveManager mgr = DistributedPrimitiveManager.newInstanceOf(managerClassName(), setConfig(config));
      mgr.start();
      mgr.stop();
   }

   @Test(expected = InvocationTargetException.class)
   public void reflectiveManagerCreationFailIfNoAdapterSpecified() throws Exception {
      DistributedPrimitiveManager.newInstanceOf(managerClassName(), Collections.emptyMap());
   }

   @Test
   public void lockMaintenanceTest() throws Exception {
      DatabaseConnectionProvider setup = new DefaultDatabaseConnectionProvider(setConfig(new HashMap<>()));
      try (Connection c = setup.getConnection()) {
         DatabasePrimitiveManager pm = new DatabasePrimitiveManager(setConfig(new HashMap<>()));
         deleteAllLocks(c);
         addSomeDummyOldDataToBePurged(c);
         assertTrue("Lock must exist before maintenance is run", verifyLockExistence(c, "first"));
         pm.start(); // technically this starts a race condition with the line below because of maintenance thread
         pm.cleanupOldLocks();  // but it doesn't matter because they both lock and do the same things
         assertFalse(verifyLockExistence(c, "first"));
         assertFalse(verifyLockExistence(c, "second"));
         assertTrue(verifyLockExistence(c, "third"));
         pm.stop();
      }
   }
}
