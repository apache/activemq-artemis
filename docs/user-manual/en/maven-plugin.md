# Maven Plugins

Since Artemis 1.1.0 Artemis provides the possibility of using Maven Plugins to manage the life cycle of servers.

## When to use it

These Maven plugins were initially created to manage server instances across our examples. They can create a server, start, and do any CLI operation over servers.

You could for example use these maven plugins on your testsuite or deployment automation.


## Goals

There are three goals that you can use

- `create`

  This will create a server accordingly to your arguments. You can do some extra tricks here such as installing extra libraries for external modules.

- `cli`

  This will perform any CLI operation. This is basically a maven expression of the CLI classes

- `runClient`

  This is a simple wrapper around classes implementing a static main call. Notice that this won't spawn a new VM or new Thread.


## Declaration

On your pom, use the plugins section:

```xml
<build>
   <plugins>
      <plugin>
         <groupId>org.apache.activemq</groupId>
         <artifactId>artemis-maven-plugin</artifactId>
      </plugin>
   </plugins>
</build>
```

## create goal

I won't detail every operation of the create plugin here, but I will try to describe the main parameters:

Name | Description
--- | ---
configuration | A place that will hold any file to replace on the configuration. For instance if you are providing your own broker.xml. Default is "${basedir}/target/classes/activemq/server0"
home | The location where you downloaded and installed artemis. Default is "${activemq.basedir}"
alternateHome | This is used case you have two possible locations for your home (e.g. one under compile and one under production
instance | Where the server is going to be installed. Default is "${basedir}/target/server0"
liblist[] | A list of libraries to be installed under ./lib. ex: "org.jgroups:jgroups:3.6.0.Final"


Example:

```xml
<execution>
   <id>create</id>
   <goals>
      <goal>create</goal>
   </goals>
   <configuration>
      <ignore>${noServer}</ignore>
   </configuration>
</execution>
```


## cli goal

Some properties for the CLI

Name | Description
--- | ---
configuration | A place that will hold any file to replace on the configuration. For instance if you are providing your own broker.xml. Default is "${basedir}/target/classes/activemq/server0"
home | The location where you downloaded and installed artemis. Default is "${activemq.basedir}"
alternateHome | This is used case you have two possible locations for your home (e.g. one under compile and one under production
instance | Where the server is going to be installed. Default is "${basedir}/target/server0"


Similarly to the create plugin, the artemis exampels are using the cli plugin. Look at them for concrete examples.

Example:
```xml
<execution>
  <id>start</id>
  <goals>
     <goal>cli</goal>
  </goals>
  <configuration>
     <spawn>true</spawn>
     <ignore>${noServer}</ignore>
     <testURI>tcp://localhost:61616</testURI>
     <args>
        <param>run</param>
     </args>
  </configuration>
</execution>
```


### runClient goal

This is a simple solution for running classes implementing the main method.

Name | Description
--- | ---
clientClass | A class implement a static void main(String arg[])
args | A string array of arguments passed to the method

Example:

```xml
<execution>
  <id>runClient</id>
  <goals>
     <goal>runClient</goal>
  </goals>
  <configuration>
     <clientClass>org.apache.activemq.artemis.jms.example.QueueExample</clientClass>
  </configuration>
</execution>
```

### Complete example


The following example is a copy of the /examples/features/standard/queue example. You may refer to it directly under the examples directory tree.

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
   <modelVersion>4.0.0</modelVersion>

   <parent>
      <groupId>org.apache.activemq.examples.broker</groupId>
      <artifactId>jms-examples</artifactId>
      <version>1.1.0</version>
   </parent>

   <artifactId>queue</artifactId>
   <packaging>jar</packaging>
   <name>ActiveMQ Artemis JMS Queue Example</name>

   <properties>
      <activemq.basedir>${project.basedir}/../../../..</activemq.basedir>
   </properties>

   <dependencies>
      <dependency>
         <groupId>org.apache.activemq</groupId>
         <artifactId>artemis-jms-client</artifactId>
         <version>${project.version}</version>
      </dependency>
   </dependencies>

   <build>
      <plugins>
         <plugin>
            <groupId>org.apache.activemq</groupId>
            <artifactId>artemis-maven-plugin</artifactId>
            <executions>
               <execution>
                  <id>create</id>
                  <goals>
                     <goal>create</goal>
                  </goals>
                  <configuration>
                     <ignore>${noServer}</ignore>
                  </configuration>
               </execution>
               <execution>
                  <id>start</id>
                  <goals>
                     <goal>cli</goal>
                  </goals>
                  <configuration>
                     <spawn>true</spawn>
                     <ignore>${noServer}</ignore>
                     <testURI>tcp://localhost:61616</testURI>
                     <args>
                        <param>run</param>
                     </args>
                  </configuration>
               </execution>
               <execution>
                  <id>runClient</id>
                  <goals>
                     <goal>runClient</goal>
                  </goals>
                  <configuration>
                     <clientClass>org.apache.activemq.artemis.jms.example.QueueExample</clientClass>
                  </configuration>
               </execution>
               <execution>
                  <id>stop</id>
                  <goals>
                     <goal>cli</goal>
                  </goals>
                  <configuration>
                     <ignore>${noServer}</ignore>
                     <args>
                        <param>stop</param>
                     </args>
                  </configuration>
               </execution>
            </executions>
            <dependencies>
               <dependency>
                  <groupId>org.apache.activemq.examples.broker</groupId>
                  <artifactId>queue</artifactId>
                  <version>${project.version}</version>
               </dependency>
            </dependencies>
         </plugin>
      </plugins>
   </build>

</project>

```