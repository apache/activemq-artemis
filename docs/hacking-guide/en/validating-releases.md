# Validating releases

## Setting up the maven repository

When a release is proposed a maven repository is staged.

This information was extracted from [Guide to Testing Staged Releases](https://maven.apache.org/guides/development/guide-testing-releases.html)

For examples, the 1.1.0 release had the Maven Repository statged as [https://repository.apache.org/content/repositories/orgapacheactivemq-1066](https://repository.apache.org/content/repositories/orgapacheactivemq-1066).

The first thing you need to do is to be able to use this release. The easiest way we have found is to change your maven settings at ``~/.m2/settings.xml``, setting up the staged repo.


*file ~/.m2/settings.xml:*

```xml
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<settings>
   <profiles>
      <profile>
         <id>apache-artemis-test</id>
         <repositories>

            <repository>
               <id>artemis-test</id>
               <name>Apache Artemis Test</name>
               <url>https://repository.apache.org/content/repositories/orgapacheactivemq-1066</url>
               <layout>default</layout>
               <releases>
                  <enabled>true</enabled>
               </releases>
               <snapshots>
                  <enabled>true</enabled>
               </snapshots>
            </repository>
         </repositories>

         <pluginRepositories>

            <pluginRepository>
               <id>artemis-test2</id>
               <name>Apache Artemis Test</name>
               <url>https://repository.apache.org/content/repositories/orgapacheactivemq-1066</url>
               <releases>
                  <enabled>true</enabled>
               </releases>
               <snapshots>
                  <enabled>true</enabled>
               </snapshots>
            </pluginRepository>
         </pluginRepositories>
      </profile>
   </profiles>

   <activeProfiles>
      <activeProfile>apache-artemis-test</activeProfile>
   </activeProfiles>
</settings>
```

After you configure this, all the maven objects will be available to your builds.


## Using the examples

The Apache ActiveMQ Artemis examples will create servers and use most of the maven components as real application were supposed to do.
You can do this by running these examples after the .m2 profile installations for the staged repository.

Of course you can use your own applications after you have staged the maven repository.
