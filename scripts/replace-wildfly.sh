
#this script is useful to replace libraries on a wildfly master
# it assumes JBOSS_HOME pre defined
# run this from the project home

export JBOSS_HOME=$1


# fis the VERSION before running it
export VERSION="2.4.0-SNAPSHOT"
#replace jars
echo cp ./hornetq-core-client/target/hornetq-core-client-$VERSION.jar $JBOSS_HOME/modules/system/layers/base/org.apache.activemq6/main/hornetq-core-client*.jar
cp ./hornetq-core-client/target/hornetq-core-client-$VERSION.jar $JBOSS_HOME/modules/system/layers/base/org.apache.activemq6/main/hornetq-core-client*.jar
cp ./hornetq-server/target/hornetq-server-$VERSION.jar $JBOSS_HOME/modules/system/layers/base/org.apache.activemq6/main/hornetq-server*.jar
cp ./hornetq-commons/target/hornetq-commons-$VERSION.jar $JBOSS_HOME/modules/system/layers/base/org.apache.activemq6/main/hornetq-commons*.jar
cp ./hornetq-journal/target/hornetq-journal-$VERSION.jar $JBOSS_HOME/modules/system/layers/base/org.apache.activemq6/main/hornetq-journal*.jar
cp ./hornetq-jms-client/target/hornetq-jms-client-$VERSION.jar $JBOSS_HOME/modules/system/layers/base/org.apache.activemq6/main/hornetq-jms-client*.jar
cp ./hornetq-jms-server/target/hornetq-jms-server-$VERSION.jar $JBOSS_HOME/modules/system/layers/base/org.apache.activemq6/main/hornetq-jms-server*.jar
cp ./hornetq-ra/target/hornetq-ra-$VERSION.jar $JBOSS_HOME/modules/system/layers/base/org.apache.activemq6/ra/main/hornetq-ra*.jar

#update jboss-client.jar
rm -rf tmp
mkdir ./tmp
cd ./tmp
unzip $JBOSS_HOME/bin/client/jboss-client.jar
rm -rf ./org.apache.activemq6
unzip -o ../hornetq-jms-client/target/hornetq-jms-client-$VERSION.jar  -x META-INF\*
unzip -o ../hornetq-core-client/target/hornetq-core-client-$VERSION.jar -x \*META-INF\* 
unzip -o ../hornetq-commons/target/hornetq-commons-$VERSION.jar -x \*META-INF\*
zip -r jboss-client.jar *
cp jboss-client.jar $JBOSS_HOME/bin/client/jboss-client.jar
cd ..
rm -rf tmp

