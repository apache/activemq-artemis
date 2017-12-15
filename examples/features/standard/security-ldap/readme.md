# JMS Security LDAP Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows how to configure and use security using ActiveMQ Artemis and the Apache DS LDAP server.

With security properly configured, ActiveMQ Artemis can restrict client access to its resources, including connection creation, message sending/receiving, etc. This is done by configuring users and roles as well as permissions in the configuration files.

ActiveMQ Artemis supports wild-card security configuration. This feature makes security configuration very flexible and enables fine-grained control over permissions in an efficient way.

For a full description of how to configure security with ActiveMQ Artemis, please consult the user manual.

This example demonstrates how to configure users/roles in the Apache DS LDAP server, how to configure topics with proper permissions using wild-card expressions, and how they take effects in a simple program.

Users and roles are configured in Apache DS. The SecurityExample class will start an embedded version of Apache DS and load the contents of example.ldif which contains the users and passwords for this example.

         dn: dc=activemq,dc=org
         dc: activemq
         objectClass: top
         objectClass: domain

         dn: uid=bill,dc=activemq,dc=org
         uid: bill
         userPassword: activemq
         objectClass: account
         objectClass: simpleSecurityObject
         objectClass: top

         dn: uid=andrew,dc=activemq,dc=org
         uid: andrew
         userPassword: activemq1
         objectClass: account
         objectClass: simpleSecurityObject
         objectClass: top

         dn: uid=frank,dc=activemq,dc=org
         uid: frank
         userPassword: activemq2
         objectClass: account
         objectClass: simpleSecurityObject
         objectClass: top

         dn: uid=sam,dc=activemq,dc=org
         uid: sam
         userPassword: activemq3
         objectClass: account
         objectClass: simpleSecurityObject
         objectClass: top

         ###################
         ## Define roles ##
         ###################

         dn: cn=user,dc=activemq,dc=org
         cn: user
         member: uid=bill,dc=activemq,dc=org
         member: uid=andrew,dc=activemq,dc=org
         member: uid=frank,dc=activemq,dc=org
         member: uid=sam,dc=activemq,dc=org
         objectClass: groupOfNames
         objectClass: top

         dn: cn=europe-user,dc=activemq,dc=org
         cn: europe-user
         member: uid=andrew,dc=activemq,dc=org
         objectClass: groupOfNames
         objectClass: top

         dn: cn=news-user,dc=activemq,dc=org
         cn: news-user
         member: uid=frank,dc=activemq,dc=org
         member: uid=sam,dc=activemq,dc=org
         objectClass: groupOfNames
         objectClass: top

         dn: cn=us-user,dc=activemq,dc=org
         cn: us-user
         member: uid=frank,dc=activemq,dc=org
         objectClass: groupOfNames
         objectClass: top`

User name and password consists of a valid account that can be used to establish connections to a ActiveMQ Artemis server, while roles are used in controlling the access privileges against ActiveMQ Artemis topics and queues. You can achieve this control by configuring proper permissions in `broker.xml`, like the following

<security-settings>
    <!-- any user can have full control of generic topics -->
    <security-setting match="#">
        <permission type="createDurableQueue" roles="user"/>
        <permission type="deleteDurableQueue" roles="user"/>
        <permission type="createNonDurableQueue" roles="user"/>
        <permission type="deleteNonDurableQueue" roles="user"/>
        <permission type="send" roles="user"/>
        <permission type="consume" roles="user"/>
    </security-setting>
    <security-setting match="news.europe.#">
        <permission type="createDurableQueue" roles="user"/>
        <permission type="deleteDurableQueue" roles="user"/>
        <permission type="createNonDurableQueue" roles="user"/>
        <permission type="deleteNonDurableQueue" roles="user"/>
        <permission type="send" roles="europe-user"/>
        <permission type="consume" roles="news-user"/>
    </security-setting>
    <security-setting match="news.us.#">
        <permission type="createDurableQueue" roles="user"/>
        <permission type="deleteDurableQueue" roles="user"/>
        <permission type="createNonDurableQueue" roles="user"/>
        <permission type="deleteNonDurableQueue" roles="user"/>
        <permission type="send" roles="us-user"/>
        <permission type="consume" roles="news-user"/>
    </security-setting>
</security-settings>

Permissions can be defined on any group of queues, by using a wildcard. You can easily specify wildcards to apply certain permissions to a set of matching queues and topics. In the above configuration we have created four sets of permissions, each set matches against a special group of targets, indicated by wild-card match attributes.

You can provide a very broad permission control as a default and then add more strict control over specific addresses. By the above we define the following access rules:

*   Only role `us-user` can create/delete and pulish messages to topics whose names match wild-card pattern `news.us.#`.
*   Only role `europe-user` can create/delete and publish messages to topics whose names match wild-card pattern `news.europe.#`.
*   Only role `news-user` can subscribe messages to topics whose names match wild-card pattern `news.us.#` and `news.europe.#`.
*   For any other topics that don't match any of the above wild-card patterns, permissions are granted to users of role `user`.

To illustrate the effect of permissions, three topics are deployed. Topic `genericTopic` matches `#` wild-card, topic `news.europe.europeTopic` matches `news.europe.#` wild-cards, and topic `news.us.usTopic` matches `news.us.#`.
