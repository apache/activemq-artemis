# JMS Security Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows how to configure and use security using ActiveMQ Artemis.

With security properly configured, ActiveMQ Artemis can restrict client access to its resources, including connection creation, message sending/receiving, etc. This is done by configuring users and roles as well as permissions in the configuration files.

ActiveMQ Artemis supports wild-card security configuration. This feature makes security configuration very flexible and enables fine-grained control over permissions in an efficient way.

For a full description of how to configure security with ActiveMQ Artemis, please consult the user manual.

This example demonstrates how to configure users/roles, how to configure topics with proper permissions using wild-card expressions, and how they take effects in a simple program.

First we need to configure users with roles. For this example, users and roles are configured in `artemis-users.properties` and `artemis-roles.properties`. The `artemis-users.properties` file follows the syntax of <user>=<password>. This example has four users configured as below

    bill=activemq
    andrew=activemq1
    frank=activemq2
    sam=activemq3

The `artemis-roles.properties` file follows the syntax of <role>=<users> where <users> can be a comma-separated list of users from `artemis-users.properties` (since more than one user can belong in a particular role). This example has four roles configured as below

    user=bill,andrew,frank,sam
    europe-user=andrew
    news-user=frank,sam
    us-user=frank

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
