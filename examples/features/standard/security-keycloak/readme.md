# JMS Security Keycloak Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

In this example, broker authentication is delegated to keycloak. A keycloak server is installed and configured with
a demo realm called "artemis-keycloak-demo".

_NOTE_: The keycloak admin user is admin:admin
The keycloak admin console is at: http://localhost:8080/auth/admin/master/console/#/realms/artemis-keycloak-demo

Artemis uses JAAS for authentication and authorization, when authentication is delegated to keycloak, JAAS needs a
way to query keycloak and resolve tokens or authenticate directly.

There are two keycloak clients configured in the "artemis-keycloak-demo" keycloak realm, one each for the two JAAS
configurations in login.config. Each are considered in turn:

###### console realm

The web console, using client id: "artemis-console" delegates authentication to keycloak using the openid-connect
protocol and presents a bearer token to JAAS.

The keycloak BearerTokenLoginModule in the "console" JAAS realm, converts the bearer token into the relevant Artemis
roles for consumption by the management console role based access control(RBAC) defined in management.xml. 
Note: Hawtio does a higher level role check because -Dhawtio.role=guest is configured in the artemis run script.
The user 'jdoe' has the required "guest" role configured in keycloak.

````
   org.keycloak.adapters.jaas.BearerTokenLoginModule required
   keycloak-config-file="${artemis.instance}/etc/keycloak-bearer-token.json"
````
The contents of keycloak-bearer-token.json defines the url to connect to keycloak and the relevant keycloak realm.
Of note are:
````
  "use-resource-role-mappings": true,
````
which is required because the relevant Artemis roles are intentionally scoped to the keycloak clients.

````
"principal-attribute": "preferred_username",
````
which tells keycloak the attribute to use as the name of the keycloak principal that maps the bearer token. The default
value 'sub' pulls in the system id of the user which is less human-readable and has no meaning outside keycloak.


###### activemq realm

The broker has to support clients that present plain credentials. In the activemq realm, the keycloak 
DirectAccessGrantsLoginModule validates these credentials against keycloak and populates the relevant roles. 

````
org.keycloak.adapters.jaas.DirectAccessGrantsLoginModule required
        keycloak-config-file="${artemis.instance}/etc/keycloak-direct-access.json"
        role-principal-class=org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal
    ;
````
The 'keycloak-direct-access.json' configuration for this keycloak client is different because of the nature of
the credentials. The "artemis-broker" client must authenticate with keycloak using a secret. TLS may also make
sense here.
````
  "credentials": {
    "secret": "9699685c-8a30-45cf-bf19-0d38bbac5fdc"
  }
````

Further, Artemis sessions will want to verify there is a valid Artemis UserPrincipal such that it can verify
authentication and potentially populate a message header.
The PrincipalConversionLoginModule does the necessary transformation on the first KeycloakPrincipal it encounters.
````

    org.apache.activemq.artemis.spi.core.security.jaas.PrincipalConversionLoginModule required
        principalClassList=org.keycloak.KeycloakPrincipal
````

###### Broker authentication configuration

The broker is configured to use the 'activemq' jaas domain via the 'jaas-security' domain in 
bootstrap.xml.

````
    <jaas-security domain="activemq"/>
````

The broker.xml security-settings for the Info address, it locks down consumption to users with the "amq" role while
users with the "guest" role can send messages.

````
         <!-- only amq role can consume, guest role can send  -->
         <security-setting match="Info">
            <permission roles="amq" type="createDurableQueue"/>
            <permission roles="amq" type="deleteDurableQueue"/>
            <permission roles="amq" type="createNonDurableQueue"/>
            <permission roles="amq" type="deleteNonDurableQueue"/>
            <permission roles="guest" type="send"/>
            <permission roles="amq" type="consume"/>
         </security-setting>
````


###### Web console delegate authentication configuration

The web console already uses the Artemis rolePrincipalClasses and JAAS to authenticate with the broker. When the console
delegates authentication to keycloak, the bearer token needs to be resolved to a JAAS subject such that it's roles can
be queried, this requires the use of the "console" jaas realm. 
This is achieved with system property overrides, passed via the artemis.profile JAVA_ARGS from the pom.xml 
command to create the broker using the artemis-maven-plugin.

    JAVA_ARGS=".. -Dhawtio.rolePrincipalClasses=org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal 
     -Dhawtio.keycloakEnabled=true -Dhawtio.keycloakClientConfig=keycloak-js-client.json 
     -Dhawtio.authenticationEnabled=true -Dhawtio.realm=console"

Note the 'hawtio.realm=console' and the 'hawtio.keycloakClientConfig' in 'keycloak-js-client.json' which provides the keycloak
url, keycloak realm and client-id.

The keycloak login modules need access to keycloak jars and dependencies. These are copied into the lib directory of
the artemis instance in this example as part of broker creation via the pom.xml see: libListWithDeps
````
  +- org.keycloak:keycloak-adapter-core:jar
  +- org.keycloak:keycloak-core:jar
  |  +- org.keycloak:keycloak-common:jar
  |  |  \- com.sun.activation:jakarta.activation:jar
  |  +- org.bouncycastle:bcprov-jdk15on:jar
  |  +- org.bouncycastle:bcpkix-jdk15on:jar
  |  +- com.fasterxml.jackson.core:jackson-core:jar
  |  \- com.fasterxml.jackson.core:jackson-databind:jar
  |     \- com.fasterxml.jackson.core:jackson-annotations:jar
````

###### Keycloak server configuration

In the keycloak realm "artemis-keycloak-demo", described in ./src/main/resources/artemis-keycloak-demo-realm.json
the user "jdoe" has the 'guest' role while the user "mdoe" has an additional 'amq' role from their relevant keycloak
client's.

The new keycloak installation is started with a system property indicating that it should import it's state from 
'artemis-keycloak-demo-realm.json'.


###### The example client

The example jms client connects as user "mdoe" and expects to consume a message from the "Info" address.
It will block till it gets a message, so we need to send a message to the Info address from the web console
(or from another client) to have the jms client exit, allowing the example to complete.

Feel free to explore the realm configuration in keycloak via the keycloak admin console, details at the start
of this file. Note the two configured clients: 'artemis-broker' and 'artemis-console'.

To send a message to the Info address from the Artemis web console:

 login on to the web console:
    http://localhost:8161/console

Note: you will be redirected to the keycloak login screen where you will authenticate your browser with keycloak.
 login in with user/password: jdoe/password

Navigate to the Info address and send a message. The first sent may fail, the user/password are taken from the
preferences panel and there is no password stored by default. Configure a password for jdoe in your preferences
and send a message to the Info address.

The client will get this message, print out the user from the message auth header and exit this example.