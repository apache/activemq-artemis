# Broker Plugin Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows how a message plugin can be used to filter message sent to a consumer depending on that consumers roles. Credentials for a user are by default invalidated every 10 seconds so this plugin may cause excessive authentication if used without configuring the security-invalidation-interval limit appropriately. 