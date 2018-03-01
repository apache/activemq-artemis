# Database example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to configure ActiveMQ Artemis to run with a database.

### Notice
 
This is not making any assumption of what is the recommended database to be used with Artemis. We generally recommend the Artemis journal to be used. However, in certain environments users will prefer databases for specific reasons.
