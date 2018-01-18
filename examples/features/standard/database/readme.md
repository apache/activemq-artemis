# Database example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to configure ActiveMQ Artemis to run with a database.

Notice this is not making any assumption of what is the recommended database to be used with Artemis. After all we recommend the artemis journal to be used, however in certain environments users will prefer databases for specific reasons.
