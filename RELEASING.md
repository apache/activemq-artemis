# A check list of things to be done before a release. #

Things to do before issuing a new release:

* Ensure all new Configuration parameters are documented

* Use your IDE to regenerate equals/hashCode for ConfigurationImpl (this is just much safer than trying to inspect the code).

* Ensure all public API classes have a proper Javadoc.

* Update the README to include appropriate release notes.

* Bump the version numbers in example and test poms to the next release version. e.g. 6.0.0

* Build the release locally: mvn clean install -DskipLicenseCheck=false -Prelease

* Test the standalone release (this should be done on windows as well as linux):
1. Unpack the distribution zip or tar.gz
2. Start and stop the server (run.sh and stop.sh) for each configuration
3. Run the examples (follow the instructions under examples/index.html)
5. Check the manuals have been created properly
6. Check the javadocs are created correctly (including the diagrams)

* If every thing is successful.  Follow the Apache guide (http://www.apache.org/dev/publishing-maven-artifacts.html) to build and publish artifacts to Nexus and send out a release vote.

Note: There is one additional step to remove the activemq-pom-<version>-source-release.zip from the Nexus staging repository before closing the staging repository.  At the moment this artifact is uploaded automatically by the Apache release plugin.  In future versions the ActiveMQ pom will be updated to take this into account.
