# A check list of things to be done before a release. #

Things to do before issuing a new release:

* Ensure all new Configuration parameters are documented

* Use your IDE to regenerate equals/hashCode for ConfigurationImpl (this is just much safer than trying to inspect the code).

* Ensure all public API classes have a proper Javadoc.

* Update the README to include appropriate release notes.

* Bump the version numbers in example and test poms to the next release version. e.g. 2.0.0

* Build the release locally: mvn clean install -Prelease

* Test the standalone release (this should be done on windows as well as linux):
1. Unpack the distribution zip or tar.gz
2. Start and stop the server (run.sh and stop.sh) for each configuration
3. Run the examples (follow the instructions under examples/index.html)
5. Check the manuals have been created properly
6. Check the javadocs are created correctly (including the diagrams)

* If every thing is successful.  Follow the Apache guide (http://www.apache.org/dev/publishing-maven-artifacts.html) to build and publish artifacts to Nexus and send out a release vote.


## Extra tests
Note: The Apache Release plugin does not bump the version on the extraTests module.  Release manager should manually bump the version in the test/extra-tests/pom.xml to the next development version.

## Removing additional files

Note: There is one additional step to remove the activemq-pom-<version>-source-release.zip from the Nexus staging repository before closing the staging repository.  At the moment this artifact is uploaded automatically by the Apache release plugin.  In future versions the ActiveMQ Artemis pom will be updated to take this into account.

The file will be located under ./artemis-pom/RELEASE/

Remove these files manually under Nexus while the repository is still open.


## Running the release

You will have to use this following maven command to perform the release:

```sh
mvn clean release:prepare -DautoVersionSubmodules=true -Prelease
```


When prompted make sure the next is a major release. Example:

```
[INFO] Checking dependencies and plugins for snapshots ...
What is the release version for "ActiveMQ Artemis Parent"? (org.apache.activemq:artemis-pom) 1.4.0: :
What is SCM release tag or label for "ActiveMQ Artemis Parent"? (org.apache.activemq:artemis-pom) artemis-pom-1.4.0: : 1.4.0
What is the new development version for "ActiveMQ Artemis Parent"? (org.apache.activemq:artemis-pom) 1.4.1-SNAPSHOT: : 1.5.0-SNAPSHOT
```

Otherwise snapshots will be created at 1.4.1 and forgotten. (Unless we ever elease 1.4.1 on that example).


### Web site update:

Make sure you get a copy of the website at:

```sh
svn co https://svn.apache.org/repos/infra/websites/production/activemq/content/artemis/
```


## Common Pittfals

Everything is documented at the Apache guide, but these are common issues you may go through:

- Make sure someone add your GPG key to add https://dist.apache.org/repos/dist/release/activemq/KEYS
- Add your keys to id.apache.org
