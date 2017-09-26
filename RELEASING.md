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

## Checking out a new empty git repository

Before starting make sure you clone a brand new git as follows as the release plugin will use the upstream for pushing the tags:

```sh
git clone git://github.com/apache/activemq-artemis.git
cd activemq-artemis
git remote add upstream https://git-wip-us.apache.org/repos/asf/activemq-artemis.git
```

## Running the release

You will have to use this following maven command to perform the release:

```sh
mvn clean release:prepare -DautoVersionSubmodules=true -Prelease
```

You could optionally set pushChanges=false, so the commit and tag won't be pushed upstream (you would have to do it yourself):

```sh
mvn clean release:prepare -DautoVersionSubmodules=true -DpushChanges=false -Prelease
```



When prompted make sure the next is a major release. Example:

```
[INFO] Checking dependencies and plugins for snapshots ...
What is the release version for "ActiveMQ Artemis Parent"? (org.apache.activemq:artemis-pom) 1.4.0: :
What is SCM release tag or label for "ActiveMQ Artemis Parent"? (org.apache.activemq:artemis-pom) artemis-pom-1.4.0: : 1.4.0
What is the new development version for "ActiveMQ Artemis Parent"? (org.apache.activemq:artemis-pom) 1.4.1-SNAPSHOT: : 1.5.0-SNAPSHOT
```

Otherwise snapshots will be created at 1.4.1 and forgotten. (Unless we ever elease 1.4.1 on that example).

For more information look at the prepare plugin:

- http://maven.apache.org/maven-release/maven-release-plugin/prepare-mojo.html#pushChanges


## Uploading to nexus

To upload it to nexus, perform this command:

```sh
mvn release:perform -Prelease
```


### Resuming release upload

If something happened during the release upload to nexus, you may need to eventually redo the upload.

There is a release.properties file that is generated at the root of the project during the release. In case you want to upload a previously tagged release, add this file as follows:

- release.properties
```
scm.url=scm:git:https://github.com/apache/activemq-artemis.git
scm.tag=1.4.0
```


## Removing additional files

Note: There is one additional step to remove the activemq-pom-<version>-source-release.zip from the Nexus staging repository before closing the staging repository.  At the moment this artifact is uploaded automatically by the Apache release plugin.  In future versions the ActiveMQ Artemis pom will be updated to take this into account.

The file will be located under ./artemis-pom/RELEASE/

Remove these files manually under Nexus while the repository is still open.


## Stage the release to the dist dev area

Use the closed staging repo contents to populate the the dist dev svn area
with the official release artifacts for voting. Use the script already present
in the repo to download the files and populate a new ${CURRENT-RELEASE} dir:

```sh
svn co https://dist.apache.org/repos/dist/dev/activemq/activemq-artemis/
cd activemq-artemis
./prepare-release.sh https://repository.apache.org/content/repositories/orgapacheactivemq-${NEXUS-REPO-ID} ${CURRENT-RELEASE}
```
Give the files a check over and commit the new dir and start a vote if all looks well. Old staged releases can be cleaned out periodically.


## Promote artifacts to the dist release area

After a successfull vote, populate the dist release area using the staged
files from the dist dev area to allow them to mirror.

```sh
svn cp -m "add files for activemq-artemis-${CURRENT-RELEASE}" https://dist.apache.org/repos/dist/dev/activemq/activemq-artemis/${CURRENT-RELEASE} https://dist.apache.org/repos/dist/release/activemq/activemq-artemis/${CURRENT-RELEASE}
```
It can take up to 24hrs for there to be good mirror coverage. Mirror status can be viewed at https://www.apache.org/mirrors/.


## Web site update:

Make sure you get a copy of the website at:

```sh
svn co https://svn.apache.org/repos/infra/websites/production/activemq/content/artemis/
```


## Cleaning older releases out of the dist release area

Only the current releases should be mirrored. Older releases must be cleared:

```sh
svn rm -m "clean out older release" https://dist.apache.org/repos/dist/release/activemq/activemq-artemis/${OLD-RELEASE}
```
Any links to them on the site must be updated to reference the ASF archive instead at
https://archive.apache.org/dist/activemq/activemq-artemis/

Ensure old releases are only removed after the site is updated in order to avoid broken links.


## Common Pittfals

Everything is documented at the Apache guide, but these are common issues you may go through:

- Make sure someone add your GPG key to add https://dist.apache.org/repos/dist/release/activemq/KEYS
- Add your keys to id.apache.org
