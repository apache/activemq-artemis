# A check list of things to be done before a release. #

Things to do before issuing a new MAJOR release:

* check if all new Configuration parameters are documented;

* use your IDE to regenerate equals/hashCode for ConfigurationImpl (this
  is just much safer than trying to inspect the code).

* check if all public API classes have a proper Javadoc.


## Tagging a release in Git ##

We must avoid having multiple commits with the same final release version in the POMs. To achieve that, the commit changing the pom versions to the final release version, should be merged together with a second commit changing to version in all pom's to ``X.Y.Z-SNAPSHOT``.

Assuming current version is ``X.Y.Z-SNAPSHOT``

0. Update the release notes.
1. Prepare a single commit changing all version tags in all pom's.
2. tag this commit locally by 'git tag -a ActiveMQ_X_Y_Z_Final -m "release for x.y.z.Final' or what ever the version is
3. remember to update the version in the main pom, think of a cool name if you can
4. update the hornetq-maven-plugin plugin in the main pom to one that is released, if needed release a new version of the plugin.
5. Either use ``git revert`` to create a new commit reverting the commit with the version changes. Or change again all versions to ``R.S.T-SNAPSHOT``.
6. push both commits with version changes together, including them in the same _pull-request_.
7. push the committed tag upstream 'git push upstream ActiveMQ_X_Y_Z_Final'
8. download and unpack the tag from github
9. firstly upload the maven artifacts to the staged repository 'mvn -Pmaven-release deploy' (you will need the repository details in your settings.xml'
10. go to nexus (https://repository.jboss.org/nexus/index.html), log in, select staging repositories, select the staging profile that you uploaded and close it.
11. build the standalone release 'mvn -Prelease package' to build the zip and tar.gz

testing the standalone release (this should be done on windows as well as linux if possible)

1. unpack the zip or tar.gz
2. start and stop the server (run.sh and stop.sh) for each configuration
3. run the jms examples (follow the instructions under examples/jms/README.md), if you have a failure (memory etc) you can use mvn -rf to restart
4. run the javaee examples (follow instructions under examples/javaee/README.md)
5. check the manuals have been created properly
6. check the javadocs are created correctly (including the diagrams)

testing the release for AS7

1. update the hornetq version in you local AS7 clone (there may be integration work you may have to include that is in the hornetq AS7 clone)
2. commit this and push to your repo
3. go to http://lightning.mw.lab.eng.bos.redhat.com/jenkins/job/as7-param-all-tests/ and run against your repo (using the staged repo).
4. go to http://lightning.mw.lab.eng.bos.redhat.com/jenkins/job/tck6-as7-jms/ and run against your repo (using the staged repo). (note this takes 5 hours)

If everything is ok then release by:

1. Upload the binaries and docs to jboss.org (instructions can be found at https://mojo.redhat.com/docs/DOC-81955)
2. go to https://www.jboss.org/author/.magnolia/pages/adminCentral.html and update the download/docs pages and add a release announcement if needed.
3. when you are happy activate the changes
4. go to nexus and release the maven artifacts from the staging repository
5. do announcements twitter/blog

if there is a problem

1. delete the tag locally 'git tag -d ActiveMQ_X_Y_Z_Final"
2. delete the tag remotely 'git push origin :refs/tags/ActiveMQ_X_Y_Z_Final"
3. go to nexus and drop the profile
4. fix what's broken and start again
