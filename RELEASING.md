# A check list of things to be done before a release.

Things to do before issuing a new release:

* Ensure all new Configuration parameters are documented

* Use your IDE to regenerate equals/hashCode for ConfigurationImpl (this is just much safer than trying to inspect the code).

* Ensure all public API classes have a proper Javadoc.

* Update docs/user-manual/en/versions.md to include appropriate release notes and upgrade instructions. See previous
  entries for guidance.

* Bump the version numbers in example and test poms to the next release version. e.g. 2.0.0

* Build the release locally: mvn clean install -Prelease

* Test the standalone release (this should be done on windows as well as linux):
1. Unpack the distribution zip or tar.gz
2. Start and stop the server
3. Run the examples (follow the instructions under examples/index.html)
5. Check the manuals have been created properly
6. Check the javadocs are created correctly (including the diagrams)

If everything is successful follow these next steps to build and publish artifacts to Nexus and send out a release vote.

## Key to Sign the Release

If you don't have a key to sign the release artifacts you can generate one using this command:

```
gpg --gen-key
```

Ensure that your key is listed at https://dist.apache.org/repos/dist/release/activemq/KEYS.
If not, generate the key information, e.g.:

```
gpg --list-sigs username@apache.org > /tmp/key; gpg --armor --export username@apache.org >> /tmp/key
```

Then send the key information in `/tmp/key` to `private@activemq.apache.org` so it can be added.

Add your key id (available via `gpg --fingerprint <key>`) to the `OpenPGP Public Key Primary Fingerprint` field at 
https://id.apache.org/. Note: this is just the key id, not the whole fingerprint.

## Checking out a new empty git repository

Before starting make sure you clone a brand new git as follows as the release plugin will use the upstream for pushing the tags:

```sh
git clone git://github.com/apache/activemq-artemis.git
cd activemq-artemis
git remote add upstream https://git-wip-us.apache.org/repos/asf/activemq-artemis.git
```

If your git `user.email` and/or `user.name` are not set globally then you'll need to set these on the newly clone 
repository as they will be used during the release process to make commits to the upstream repository, e.g.:

```
git config user.email "username@apache.org"
git config user.name "FirstName LastName"
```

This should be the same `user.email` and `user.name` you use on your main repository.

## Running the release

You will have to use this following maven command to perform the release:

```sh
mvn clean release:prepare -DautoVersionSubmodules=true -Prelease
```

You could optionally set `pushChanges=false` so the version commit and tag won't be pushed upstream (you would have to do it yourself):

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

Otherwise snapshots will be created at 1.4.1 and forgotten. (Unless we ever release 1.4.1 on that example).

For more information look at the prepare plugin:

- https://maven.apache.org/maven-release/maven-release-plugin/prepare-mojo.html#pushChanges

If you set `pushChanges=false` then you will have to push the changes manually.  The first command is to push the commits
which are for changing the `&lt;version>` in the pom.xml files, and the second push is for the tag, e.g.:

```sh
git push upstream
git push upstream <version>
```


## Extra tests

Note: The Apache Release plugin does not bump the version on the `extraTests` module.  Release manager should manually
bump the version in the test/extra-tests/pom.xml to the next development version.


## Uploading to nexus

To upload it to nexus, perform this command:

```sh
mvn release:perform -Prelease
```

Note: this can take quite awhile depending on the speed for your Internet connection.


### Resuming release upload

If something happened during the release upload to nexus, you may need to eventually redo the upload.

There is a release.properties file that is generated at the root of the project during the release. In case you want to upload a previously tagged release, add this file as follows:

- release.properties
```
scm.url=scm:git:https://github.com/apache/activemq-artemis.git
scm.tag=1.4.0
```


## Removing additional files

The last step before closing the staging repository is removing the activemq-pom-&lt;version>-source-release.zip.  At 
the moment this artifact is uploaded automatically by the Apache release plugin. In future versions the ActiveMQ Artemis
pom will be updated to take this into account.

The file will be located under ./artemis-pom/&lt;version>/

Remove these files manually under Nexus (https://repository.apache.org/#stagingRepositories) while the repository is still open.

Once the file is removed close the staging repo using the "Close" button on Nexus website.


## Stage the release to the dist dev area

Use the closed staging repo contents to populate the the dist dev svn area
with the official release artifacts for voting. Use the script already present
in the repo to download the files and populate a new ${CURRENT-RELEASE} dir:

```sh
svn co https://dist.apache.org/repos/dist/dev/activemq/activemq-artemis/
cd activemq-artemis
./prepare-release.sh https://repository.apache.org/content/repositories/orgapacheactivemq-${NEXUS-REPO-ID} ${CURRENT-RELEASE}
```
Give the files a check over and commit the new dir and start a vote if all looks well. 

```bash
svn add <version>
svn commit
```

Old staged releases can be cleaned out periodically.


## Locate Release Notes

1. Go to the "Releases" page for the Artemis JIRA project: https://issues.apache.org/jira/projects/ARTEMIS?selectedItem=com.atlassian.jira.jira-projects-plugin:release-page
2. Click on the version being released.
3. Click the "Release Notes" link near the top of the page.
4. Grab the URL to put into the VOTE email.


## Send Email

Once all the artifacts are stage then send an email to `dev@activemq.apache.org`.  It should have a subject like `[VOTE] 
Apache ActiveMQ Artemis <version>`.  Here is an example for the body of the message:

```
I would like to propose an Apache ActiveMQ Artemis <version> release.

We added these new features as part of <version>:

[ARTEMIS-123] - Great new feature 1
[ARTEMIS-456] - Great new feature 2

The release notes can be found here:
https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=<releaseNotesVersionID>&projectId=12315920

Source and binary distributions can be found here:
https://dist.apache.org/repos/dist/dev/activemq/activemq-artemis/<version>/

The Maven repository is here:
https://repository.apache.org/content/repositories/orgapacheactivemq-<repoID>

In case you want to give it a try with the maven repo on examples:
http://activemq.apache.org/artemis/docs/latest/hacking-guide/validating-releases.html

The source tag:
https://git-wip-us.apache.org/repos/asf?p=activemq-artemis.git;a=tag;h=refs/tags/<version>

I will update the website after the vote has passed.


[ ] +1 approve the release as Apache Artemis 2.4.0
[ ] +0 no opinion
[ ] -1 disapprove (and reason why)

Here's my +1
```


## Voting process

Rules for the Apache voting process are stipulated [here](https://www.apache.org/foundation/voting.html).

Assuming the vote is successful send a email with a subject like `[RESULT] [VOTE] Apache ActiveMQ Artemis <version>` 
informing the list about the voting results, e.g.:

```
Results of the Apache ActiveMQ Artemis <version> release vote.

Vote passes with 2 +1 binding votes.

The following votes were received:

Binding:
+1 John Doe
+1 Bill Smith

Non Binding:
+1 Mike Williams

Thank you to everyone who contributed and took the time to review the
release candidates and vote.

I'll move forward with the getting the release out and updating the
relevant documentation.

Regards
```


## Promote artifacts to the dist release area

After a successful vote, populate the dist release area using the staged
files from the dist dev area to allow them to mirror. Note: this can only
be done by a PMC member.

```sh
svn cp -m "add files for activemq-artemis-${CURRENT-RELEASE}" https://dist.apache.org/repos/dist/dev/activemq/activemq-artemis/${CURRENT-RELEASE} https://dist.apache.org/repos/dist/release/activemq/activemq-artemis/${CURRENT-RELEASE}
```

Good mirror coverage can take up to 24 hours. Mirror status can be viewed [here](https://www.apache.org/mirrors/).


## Release the staging repo

Go to https://repository.apache.org/#stagingRepositories and click the "Release" button.


## Web site update:

Make sure you get a copy of the website at:

```sh
svn co https://svn.apache.org/repos/infra/websites/production/activemq/content/artemis/
```

Once the mirrors are up-to-date then update the following:
1. Copy release-notes-<old-version>.html to release-notes-<new-version>.html.
2. Update release-notes-<new-version>.html. Delete the existing list of bugs, features, improvements, etc. and replace it
   with the HTML from the bottom of the release notes link you sent out with your VOTE email.
3. Update past-releases.html. Copy the block of HTML dealing with the 2nd-to-last release, paste it above the original, 
   and modify the version numbers for the last release.
4. Update download.html. Modify the block of HTML dealing with the last release so that the version numbers are for
   the new release.
5. Copy docs/latest to docs/<old-version>.
6. Create docs/latest and copy these files into it:
    1. contents of user-manual from <new-version>
    2. book.pdf version of user-manual (generated with `gitbook pdf`)
    3. book.epub version of user-manual (generated with `gitbook epub`)
    4. book.mobi version of user-manual (generated with `gitbook mobi`)
    5. hacking-guide directory from <new-version>
    6. book.pdf version of hacking-guide (generated with `gitbook pdf`)        
7. Copy docs/javadocs/javadoc-latest to docs/javadocs/javadoc-<old-version>.
8. Create docs/javadocs/javadoc-latest and copy the contents of <new-release>/web/api into it.
9. Update previous-docs.html. Copy the block of HTML dealing with the 2nd-to-last release, paste it above the original, 
   and modify the version numbers for the last release.
   
Run `svn add` for all the added directories & files and then `svn commit -m "updates for <version> release"`.

Note: Generating PDFs, etc. with gitbook requires the installation of [Calibre](https://calibre-ebook.com). You can
install this manually, but it is recommended you use your platform's package management to install (e.g. `sudo apt-get install calibre`).


## Send announcement to user list

Once the website is updated then send an email with a subject like `[ANNOUNCE] ActiveMQ Artemis <version> Released` to 
`user@activemq.apache.org` & `dev@activemq.apache.org`, e.g.:

```
I'm pleased to announce the release of ActiveMQ Artemis <version>.

Downloads are now available at:
http://activemq.apache.org/artemis/download.html

For a complete list of updates:
http://activemq.apache.org/artemis/release-notes-<version>.html

I would like to highlight these improvements:

- Summary of feature 1:
<link to relevant documentation>

- Summary of feature 2:
<link to relevant documentation>

- Summary of feature 3:
<link to relevant documentation>

As usual it contains a handful of bug fixes and other improvements.

Many thanks for all the contributors to this release.
```


## Cleaning older releases out of the dist release area

Only the current releases should be mirrored. Older releases must be cleared:

```sh
svn rm -m "clean out older release" https://dist.apache.org/repos/dist/release/activemq/activemq-artemis/${OLD-RELEASE}
```
Any links to them on the site must be updated to reference the ASF archive instead at
https://archive.apache.org/dist/activemq/activemq-artemis/ (this should already have been done implicitly in the previous step).

Ensure old releases are only removed after the site is updated in order to avoid broken links.


## Apache Guide

For more information consult the apache guide at this address:

* http://www.apache.org/dev/publishing-maven-artifacts.html
