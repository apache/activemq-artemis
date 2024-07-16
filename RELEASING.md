# A check list of things to be done before a release.

Things to do before issuing a new release:

* Update docs/user-manual/en/versions.adoc to include appropriate release notes and upgrade instructions. See previous
  entries for guidance.

* Build the release locally: mvn clean install -Prelease
   1. Check the manuals have been created properly
   2. Check the javadocs are created correctly (including the diagrams)

* Test the standalone release (this should be done on windows as well as linux):
   1. Unpack the distribution zip or tar.gz
   2. Start and stop the server

*  Run the examples, from the [development branch](https://github.com/apache/activemq-artemis-examples/tree/development).

If everything is successful follow these next steps to build and publish artifacts to Nexus and send out a release vote.

## Key to Sign the Release

If you don't have a key to sign the release artifacts, follow the instruction at [apache release signing](https://infra.apache.org/release-signing.html). 

Ensure that your key is listed at https://dist.apache.org/repos/dist/release/activemq/KEYS.
If not, generate the key information, e.g.:

```
gpg --list-sigs username@apache.org > /tmp/key; gpg --armor --export username@apache.org >> /tmp/key
```

Then send the key information in `/tmp/key` to `private@activemq.apache.org` so it can be added.

Ensure that your key is listed at https://home.apache.org/keys/committer/.
If not add your key id (available via `gpg --fingerprint <key>`) to the `OpenPGP Public Key Primary Fingerprint` field at 
https://id.apache.org/. Note: this is just the key id, not the whole fingerprint.

## Checking out a new empty git repository

Before starting make sure you clone a brand new git as follows as the release plugin will use the upstream for pushing the tags:

```sh
git clone https://github.com/apache/activemq-artemis.git
cd activemq-artemis
git remote add upstream https://gitbox.apache.org/repos/asf/activemq-artemis.git
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
mvn clean release:prepare -Prelease
```

You could optionally set `pushChanges=false` so the version commit and tag won't be pushed upstream (you would have to do it yourself):

```sh
mvn clean release:prepare -DpushChanges=false -Prelease
```

When prompted make sure the new development version matches with the next expected release, rather than the offered patch release. Example:

```
[INFO] Checking dependencies and plugins for snapshots ...
What is the release version for "ActiveMQ Artemis Parent"? (artemis-pom) 2.31.0: :
What is the SCM release tag or label for "ActiveMQ Artemis Parent"? (artemis-pom) 2.31.0: :
What is the new development version for "ActiveMQ Artemis Parent"? (artemis-pom) 2.31.1-SNAPSHOT: : 2.32.0-SNAPSHOT
```

Otherwise snapshots would be created at 2.31.1-SNAPSHOT and probably left to go stale rather than get cleaned out if the next release is actually 2.32.0. (Unless we did ever release 2.31.1 in that example).

For more information look at the prepare plugin:

- https://maven.apache.org/maven-release/maven-release-plugin/prepare-mojo.html#pushChanges

If you set `pushChanges=false` then you will have to push the changes manually.  The first command is to push the commits
which are for changing the `&lt;version>` in the pom.xml files, and the second push is for the tag, e.g.:

```sh
git push upstream
git push upstream <version>
```

## Uploading to nexus

Ensure that your environment is ready to deploy to the ASF Nexus repository as described at
[Publishing Maven Artifacts](https://infra.apache.org/publishing-maven-artifacts.html)

Copy the file release.properties, that is generated at the root of the project during the release,
before starting the upload. You could need it if the upload fails.

To upload it to nexus, perform this command:

```sh
mvn release:perform -Prelease
```

Note: this can take quite a while depending on the speed for your Internet connection.
If the upload fails or is interrupted, remove the incomplete repository
using the "Drop" button on [Nexus website](https://repository.apache.org/#stagingRepositories).
Before starting the upload again, check the release.properties at the root of the project.

**_Keep the checkout used to run the release process for later, the website update scripts will reference it for documentation output._**

### Resuming release upload

If something happened during the release upload to nexus, you may need to eventually redo the upload.
Remove the incomplete repository using the "Drop" button on [Nexus website](https://repository.apache.org/#stagingRepositories).
Before starting the upload again, check the release.properties at the root of the project.

There is a release.properties file that is generated at the root of the project during the release.
In case you want to upload a previously tagged release, add this file as follows:

- release.properties
```
scm.url=scm:git:https://github.com/apache/activemq-artemis.git
scm.tag=2.31.0
```


## Closing the staging repository

Give the staging repository contents a quick inspection using the content navigation area, then proceed to close the
staging repo using the "Close" button on Nexus website, locking it from further modification and exposing its contents
at a staging URL to allow testing. Set a description such as "ActiveMQ Artemis <version> (RC1)" while closing.

## Stage the release to the dist dev area

Use the closed staging repo contents to populate the dist dev svn area
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


## Generate the git-report

Please, include the git-commit-report as part of the release process. To generate it, follow these steps:

- Check out the [activemq-website](https://gitbox.apache.org/repos/asf/activemq-website.git) repository.
- Execute the following commands:
```bash
$ cd activemq-website
$ ./scripts/release/create-artemis-git-report.sh <path.to/activemq-artemis> <previous-version> <version>
```

For example the command used for 2.31.0, which followed 2.30.0, could have been:
```bash
$ cd activemq-website
$ ./scripts/release/create-artemis-git-report.sh ../path.to/activemq-artemis 2.30.0 2.31.0
```

This will parse all the git commits between the previous and current release tag while looking at current JIRA status.

The report page should have been created in the website repo at: `src/components/artemis/download/commit-report-<version>.html`. Check it over and commit + push when satisfied.

## Cleanup JIRA

Use the git-report to do some JIRA cleanup making sure your commits and JIRA are accurate:
- Close as done all JIRA related at the commits included in the git-report
  but exclude all JIRA related to a commit reverted by a commit included in the same git-report.
  You can execute a bulk change on all JIRA related at the commits included in the git-report
  using the link `JIRAS on this Report` at the bottom of the report.
- Ensure that the version next the version being released exists checking the [ActiveMQ Artemis Releases page](https://issues.apache.org/jira/projects/ARTEMIS?selectedItem=com.atlassian.jira.jira-projects-plugin:release-page).
  If not, you need an administrator account to create it using the `Manage Versions` button at the [ActiveMQ Artemis Releases page](https://issues.apache.org/jira/projects/ARTEMIS?selectedItem=com.atlassian.jira.jira-projects-plugin:release-page)
- Move all JIRA not closed to the next release setting the `Fix Version` field.
- Regenerate the report once you cleared JIRA, to check your job.


## Locate Release Notes

1. Use the previous report to cleanup JIRA
2. Go to the "Releases" page for the Artemis JIRA project: https://issues.apache.org/jira/projects/ARTEMIS?selectedItem=com.atlassian.jira.jira-projects-plugin:release-page
3. Click on the version being released.
4. Click the "Release Notes" link near the top of the page.
5. Grab the URL to put into the VOTE email.


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

Ths git commit report is here:
https://activemq.apache.org/components/artemis/download/commit-report-<version>

Source and binary distributions can be found here:
https://dist.apache.org/repos/dist/dev/activemq/activemq-artemis/<version>/

The Maven staging repository is here:
https://repository.apache.org/content/repositories/orgapacheactivemq-<repoID>

If you would like to validate the release:
https://activemq.apache.org/components/artemis/documentation/hacking-guide/#validating-releases

It is tagged in the git repo as <version>

[ ] +1 approve this release
[ ] +0 no opinion
[ ] -1 disapprove (and reason why)

Here's my +1
```


## Voting process

Rules for the Apache voting process are stipulated [here](https://www.apache.org/foundation/voting.html).

Assuming the vote is successful send a email with a subject like `[RESULT] [VOTE] Apache ActiveMQ Artemis <version>` 
informing the list about the voting results, e.g.:

```
The vote passed with 4 votes, 3 binding and 1 non-binding.

The following votes were received:

Binding:
+1 John Doe
+1 Jane Doe
+1 Bill Smith

Non Binding:
+1 Mike Williams

Thank you to everyone who contributed and took the time to review the
release candidates and vote.

I will add the files to the dist release repo and release the maven
staging repo, updating the website once it has had time to sync to the
CDN and Maven Central.


Regards
```


## Promote artifacts to the dist release area

After a successful vote, populate the dist release area using the staged
files from the dist dev area to allow them to mirror. Note: this can only
be done by a PMC member.

Use the script already present in the repo to copy the staged files
from the dist dev area to the dist release area:

```sh
svn co https://dist.apache.org/repos/dist/dev/activemq/activemq-artemis/
cd activemq-artemis
./promote-release.sh ${CURRENT-RELEASE}
```

It takes a small period to sync with the CDN, say ~15mins. The CDN content can be viewed [here](https://dlcdn.apache.org/activemq/activemq-artemis/).


## Release the staging repo

Go to https://repository.apache.org/#stagingRepositories and click the "Release" button.

It takes a small period to sync with Maven Central, say ~30-60mins. The Central content can be viewed [here](https://repo1.maven.org/maven2/org/apache/activemq/).


## Web site update:

Wait for the CDN to sync first after updating SVN, and additionally for Maven Central to sync, before proceeding.

The CDN content can be viewed [here](https://dlcdn.apache.org/activemq/activemq-artemis/).
The Maven Central content can be viewed [here](https://repo1.maven.org/maven2/org/apache/activemq/).


Clone the activemq-website repository:

```sh
git clone https://gitbox.apache.org/repos/asf/activemq-website.git
cd activemq-website
```

**NOTE**: Some of the release scripts use [Python](https://www.python.org/), ensure you have it installed before proceeding.
Also, the [PyYAML](https://pyyaml.org/wiki/PyYAMLDocumentation) lib is used. Examples for installing that include
using `dnf install python3-pyyaml` on Fedora, or installing it using Pip by running `pip install pyyaml`.

Once the CDN and Maven Central are up-to-date then update the site as follows:

1. Run the release addition script to generate/perform most of the updates by running command of form:
```
./scripts/release/add-artemis-release.sh <path.to/activemq-artemis> <previous-version> <new-version>
```

This does the following:
- Creates the new release collection file at `src/_artemis_releases/artemis-<padded-version-string>.md`.
- Creates the new release notes file at `src/components/artemis/download/release-notes-<new-version>.md`.
- Creates the git-report if it wasnt already present (as it should be, at `src/components/artemis/download/commit-report-<new-version>.html`).
- Moves the prior latest documentation content to `src/components/artemis/documentation/<previous-version>`.
- Replaces the latest documentation at `src/components/artemis/documentation/latest` with those from the new release.
- Replaces the javadaoc at `src/components/artemis/documentation/javadocs/javadoc-latest` with those from the new release.

Example from the 2.32.0 release:
```
./scripts/release/add-artemis-release.sh ../activemq-artemis 2.31.2 2.32.0
```
2. Open the release collection file at `src/_artemis_releases/artemis-<padded-version-string>.md` and update _shortDescription_ as appropriate to the release content.
3. Update the _artemis_ list within the `src/_data/current_releases.yml` file if needed to set the new version stream as current.

Check over `git status` etc. Run `git add` for all the added directories & files and then `git commit -m "updates for <version> release"`.
Once pushed, the changes should be published automatically by the `jekyll_websites` builder of the [apache buildbot](https://ci2.apache.org/#/builders).

## Update Examples Repo

The [examples repo](https://github.com/apache/activemq-artemis-examples) should be updated to reflect the new release and development versions.

Take a fresh clone of the repo, check out the `development` branch, and run the provided script to update the branches and create a tag, then check the results and push.

```
git clone https://gitbox.apache.org/repos/asf/activemq-artemis-examples.git

cd activemq-artemis-examples
git checkout development
./scripts/release/update-branch-versions.sh <release-version> <new-main-snapshot-version>"
```

Example from the 2.32.0 release:
```
git clone https://gitbox.apache.org/repos/asf/activemq-artemis-examples.git

cd activemq-artemis-examples
git checkout development
./scripts/release/update-branch-versions.sh 2.32.0 2.33.0-SNAPSHOT"
```

Check things over and then push the `development` and `main` branches and the `<release-version>` tag (optionally use your fork, to test things out before pushing to the main examples repo or even to raise PRs).

NOTE: The `main` branch CI build does not build Artemis, so the release must be available on Maven Central before pushing main or the build will fail. The `development` branch will check out the Artemis main branch and build against that, or it can be manually triggered and pointed to e.g a release tag.

```
git push origin main development <release-version>
```

## Upload Docker Images

1. If you don't have an account on https://hub.docker.com/ then create one.
2. [Install `docker`](https://docs.docker.com/engine/install/) in your environment.
3. If you don't already have it, then install the [`buildx` Docker plugin](https://github.com/docker/buildx#installing) to support multi-platform builds because `release-docker.sh` will create images for both `linux/amd64` and `linux/arm64`. This, of course, requires the base images from Eclipse Temurin to support these platforms as well (which they do).
4. Ensure you have access to push images to `apache/activemq-artemis`. If you don't have access you can request it by creating an INFRA Jira ticket (e.g. https://issues.apache.org/jira/browse/INFRA-24831).
5. Go to the `scripts` directory and run `release-docker.sh` with the proper parameters, e.g.:
   ```shell
   $ ./release-docker.sh 2.31.0 apache
   ```
   You can easily perform a test run by using your personal account, e.g.:
   ```shell
   $ ./release-docker.sh 2.31.0 myUsername
   ```

## Send announcement to user list

Once the website is updated then send an email with a subject like `[ANNOUNCE] ActiveMQ Artemis <version> Released` to 
`user@activemq.apache.org` & `dev@activemq.apache.org`, e.g.:

```
I'm pleased to announce the release of ActiveMQ Artemis <version>.

Downloads are now available at:
https://activemq.apache.org/components/artemis/download/

For a complete list of updates:
https://activemq.apache.org/components/artemis/download/release-notes-<version>

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
  
## Update Apache Release Data
  
Use https://reporter.apache.org/addrelease.html?activemq to update the release information.
  
This information will be used in the quarterly reports submitted to the Apache Board.

## Apache Guide

For more information consult the apache guide at this address:

* http://www.apache.org/dev/publishing-maven-artifacts.html

