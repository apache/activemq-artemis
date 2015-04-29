# ActiveMQ Artemis

This file describes some minimum 'stuff one needs to know' to get
started coding in this project.

## Source

The project's source code is hosted at:

https://git-wip-us.apache.org/repos/asf/activemq-artemis.git

### Git usage:

Pull requests should be merged without fast forwards '--no-ff'. An easy way to achieve that is to use

```% git config branch.master.mergeoptions --no-ff```

## Maven

The minimum required Maven version is 3.0.0.

Do note that there are some compatibility issues with Maven 3.X still
unsolved [1]. This is specially true for the 'site' plugin [2].

[1]: <https://cwiki.apache.org/MAVEN/maven-3x-compatibility-notes.html>
[2]: <https://cwiki.apache.org/MAVEN/maven-3x-and-site-plugin.html>


## building the distribution

If you want to build the full release with documentation, Javadocs and the full web site then run the following:

```% mvn -Prelease package```

If you want to install it to your local maven repo then run

```% mvn -Prelease install```

The full release uses gitbook to build a static website from the documentation, if you don't have gitbook installed then
 install gitbook using npm

```npm install -g gitbook gitbook-cli```

### Installing NPM

If you don't have npm installed then you would need to install  it first.

#### On Fedora

```yum install npm```

#### On Mac-OS

The easiest way would be through brew [brew]

You first install brew using the instructions on the [brew] website.

After you installed brew you can install npm by:

```brew install npm```

[brew]: <http://brew.sh>

#### Build without docs


It is possible to build a distribution with out the manuals and javadocs if you dont have or want gitbook installed, 
simply run

```% mvn package```

## Tests

To run the unit tests:

```% mvn -Ptests test```

Generating reports from unit tests:

```% mvn install site```


Running tests individually

```% mvn -Ptests -DfailIfNoTests=false -Dtest=<test-name> test ```

where &lt;test-name> is the name of the Test class without its package name


## Examples

To run an example firstly make sure you have run

```% mvn -Prelease install```

If the project version has already been released then this is unnecessary.

then you will need to set the following maven options, on Linux by

```export MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=512m"```

and the finally run the examples by

```% mvn verify```

You can also run individual examples by running the same command from the directory of which ever example you want to run.
NB for this make sure you have installed examples/common.

### Recreating the examples

If you are trying to copy the examples somewhere else and modifying them. Consider asking Maven to explicitly list all the dependencies:

```
# if trying to modify the 'topic' example:
cd examples/jms/topic && mvn dependency:list
```

## Eclipse

We recommend using Eclipse Kepler (4.3), due to the built-in support
for Maven and Git. Note that there are still some Maven plugins used
by sub-projects (e.g. documentation) which are not supported even in
Eclipse Kepler (4.3).

Eclipse [m2e] is already included in "Eclipse IDE for Java Developers", or it
can be installed from [Eclipse Kepler release repository].

[m2e]: http://eclipse.org/m2e/
[Eclipse Kepler release repository]: http://download.eclipse.org/releases/kepler

## IntelliJ IDEA

### Importing the Project

The following steps show how to import ActiveMQ Artemis source into IntelliJ IDEA and setup the correct maven profile to allow
running of JUnit tests from within the IDE.  (Steps are based on version: 13.1.4)

* File --> Import Project --> Select the root directory of the ActiveMQ Artemis source folder. --> Click OK

This should open the import project wizard.  From here:

* Select "Import from existing model" toggle box, then select Maven from the list box below.  Click Next.
* Leave the defaults set on this page and click next.
* On the "Select profiles page", select the checkbox next to "tests" and click next.
* From here the default settings should suffice.  Continue through the wizard, clicking next until the wizard is complete.

Once the project has been imported and IDEA has caught up importing all the relevant dependencies, you should be able to
run JUnit tests from with the IDE.  Select any test class in the tests -> integration tests folder.  Right click on the
class in the project tab and click "Run <classname>".  If the "Run <classname>" option is present then you're all set to go.


### Style Templates for Idea

We have shared the style templates that are good for this project. If you want to apply them use these steps:

* File->Import Settings
* Select the file under ./artemis-cloned-folder/etc/IDEA-style.jar
* Select both Code Style Templates and File templates (it's the default option)
* Select OK and restart Idea

### Issue: My JUnit tests are not runnable with in the IDE.

If the "Run <classname>" or "Run all tests" option is not present.  It is likely that the default profile has not been
imported properly.  To (re)import the "tests" Maven profile in an existing project.

* Open the Maven Projects Tool Window: View -> Tool Windows -> Maven Projects
* Select the "profiles" drop down
* Unselect then reselect the checkbox next to "tests".
* Click on the "Reimport all maven projects" button in the top left hand corner of the window. (It looks like a ciruclar
blue arrow.
* Wait for IDEA to reload and try running a JUnit test again.  The option to run should now be present.

### Annotation Pre-Processing

ActiveMQ Artemis uses [JBoss Logging] and that requires source code generation from Java
annotations. In order for it to 'just work' in Eclipse you need to install the
_Maven Integration for Eclipse JDT Annotation Processor Toolkit_ [m2e-apt]. See
this [JBoss blog post] for details.

[JBoss Logging]: <https://community.jboss.org/wiki/JBossLoggingTooling>
[m2e-apt]: https://github.com/jbosstools/m2e-apt
[JBoss blog post]: https://community.jboss.org/en/tools/blog/2012/05/20/annotation-processing-support-in-m2e-or-m2e-apt-100-is-out

### M2E Connector for Javacc-Maven-Plugin

Eclipse Indigo (3.7) has out-of-the-box support for it.

As of this writing, Eclipse Kepler (4.3) still lacks support for
Maven's javacc plugin. The available [m2e connector for
javacc-maven-plugin] requires a downgrade of Maven components to be
installed. manual installation instructions (as of this writing you
need to use the development update site). See [this post] for how to
do this with Eclipse Juno (4.2).

The current recommended solution for Eclipse Kepler is to mark
`javacc-maven-plugin` as ignored by Eclipse, run Maven from the
command line and then modify the project `activemq-core-client` adding
the folder `target/generated-sources/javacc` to its build path.

[m2e connector for javacc-maven-plugin]: https://github.com/objectledge/maven-extensions
[this post]:
http://dev.eclipse.org/mhonarc/lists/m2e-users/msg02725.html

### Use _Project Working Sets_

Importing all ActiveMQ Artemis subprojects will create _too many_ projects in Eclipse,
cluttering your _Package Explorer_ and _Project Explorer_ views. One way to address
that is to use [Eclipse's Working Sets] feature. A good introduction to it can be
found at a [Dzone article on Eclipse Working Sets].

[Eclipse's Working Sets]: http://help.eclipse.org/juno/index.jsp?topic=%2Forg.eclipse.platform.doc.user%2Fconcepts%2Fcworkset.htm
[Dzone article on Eclipse Working Sets]: http://eclipse.dzone.com/articles/categorise-projects-package

### Code Formatting

Eclipse code formatting and (basic) project configuration files can be found at
the ```etc/``` folder. You should manually copy them _after importing all your
projects_:

```
for settings_dir in `find . -type d -name .settings`; do
   \cp -v etc/org.eclipse.jdt.* $settings_dir
done
```

Do not use the [maven-eclipse-plugin] to copy the files as it conflicts with [m2e].

[maven-eclipse-plugin]: https://maven.apache.org/plugins/maven-eclipse-plugin/
[m2e]: http://eclipse.org/m2e/

## Committing Changes

### Repositories

The code repository for ActiveMQ Artemis is hosted by Apache org and lives here: https://git-wip-us.apache.org/repos/asf/activemq-artemis.git.

We also host a mirror of the ActiveMQ Artemis repository on GitHub: https://github.com/apache/activemq-artemis.  We use this mirror for all code submissions and reviews.  To submit code to ActiveMQ Artemis please open a Pull Request as outlined as part of the GitHub workflow described here: https://guides.github.com/introduction/flow/index.html.  Once a pull request is opened it will be reviewed and commented on.  Any further changes as a result of comments / review process should be addressed and reflected in the original pull request as outlined in the GitHub workflow.  When the pull request has went through the review process and ready to merge, the reviewer should comment with "Ack, Ready to Push".  Once an Ack message is received one of the ActiveMQ Artemis core team members will push the changes to upstream Apache ActiveMQ Artemis repository and close the pull request.

### Commit Messages

We follow the 50/72 git commit message format.  An ActiveMQ Artemis commit message should be formatted in the following manner:

* Add the ACTIVEMQ6 JIRA or Bugzilla reference (if one exists) followed by a brief description of the change in the first line.
* Insert a single blank line after the first line.
* Provide a detailed description of the change in the following lines, breaking paragraphs where needed.
* The first line should be limited to 50 characters
* Subsequent lines should be wrapped at 72 characters.

An example correctly formatted commit message:

```
ACTIVEMQ6-123 Add new commit msg format to README

Adds a description of the new commit message format as well as examples
of well formatted commit messages to the README.md.  This is required 
to enable developers to quickly identify what the commit is intended to 
do and why the commit was added.
```
### Adding New Dependencies

Due to incompatibilities between some open source licenses and the Apache v2.0 license (that this project is licensed under)
care must be taken when adding new dependencies to the project.  The Apache Software Foundation 3rd party
 licensing policy has more information here: http://www.apache.org/legal/3party.html

To keep track of all licenses in ActiveMQ Artemis, new dependencies must be added in either the top level pom.xml or in test/pom.xml
(depending on whether this is a test only dependency or if it is used in the main code base).  The dependency should be
added under the dependency management section with version and labelled with a comment highlighting the license for the
dependency version.  See existing dependencies in the main pom.xml for examples.  The dependency can then be added to
individual ActiveMQ Artemis modules *without* the version specified (the version is implied from the dependency management
section of the top level pom).  This allows ActiveMQ Artemis developers to keep track of all dependencies and licenses.

### Core Contributers

Core ActiveMQ Artemis members have write access to the Apache ActiveMQ Artemis repositories and will be responsible for Ack'ing and pushing commits contributed via pull requests on GitHub.  The follow steps can be used as an example for how to set up relevant ActiveMQ Artemis repositories for reviewing and pushing changes.

To setup repositories for reviewing and pushing:

```bash
  # Clone the GitHub Mirror of ActiveMQ Artemis Repo:
  git clone git@github.com:apache/activemq-artemis.git

  # Add the following section to your <artemis-repo>/.git/config statement to fetch all pull requests sent to the GitHub mirror.  Note that the remote name for git@github.com:apache/activemq-artemis.git may be different.  Be sure to edit all references to the remote name.  In this case "activemq".

  [remote "origin"]
        url = git@github.com:apache/activemq-artemis.git
        fetch = +refs/heads/*:refs/remotes/origin/*
        fetch = +refs/pull/*/head:refs/remotes/origin/pr/*


  # Add the Apache repository as a remote
  git remote add upstream https://git-wip-us.apache.org/repos/asf/activemq-artemis.git

  # Fetch
  git fetch --all
```

To push commits from a pull request to the apache repository:

```bash
  cd <artemis-repo>

  # Download all the remote branches etc... including all the pull requests.
  git fetch --all
  
  # Checkout the pull request you wish to review
  git checkout pr/2

  # Review is done...  READY TO MERGE.

  # Check out the master branch.
  git checkout master

  # Ensure you are up to date
  git pull

  # Create a new merge commit from the 
  git merge --no-ff pr/2

  #  IMPORTANT: In this commit message be sure to write something along the lines of: "Merge Pull Request #2" Where 2 is the Pull Request ID.  "#2" shows up as a link in the GitHub UI for navigating to the PR from the commit message.

  # Pushes to the upstream repo.
  git push upstream master
```

#### Notes:

The GitHub mirror repository is cloning the Apache ActiveMQ Artemis repository (The root repository).  There maybe a slight delay between when a commit is pushed to the Apache repo and when that commit is reflected in the GitHub mirror.  This may cause some difficulty when trying to push a PR to upstream (Apache) that has been merged on an out of date GitHub (mirror) master.  You can wait for the mirror to update before performing the steps above.  A solution to this is to change local master branch to track the upstream (Apache) master, rather than GitHub (mirror) master by editing your config to look like this:

```bash
  [branch "master"]
        remote = upstream
        merge = refs/heads/master
```

Where upstream points to the Apache Repo.

If you'd like master to always track GitHub master, then another way to acheive this is to add another branch that tracks upstream master and push from that branch to upstream master e.g.

```bash
  # .git/config entry
  [branch "umaster"]
        remote = upstream
        merge = refs/heads/master

  git checkout umaster
  git pull
  git merge --no-ff pr/2
  git push upstream umaster:master # Push local branch umaster to upstream branch master.
```

