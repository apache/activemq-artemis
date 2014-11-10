# HornetQ

If you need information about the HornetQ project please go to

http://community.jboss.org/wiki/HornetQ

http://www.jboss.org/hornetq/

This file describes some minimum 'stuff one needs to know' to get
started coding in this project.

## Source

The project's source code is hosted at:

https://github.com/hornetq

### Git usage:

Pull requests should be merged without fast forwards '--no-ff'. An easy way to achieve that is to use

```% git config branch.master.mergeoptions --no-ff```

## Maven

The minimum required Maven version is 3.0.0.

Do note that there are some compatibility issues with Maven 3.X still
unsolved [1]. This is specially true for the 'site' plugin [2].

[1]: <https://cwiki.apache.org/MAVEN/maven-3x-compatibility-notes.html>
[2]: <https://cwiki.apache.org/MAVEN/maven-3x-and-site-plugin.html>

## Tests

To run the unit tests:

```% mvn -Phudson-tests test```

Generating reports from unit tests:

```% mvn install site```


Running tests individually

```% mvn -Phudson-tests -DfailIfNoTests=false -Dtest=<test-name> test ```

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

## To build a release artifact

```% mvn -Prelease install```

## To build the release bundle

```% mvn -Prelease package```

## Eclipse

We recommend using Eclipse Kepler (4.3), due to the built-in support
for Maven and Git. Note that there are still some Maven plugins used
by sub-projects (e.g. documentation) which are not supported even in
Eclipse Kepler (4.3).

Eclipse [m2e] is already included in "Eclipse IDE for Java Developers", or it
can be installed from [Eclipse Kepler release repository].

[m2e]: http://eclipse.org/m2e/
[Eclipse Kepler release repository]: http://download.eclipse.org/releases/kepler

### Annotation Pre-Processing

HornetQ uses [JBoss Logging] and that requires source code generation from Java
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
command line and then modify the project `hornetq-core-client` adding
the folder `target/generated-sources/javacc` to its build path.

[m2e connector for javacc-maven-plugin]: https://github.com/objectledge/maven-extensions
[this post]:
http://dev.eclipse.org/mhonarc/lists/m2e-users/msg02725.html

### Use _Project Working Sets_

Importing all HornetQ subprojects will create _too many_ projects in Eclipse,
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

### GitHub

We follow the GitHub workflow for all code changes in HornetQ. For information on the GitHub workflow please see:
https://guides.github.com/introduction/flow/index.html

### Commit Messages

We follow the 50/72 git commit message format.  A HornetQ commit message should be formatted in the following manner:

* Add the HornetQ JIRA or Bugzilla reference (if one exists) followed by a brief description of the change in the first line.
* Insert a single blank line after the first line.
* Provide a detailed description of the change in the following lines, breaking paragraphs where needed.
* The first line should be limited to 50 characters
* Subsequent lines should be wrapped at 72 characters.

An example correctly formatted commit message:

```
HORNETQ-1234 Add new commit msg format to README

Adds a description of the new commit message format as well as examples
of well formatted commit messages to the README.md.  This is required 
to enable developers to quickly identify what the commit is intended to 
do and why the commit was added.
```
