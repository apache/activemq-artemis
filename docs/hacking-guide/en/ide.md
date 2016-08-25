# IDE Integration

## IntelliJ IDEA

### Importing the Project

The following steps show how to import ActiveMQ Artemis source into IntelliJ IDEA and setup the correct maven profile to allow
running of JUnit tests from within the IDE.  (Steps are based on version: 13.1.4)

* File --> Import Project --> Select the root directory of the ActiveMQ Artemis source folder. --> Click OK

This should open the import project wizard.  From here:

* Select "Import from existing model" toggle box, then select Maven from the list box below.  Click Next.
* Leave the defaults set on this page and click next.
* On the "Select profiles page", select the checkbox next to "dev" and click next.
* From here the default settings should suffice.  Continue through the wizard, clicking next until the wizard is complete.

Once the project has been imported and IDEA has caught up importing all the relevant dependencies, you should be able to
run JUnit tests from with the IDE.  Select any test class in the tests -> integration tests folder.  Right click on the
class in the project tab and click "Run <classname>".  If the "Run <classname>" option is present then you're all set to go.

### Note about IBM JDK on Idea

If you are running IBM JDK it may be a little tricky to get it working.

After you add the JDK to the IDE, add also the vm.jar specific to your platform under that jdk.

```
(e.g: JAVA_HOME/jre/lib/amd64/default/jclSC180/vm.jar
```

There's a [SOF Question](http://stackoverflow.com/questions/27906481/can-intellij-14-be-used-to-work-with-ibm-jdk-1-7/32852361#32852361) about this that could be useful in case you are running into this issue.

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
* Click on the "Reimport all maven projects" button in the top left hand corner of the window. (It looks like a circular
blue arrow.
* Wait for IDEA to reload and try running a JUnit test again.  The option to run should now be present.

## Eclipse

We recommend using Eclipse Kepler (4.3), due to the built-in support for Maven and Git. Note that there are still some
Maven plugins used by sub-projects (e.g. documentation) which are not supported even in Eclipse Kepler (4.3).

Eclipse [m2e](http://eclipse.org/m2e/) is already included in "Eclipse IDE for Java Developers", or it can be installed
from [Eclipse Kepler release repository](http://download.eclipse.org/releases/kepler).

### Git setup
It is strongly recommended to turn off the auto-updating of .gitignore files by the Git Team extension.  Otherwise, it
generates new .gitignore files in many directories that are not needed due to the top level .gitignore file.  To turn
it off, go to Preferences->Team->Git->Projects and deselect the "Automatically ignore derived resources" checkbox.


### Annotation Pre-Processing

ActiveMQ Artemis uses [JBoss Logging](https://community.jboss.org/wiki/JBossLoggingTooling) and that requires source
code generation from Java annotations. In order for it to 'just work' in Eclipse you need to install the
_Maven Integration for Eclipse JDT Annotation Processor Toolkit_ [m2e-apt](https://github.com/jbosstools/m2e-apt). See
this [JBoss blog post](https://community.jboss.org/en/tools/blog/2012/05/20/annotation-processing-support-in-m2e-or-m2e-apt-100-is-out)
 for details.

### M2E Connector for Javacc-Maven-Plugin

Eclipse Indigo (3.7) has out-of-the-box support for it.

As of this writing, Eclipse Kepler (4.3) still lacks support for Maven's javacc plugin. The available [m2e connector for
javacc-maven-plugin](https://github.com/objectledge/maven-extensions) requires a downgrade of Maven components to be
installed. manual installation instructions (as of this writing you need to use the development update site). See
[this post](http://dev.eclipse.org/mhonarc/lists/m2e-users/msg02725.html) for how to do this with Eclipse Juno (4.2).

The current recommended solution for Eclipse Kepler is to mark `javacc-maven-plugin` as ignored by Eclipse, run Maven
from the command line and then modify the project `activemq-core-client` adding the folder
`target/generated-sources/javacc` to its build path.

### Use _Project Working Sets_

Importing all ActiveMQ Artemis subprojects will create _too many_ projects in Eclipse, cluttering your _Package Explorer_
and _Project Explorer_ views. One way to address that is to use
[Eclipse's Working Sets](http://help.eclipse.org/juno/index.jsp?topic=%2Forg.eclipse.platform.doc.user%2Fconcepts%2Fcworkset.htm)
feature. A good introduction to it can be found at a
[Dzone article on Eclipse Working Sets](http://eclipse.dzone.com/articles/categorise-projects-package).
