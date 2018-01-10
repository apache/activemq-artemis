# IDE Integration

There a few files useful for IDE integration under ./etc/ide-settings on a checked out folder. This folder is not part of the source distribution, but it can be easily obtained:

- https://github.com/apache/activemq-artemis/tree/master/etc/ide-settings

## IntelliJ IDEA

### Importing the Project

The following steps show how to import ActiveMQ Artemis source into IntelliJ IDEA and setup the correct maven profile to allow
running of JUnit tests from within the IDE.  (Steps are based on version: 2017.1.2)

* File --> Import Project --> Select the root directory of the ActiveMQ Artemis source folder. --> Click OK

This should open the import project wizard.  From here:

* Select "Import project from external model" toggle box, then select Maven from the list box below.  Click Next.
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

There's a [SOF Question](https://stackoverflow.com/questions/27906481/can-intellij-14-be-used-to-work-with-ibm-jdk-1-7/32852361#32852361) about this that could be useful in case you are running into this issue.

### Style Templates and Inspection Settings for Idea

We have shared the style templates that are good for this project. If you want to apply them use these steps:

* File->Import Settings
* Select the file under ./artemis-cloned-folder/etc/ide-settings/idea/IDEA-style.jar
* Select both Code Style Templates and File templates (it's the default option)
* Select OK and restart Idea

Alternatively you can copy artemis-codestyle.xml under your home settings at ``IntelliJIdea15/codestyles``.

#### To import inspection settings:

* File->Settings->Editor->Inspections->Manage->Import
* Select the file ./artemis-cloned-folder/etc/ide-settings/idea/artemis-inspections.xml
* Select OK

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

Eclipse [m2e](https://eclipse.org/m2e/) is already included in "Eclipse IDE for Java Developers", or it can be installed
from [Eclipse Kepler release repository](http://download.eclipse.org/releases/kepler).

### Git setup
It is strongly recommended to turn off the auto-updating of .gitignore files by the Git Team extension.  Otherwise, it
generates new .gitignore files in many directories that are not needed due to the top level .gitignore file.  To turn
it off, go to Preferences->Team->Git->Projects and deselect the "Automatically ignore derived resources" checkbox.

### Schema setup
For proper schema validation you can add the Artemis schemas to your Eclipse XML Catalog

* Open: Window -> Preferences -> XML -> XML Catalog
* Select Add -> Workspace -> Navigate to artemis-server and select src/main/resources/schema/artemis-server.xsd -> click OK
* Repeat the above steps and add src/main/resources/schema/artemis-configuration.xsd

### Checkstyle setup
You can import the Artemis Checkstyle template into eclipse to do Checkstyle validation. As a prerequisite you need to make sure the Checkstyle plugin is installed into Eclipse which you can get form the Eclipse Marketplace. You also will need to configure Sevntu-Checkstyle. See https://sevntu-checkstyle.github.io/sevntu.checkstyle/ for instructions. Then to configure the template:

* Open: Window -> Preferences -> Checkstyle
* Select New -> "Project Relative Configuration" in the "Type" dropdown
* Give the configuration a name and under location put "/artemis-pom/etc/checkstyle.xml" then hit ok
* You should now see your new configuration in the list of Checkstyle profiles.  You can select the new configuration as the default if you want.

### Annotation Pre-Processing

ActiveMQ Artemis uses [JBoss Logging](https://developer.jboss.org/wiki/JBossLoggingTooling) and that requires source
code generation from Java annotations. In order for it to 'just work' in Eclipse you need to install the
_Maven Integration for Eclipse JDT Annotation Processor Toolkit_ [m2e-apt](https://github.com/jbosstools/m2e-apt). See
this [JBoss blog post](https://community.jboss.org/en/tools/blog/2012/05/20/annotation-processing-support-in-m2e-or-m2e-apt-100-is-out)
 for details.
 
### Running tests from Eclipse
Setting up annotation pre-processing in the above section is all you need to run tests in the "unit-tests" project as that will properly add the generated logger to the source.  However, one more step is needed to run tests in other projects such as "performance-tests" or "integration-tests" that have a dependency on "unit-tests". Currently m2eclipse does not properly link the generated source annotations folder from "unit-tests" which causes the logger that is generated to not be available.  To simplest way to fix this is to manually add a project dependency on "unit-tests" to each of the projects where you want to run a test class from:

* Right click on the test project (i.e. integration-tests): Properties -> Java Build Path -> Projects -> Add
* Select the "unit-tests" project and click Ok

You should now be able to run tests assuming that the annotation pre-processing was set up properly in the previous step.

### M2E Connector for Javacc-Maven-Plugin

Eclipse Indigo (3.7) has out-of-the-box support for it.

As of this writing, Eclipse Kepler (4.3) still lacks support for Maven's javacc plugin. The available [m2e connector for
javacc-maven-plugin](https://github.com/objectledge/maven-extensions) requires a downgrade of Maven components to be
installed. manual installation instructions (as of this writing you need to use the development update site). See
[this post](https://dev.eclipse.org/mhonarc/lists/m2e-users/msg02725.html) for how to do this with Eclipse Juno (4.2).

The current recommended solution for Eclipse Kepler is to mark `javacc-maven-plugin` as ignored by Eclipse, run Maven
from the command line and then modify the project `activemq-core-client` adding the folder
`target/generated-sources/javacc` to its build path.

### Use _Project Working Sets_

Importing all ActiveMQ Artemis subprojects will create _too many_ projects in Eclipse, cluttering your _Package Explorer_
and _Project Explorer_ views. One way to address that is to use
[Eclipse's Working Sets](https://help.eclipse.org/juno/index.jsp?topic=%2Forg.eclipse.platform.doc.user%2Fconcepts%2Fcworkset.htm)
feature. A good introduction to it can be found at a
[Dzone article on Eclipse Working Sets](https://dzone.com/articles/categorise-projects-package).
