package org.apache.activemq.artemis.tests.servercompatibility.base;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyShell;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.FutureTask;

public abstract class ScriptedCompatibilityTest {

    private static final Logger logger = Logger.getLogger(ScriptedCompatibilityTest.class);
    private static final Set<String> warningsPrintedFor = new HashSet<>();

    protected static final int FIRST = 1;
    protected static final int SECOND = 2;
    protected static final int THIRD = 3;

    private final Logger log = logger;
    private final String[] sides;
    /**
     * Maps side index to Groovy shell to keep the sides separated.
     */
    private final Map<Integer, Object> shells = new HashMap<>();
    /**
     * Maps side name to a classloader, which we can reuse.
     */
    private static final Map<String, GroovyClassLoader> classLoaders = new HashMap<>();

    private File workingDir;

    @Rule
    public TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Rule
    public TestRule watcher = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            log.info(String.format("**** start #test %s() ***", ScriptedCompatibilityTest.this.getClass().getName() + "::" + description.getMethodName()));
        }

        @Override
        protected void finished(Description description) {
            log.info(String.format("**** end #test %s() ***", ScriptedCompatibilityTest.this.getClass().getName() + "::" + description.getMethodName()));
        }
    };

    protected ScriptedCompatibilityTest(String... sides) {
        if (sides.length < 2) {
            throw new IllegalArgumentException("sides.length < 2");
        }
        this.sides = sides;
    }

    protected Object executeScript(int sideIndex, String scriptPath) throws Throwable {
        final Object shell = getShell(sideIndex);
        final FutureTask<Object> executionTask = new FutureTask<>(() -> shell.getClass().getMethod("evaluate", File.class).invoke(shell, getScript(scriptPath)));
        final Thread executionThread = new Thread(executionTask);
        executionThread.setName(getSide(sideIndex) + ": " + scriptPath);
        // If we don't replace the context class loader too then some things -- such as ServiceLoader -- don't work properly.
        executionThread.setContextClassLoader((ClassLoader) shell.getClass().getMethod("getClassLoader").invoke(shell));
        executionThread.start();
        executionThread.join();
        try {
            return executionTask.get();
        } catch (Exception e) {
            tryUnpackAssertionError(e);
        }
        return null;
    }

    protected Object getVariable(int sideIndex, String name) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final Object shell = getShell(sideIndex);
        return shell.getClass().getMethod("getVariable", String.class).invoke(shell, name);
    }

    protected void setVariable(int sideIndex, String name, Object value) {
        final Object shell = getShell(sideIndex);
        try {
            shell.getClass().getMethod("setVariable", String.class, Object.class).invoke(shell, name, value);
        } catch (Exception e) {
            Assume.assumeNoException(e);
        }
    }

    private Object getShell(int sideIndex) {
        return shells.computeIfAbsent(sideIndex, (i) -> {
            final String side = getSide(i);
            final Binding binding = new Binding(Map.of(
                    "workingDir", getWorkingDir(),
                    "side", side
            ));
            try {
                return createShell(getShellClassLoader(side), binding);
            } catch (Exception e) {
                Assume.assumeNoException(e);
            }
            return null;
        });
    }

    /**
     * Creates a {@linkplain GroovyShell} object using the class taken from the given class loader. This is important
     * because we need the shell to be compatible with the scripts we're about to run there.
     *
     * @param cl      The class loader.
     * @param binding The binding to be COPIED as the new shell context.
     * @return The shell as the instance of class which may not be available during compile time.
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static Object createShell(ClassLoader cl, Binding binding) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        final Class<?> confClass = cl.loadClass(CompilerConfiguration.class.getCanonicalName());
        final Object conf = confClass.getConstructor().newInstance();
        conf.getClass().getMethod("setScriptBaseClass", String.class).invoke(conf, CompatibilityTestScript.class.getCanonicalName());
        final Class<?> bindingClass = cl.loadClass(Binding.class.getCanonicalName());
        final Object newBinding = bindingClass.getConstructor(Map.class).newInstance(new HashMap<Object, Object>(binding.getVariables()));
        final Class<?> shellClass = cl.loadClass(GroovyShell.class.getCanonicalName());
        return shellClass.getConstructor(ClassLoader.class, bindingClass, confClass).newInstance(cl, newBinding, conf);
    }

    protected File getWorkingDir() {
        if (workingDir == null) {
            try {
                workingDir = temporaryFolder.newFolder();
            } catch (IOException e) {
                Assume.assumeNoException("Failed to create test working directory under '" + temporaryFolder.getRoot().getAbsolutePath() + "'.", e);
                return null;
            }
        }
        return workingDir;
    }

    protected static GroovyClassLoader getShellClassLoader(String side) {
        return classLoaders.computeIfAbsent(side, ScriptedCompatibilityTest::createShellClassLoader);
    }

    private String getSide(int sideIndex) {
        if (sideIndex < 1) {
            throw new IllegalArgumentException("sideIndex < 1");
        }
        if (sideIndex > sides.length) {
            throw new IllegalArgumentException("sideIndex > " + sides.length);
        }
        return sides[sideIndex - 1];
    }

    protected static File getScript(String scriptPath) throws IOException {
        final URL resource = ScriptedCompatibilityTest.class.getClassLoader().getResource(scriptPath);
        if (resource == null) {
            throw new IOException("Resource not found: " + scriptPath);
        }
        try {
            return new File(resource.toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a class loader which shall prioritize the side-related classes.
     *
     * @param side The side.
     * @return The class loader.
     */
    private static GroovyClassLoader createShellClassLoader(String side) {
        // Collect entries on the current classpath so that we can recompose the current class loader later.
        final List<URL> baseClassPath = new LinkedList<>();
        final Enumeration<URL> baseClassPathEnumeration;
        try {
            baseClassPathEnumeration = ScriptedCompatibilityTest.class.getClassLoader().getResources("");
            while (baseClassPathEnumeration.hasMoreElements()) {
                baseClassPath.add(baseClassPathEnumeration.nextElement());
            }
        } catch (IOException e) {
            Assume.assumeNoException(e);
        }
        // We need to put the requested server version at the beginning of class loader chain so that it gets selected
        // with higher priority. At the same time we need the other classes to be present. These will be appended to the
        // chain.
        final ClassLoader serverClassLoader = new URLClassLoader(getClassPath(side), ClassLoader.getPlatformClassLoader());
        final ClassLoader baseClassLoader = new URLClassLoader(baseClassPath.toArray(new URL[0]), serverClassLoader);
        return new GroovyClassLoader(baseClassLoader);
    }

    private static URL[] getClassPath(String side) {
        final String classPath = System.getProperties().getProperty(side);
        if ((classPath == null) || classPath.isBlank()) {
            if (!warningsPrintedFor.contains(side)) {
                warningsPrintedFor.add(side);
                System.out.println("Add \"-D" + side + "=\'CLASSPATH\'\" into your VM settings");
                System.out.println("You will see it in the output from mvn install at the compatibility-tests");
                System.out.println("... look for output from dependency-scan");
                // our dependency scan used at the pom under compatibility-tests/pom.xml will generate these, example:
                // [INFO] dependency-scan setting: -DARTEMIS-140="/Users/someuser/....."
                // copy that into your IDE setting and you should be able to debug it
            }
            Assume.assumeTrue("Cannot run these tests, no classpath found", true);
        }
        assert classPath != null; // Avoid warning
        String[] classPathArray = classPath.split(File.pathSeparator);
        List<URL> elements = new ArrayList<>(classPathArray.length);
        for (String s : classPathArray) {
            try {
                elements.add(new File(s).toURI().toURL());
            } catch (MalformedURLException e) {
                Assume.assumeNoException("Invalid " + side + " classpath entry found.", e);
            }
        }
        return elements.toArray(new URL[0]);
    }

    private static void tryUnpackAssertionError(Throwable t) throws Throwable {
        Objects.requireNonNull(t);
        Throwable cause = t;
        while (cause != null) {
            cause = cause.getCause();
            if (cause instanceof AssertionError) {
                throw cause;
            }
        }
        throw t;
    }
}
