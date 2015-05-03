package org.apache.activemq.artemis.cli.commands;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

import static java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.GROUP_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

/**
 * CLI action that creates a broker instance directory.
 */
@Command(name = "create", description = "creates a new broker instance")
public class Create implements Action
{
   @Arguments(description = "The instance directory to hold the broker's configuration and data", required = true)
   File directory;

   @Option(name = "--host", description = "The host name of the broker")
   String host;

   @Option(name = "--force", description = "Overwrite configuration at destination directory")
   boolean force;

   @Option(name = "--home", description = "Directory where ActiveMQ Artemis is installed")
   File home;

   @Option(name = "--with-ssl", description = "Generate an SSL enabled configuration")
   boolean with_ssl = true;

   @Option(name = "--clustered", description = "Enable clustering")
   boolean clustered = false;

   @Option(name = "--replicated", description = "Enable broker replication")
   boolean replicated = false;

   @Option(name = "--shared-store", description = "Enable broker shared store")
   boolean sharedStore = false;

   @Option(name = "--encoding", description = "The encoding that text files should use")
   String encoding = "UTF-8";

   ActionContext context;
   boolean IS_WINDOWS;
   boolean IS_CYGWIN;

   @Override
   public Object execute(ActionContext context) throws Exception
   {
      try
      {
         return run(context);
      }
      catch (Throwable e)
      {
         e.printStackTrace(context.err);
         return e;
      }
   }

   public Object run(ActionContext context) throws Exception
   {
      this.context = context;
      IS_WINDOWS = System.getProperty("os.name").toLowerCase().trim().startsWith("win");
      IS_CYGWIN = IS_WINDOWS && System.getenv("OSTYPE") == "cygwin";

      if (clustered & sharedStore)
      {

      }

      context.out.println(String.format("Creating ActiveMQ Artemis instance at: %s", directory.getCanonicalPath()));
      if (host == null)
      {
         host = directory.getName();
      }

      HashMap<String, String> filters = new HashMap<String, String>();
      String replicatedSettings = "";
      if (replicated)
      {
         clustered = true;
         replicatedSettings = readTextFile("etc/replicated-settings.txt");
      }
      filters.put("${replicated.settings}", replicatedSettings);

      String sharedStoreSettings = "";
      if (sharedStore)
      {
         clustered = true;
         sharedStoreSettings = readTextFile("etc/shared-store-settings.txt");
      }
      filters.put("${hared-store.settings}", sharedStoreSettings);

      String clusterSettings = "";
      if (clustered)
      {
         clusterSettings = readTextFile("etc/cluster-settings.txt");
      }
      filters.put("${cluster.settings}", clusterSettings);


      filters.put("${user}", System.getProperty("user.name", ""));
      filters.put("${host}", host);
      if (home != null)
      {
         filters.put("${home}", path(home, false));
      }
      filters.put("${artemis.home}", path(System.getProperty("artemis.home"), false));
      filters.put("${artemis.instance}", path(directory, false));
      filters.put("${java.home}", path(System.getProperty("java.home"), false));

      new File(directory, "bin").mkdirs();
      new File(directory, "etc").mkdirs();
      new File(directory, "log").mkdirs();
      new File(directory, "tmp").mkdirs();
      new File(directory, "data").mkdirs();


      if (IS_WINDOWS)
      {
         write("bin/artemis.cmd", null, false);
         write("bin/artemis-service.exe");
         write("bin/artemis-service.xml", filters, false);
         write("etc/artemis.profile.cmd", filters, false);
      }

      if (!IS_WINDOWS || IS_CYGWIN)
      {
         write("bin/artemis", null, true);
         makeExec("bin/artemis");
         write("bin/artemis-service", null, true);
         makeExec("bin/artemis-service");
         write("etc/artemis.profile", filters, true);
         makeExec("etc/artemis.profile");
      }

      write("etc/logging.properties", null, false);
      write("etc/bootstrap.xml", null, false);
      write("etc/broker.xml", filters, false);
      write("etc/artemis-roles.properties", null, false);
      write("etc/artemis-users.properties", null, false);

      context.out.println("");
      context.out.println("You can now start the broker by executing:  ");
      context.out.println("");
      context.out.println(String.format("   \"%s\" run", path(new File(directory, "bin/artemis"), true)));

      File service = new File(directory, "bin/artemis-service");
      context.out.println("");

      if (!IS_WINDOWS || IS_CYGWIN)
      {

         // Does it look like we are on a System V init system?
         if (new File("/etc/init.d/").isDirectory())
         {
            context.out.println("Or you can setup the broker as system service and run it in the background:");
            context.out.println("");
            context.out.println("   sudo ln -s \"%s\" /etc/init.d/".format(service.getCanonicalPath()));
            context.out.println("   /etc/init.d/artemis-service start");
            context.out.println("");

         }
         else
         {

            context.out.println("Or you can run the broker in the background using:");
            context.out.println("");
            context.out.println(String.format("   \"%s\" start", path(service, true)));
            context.out.println("");
         }

      }
      if (IS_WINDOWS)
      {

         context.out.println("Or you can setup the broker as Windows service and run it in the background:");
         context.out.println("");
         context.out.println(String.format("   \"%s\" install", path(service, true)));
         context.out.println(String.format("   \"%s\" start", path(service, true)));
         context.out.println("");

      }

      return null;
   }

   private void makeExec(String path) throws IOException
   {
      try
      {
         File file = new File(directory, path);
         Files.setPosixFilePermissions(file.toPath(), new HashSet<PosixFilePermission>(Arrays.asList(
            OWNER_READ, OWNER_WRITE, OWNER_EXECUTE,
            GROUP_READ, GROUP_WRITE, GROUP_EXECUTE,
            OTHERS_READ, OTHERS_EXECUTE
         )));
      }
      catch (Throwable ignore)
      {
         // Our best effort was not good enough :)
      }
   }

   String path(String value, boolean unixPaths) throws IOException
   {
      return path(new File(value), unixPaths);
   }

   private String path(File value, boolean unixPaths) throws IOException
   {
      if (unixPaths && IS_CYGWIN)
      {
//        import scala.sys.process._
//        Seq("cygpath", value.getCanonicalPath).!!.trim
         return value.getCanonicalPath();
      }
      else
      {
         return value.getCanonicalPath();
      }
   }

   private void write(String source, HashMap<String, String> filters, boolean unixTarget) throws IOException
   {
      write(source, new File(directory, source), filters, unixTarget);
   }

   private void write(String source, File target, HashMap<String, String> filters, boolean unixTarget) throws IOException
   {
      if (target.exists() && !force)
      {
         throw new RuntimeException(String.format("The file '%s' already exists.  Use --force to overwrite.", target));
      }

      String content = readTextFile(source);

      if (filters != null)
      {
         for (Map.Entry<String, String> entry : filters.entrySet())
         {
            content = replace(content, entry.getKey(), entry.getValue());
         }
      }

      // and then writing out in the new target encoding..  Let's also replace \n with the values
      // that is correct for the current platform.
      String separator = unixTarget && IS_CYGWIN ? "\n" : System.getProperty("line.separator");
      content = content.replaceAll("\\r?\\n", Matcher.quoteReplacement(separator));
      ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes(encoding));

      try (FileOutputStream fout = new FileOutputStream(target))
      {
         copy(in, fout);
      }
   }

   private String readTextFile(String source) throws IOException
   {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (InputStream in = this.getClass().getResourceAsStream(source))
      {
         copy(in, out);
      }
      return new String(out.toByteArray(), "UTF-8");
   }

   private void write(String source) throws IOException
   {
      File target = new File(directory, source);
      if (target.exists() && !force)
      {
         throw new RuntimeException(String.format("The file '%s' already exists.  Use --force to overwrite.", target));
      }
      try (FileOutputStream fout = new FileOutputStream(target))
      {
         try (InputStream in = this.getClass().getResourceAsStream(source))
         {
            copy(in, fout);
         }
      }
   }

   private boolean canLoad(String name)
   {
      try
      {
         this.getClass().getClassLoader().loadClass(name);
         return true;
      }
      catch (Throwable e)
      {
         return false;
      }
   }

   private String replace(String content, String key, String value)
   {
      return content.replaceAll(Pattern.quote(key), Matcher.quoteReplacement(value));
   }

   private int system(File wd, String... command) throws IOException, InterruptedException
   {
      Process process = Runtime.getRuntime().exec(command, null, wd);
      process.getOutputStream().close();
      drain(command[0], process.getInputStream(), context.out);
      drain(command[0], process.getErrorStream(), context.err);
      process.waitFor();
      return process.exitValue();
   }

   private void drain(String threadName, final InputStream is, final OutputStream os)
   {
      new Thread(threadName)
      {
         {
            setDaemon(true);
         }

         @Override
         public void run()
         {
            try
            {
               copy(is, os);
            }
            catch (Throwable e)
            {
            }
         }
      }.start();
   }

   private void copy(InputStream is, OutputStream os) throws IOException
   {
      byte[] buffer = new byte[1024 * 4];
      int c = is.read(buffer);
      while (c >= 0)
      {
         os.write(buffer, 0, c);
         c = is.read(buffer);
      }
   }

}
