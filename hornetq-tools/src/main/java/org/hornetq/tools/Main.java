/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tools;

public class Main
{
   public static final String USAGE = "Use: java -jar " + getJarName();
   private static final String IMPORT = "import";
   private static final String EXPORT = "export";
   private static final String PRINT_DATA = "print-data";
   private static final String PRINT_PAGES = "print-pages";
   private static final String DATA_TOOL = "data-tool";
   private static final String TRANSFER = "transfer-queue";
   private static final String OPTIONS = " [" + IMPORT + "|" + EXPORT + "|" + PRINT_DATA + "|" + PRINT_PAGES + "|" + DATA_TOOL + "|" + TRANSFER + "]";

   public static void main(String[] arg) throws Exception
   {
      if (arg.length == 0)
      {
         System.out.println(USAGE + OPTIONS);
         System.exit(-1);
      }


      if (TRANSFER.equals(arg[0]))
      {
         TransferQueue tool = new TransferQueue();
         tool.process(arg);
      }
      else if (DATA_TOOL.equals(arg[0]))
      {
         DataTool dataTool = new DataTool();
         dataTool.process(arg);
      }
      else if (EXPORT.equals(arg[0]))
      {
         if (arg.length != 5)
         {
            System.out.println(USAGE + " " + EXPORT + " <bindings-directory> <journal-directory> <paging-directory> <large-messages-directory>");
            System.exit(-1);
         }
         else
         {
            XmlDataExporter xmlDataExporter = new XmlDataExporter(System.out, arg[1], arg[2], arg[3], arg[4]);
            xmlDataExporter.writeXMLData();
         }
      }
      else if (IMPORT.equals(arg[0]))
      {
         if (arg.length != 6)
         {
            System.out.println(USAGE + " " + IMPORT + " <input-file> <host> <port> <transactional> <application-server-compatibility>");
            System.exit(-1);
         }
         else
         {
            XmlDataImporter xmlDataImporter = new XmlDataImporter(arg[1], arg[2], arg[3], Boolean.parseBoolean(arg[4]), Boolean.parseBoolean(arg[5]));
            xmlDataImporter.processXml();
         }
      }
      else if (PRINT_DATA.equals(arg[0]))
      {
         if (arg.length != 3)
         {
            System.err.println(USAGE + " " + PRINT_DATA + " <bindings-directory> <journal-directory>");
            System.exit(-1);
         }

         PrintData.printData(arg[1], arg[2]);
      }
      else if (PRINT_PAGES.equals(arg[0]))
      {
         if (arg.length != 3)
         {
            System.err.println(USAGE + " " + PRINT_PAGES + " <paging-directory> <journal-directory>");
            System.exit(-1);
         }

         PrintPages.printPages(arg[1], arg[2]);
      }
      else
      {
         System.out.println(USAGE + OPTIONS);
      }
   }

   protected static String getJarName()
   {
      try
      {
         Class klass = Main.class;
         String url = klass.getResource('/' + klass.getName().replace('.', '/') + ".class").toString();
         String jarName = url.substring(0, url.lastIndexOf('!'));
         return jarName.substring(jarName.lastIndexOf('/') + 1);
      }
      catch (Throwable e)
      {
         return "tool-jar-name.jar";
      }
   }
}