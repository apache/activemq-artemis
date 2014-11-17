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
package org.apache.activemq6.tests.unit.util;

import java.io.Serializable;
import java.util.Properties;

import javax.naming.CompoundName;
import javax.naming.Name;
import javax.naming.NameParser;
import javax.naming.NamingException;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 2868 $</tt>
 *
 */
public class InVMNameParser implements NameParser, Serializable
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 2925203703371001031L;

   // Static --------------------------------------------------------

   static Properties syntax;

   static
   {
      InVMNameParser.syntax = new Properties();
      InVMNameParser.syntax.put("jndi.syntax.direction", "left_to_right");
      InVMNameParser.syntax.put("jndi.syntax.ignorecase", "false");
      InVMNameParser.syntax.put("jndi.syntax.separator", "/");
   }

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public static Properties getSyntax()
   {
      return InVMNameParser.syntax;
   }

   public Name parse(final String name) throws NamingException
   {
      return new CompoundName(name, InVMNameParser.syntax);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
