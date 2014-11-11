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
package org.apache.activemq6.ra;

import java.io.Serializable;
import java.util.Hashtable;

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.utils.DefaultSensitiveStringCodec;
import org.apache.activemq6.utils.PasswordMaskingUtil;
import org.apache.activemq6.utils.SensitiveDataCodec;


/**
 * The RA default properties - these are set in the ra.xml file
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class HornetQRAProperties extends ConnectionFactoryProperties implements Serializable
{
   /**
    * Serial version UID
    */
   static final long serialVersionUID = -2772367477755473248L;
   /**
    * Trace enabled
    */
   private static boolean trace = HornetQRALogger.LOGGER.isTraceEnabled();

   /**
    * The user name
    */
   private String userName;

   /**
    * The password
    */
   private String password = null;

   /**
    * Use Local TX instead of XA
    */
   private Boolean localTx = false;


   /**
    * Class used to locate the Transaction Manager.
    * Using JBoss5 as the default locator
    */
   private String transactionManagerLocatorClass = "org.apache.activemq6.integration.jboss.tm.JBoss5TransactionManagerLocator;org.hornetq.integration.jboss.tm.JBoss4TransactionManagerLocator";

   /**
    * Method used to locate the TM
    */
   private String transactionManagerLocatorMethod = "getTm;getTM";

   private static final int DEFAULT_SETUP_ATTEMPTS = -1;

   private static final long DEFAULT_SETUP_INTERVAL = 2 * 1000;

   private int setupAttempts = DEFAULT_SETUP_ATTEMPTS;

   private long setupInterval = DEFAULT_SETUP_INTERVAL;

   private Hashtable<?, ?> jndiParams;

   private boolean useJNDI;

   private boolean useMaskedPassword = false;

   private String passwordCodec;

   private boolean initialized = false;

   private transient SensitiveDataCodec<String> codecInstance;

   /**
    * Class used to get a JChannel
    */
   private String jgroupsChannelLocatorClass;

   /**
    * Name used to locate a JChannel
    */
   private String jgroupsChannelRefName;

   /**
    * Constructor
    */
   public HornetQRAProperties()
   {
      if (HornetQRAProperties.trace)
      {
         HornetQRALogger.LOGGER.trace("constructor()");
      }
   }

   /**
    * Get the user name
    *
    * @return The value
    */
   public String getUserName()
   {
      if (HornetQRAProperties.trace)
      {
         HornetQRALogger.LOGGER.trace("getUserName()");
      }

      return userName;
   }

   /**
    * Set the user name
    *
    * @param userName The value
    */
   public void setUserName(final String userName)
   {
      if (HornetQRAProperties.trace)
      {
         HornetQRALogger.LOGGER.trace("setUserName(" + userName + ")");
      }

      this.userName = userName;
   }

   /**
    * Get the password
    *
    * @return The value
    */
   public String getPassword()
   {
      if (HornetQRAProperties.trace)
      {
         HornetQRALogger.LOGGER.trace("getPassword()");
      }

      return password;
   }

   /**
    * Set the password
    * Based on UseMaskedPassword property, the password can be
    * plain text or encoded string. However we cannot decide
    * which is the case at this moment, because we don't know
    * when the UseMaskedPassword and PasswordCodec are loaded. So for the moment
    * we just save the password.
    *
    * @param password The value
    */
   public void setPassword(final String password)
   {
      if (HornetQRAProperties.trace)
      {
         HornetQRALogger.LOGGER.trace("setPassword(****)");
      }

      this.password = password;
   }

   /**
    * @return the useJNDI
    */
   public boolean isUseJNDI()
   {
      return useJNDI;
   }

   /**
    * @param value the useJNDI to set
    */
   public void setUseJNDI(final Boolean value)
   {
      useJNDI = value;
   }

   /**
    * @return return the jndi params to use
    */
   public Hashtable<?, ?> getParsedJndiParams()
   {
      return jndiParams;
   }


   public void setParsedJndiParams(Hashtable<?, ?> params)
   {
      jndiParams = params;
   }

   /**
    * Get the use XA flag
    *
    * @return The value
    */
   public Boolean getUseLocalTx()
   {
      if (HornetQRAProperties.trace)
      {
         HornetQRALogger.LOGGER.trace("getUseLocalTx()");
      }

      return localTx;
   }

   /**
    * Set the use XA flag
    *
    * @param localTx The value
    */
   public void setUseLocalTx(final Boolean localTx)
   {
      if (HornetQRAProperties.trace)
      {
         HornetQRALogger.LOGGER.trace("setUseLocalTx(" + localTx + ")");
      }

      this.localTx = localTx;
   }


   public void setTransactionManagerLocatorClass(final String transactionManagerLocatorClass)
   {
      this.transactionManagerLocatorClass = transactionManagerLocatorClass;
   }

   public String getTransactionManagerLocatorClass()
   {
      return transactionManagerLocatorClass;
   }

   public String getTransactionManagerLocatorMethod()
   {
      return transactionManagerLocatorMethod;
   }

   public void setTransactionManagerLocatorMethod(final String transactionManagerLocatorMethod)
   {
      this.transactionManagerLocatorMethod = transactionManagerLocatorMethod;
   }

   public int getSetupAttempts()
   {
      return setupAttempts;
   }

   public void setSetupAttempts(Integer setupAttempts)
   {
      this.setupAttempts = setupAttempts;
   }

   public long getSetupInterval()
   {
      return setupInterval;
   }

   public void setSetupInterval(Long setupInterval)
   {
      this.setupInterval = setupInterval;
   }

   public boolean isUseMaskedPassword()
   {
      return useMaskedPassword;
   }

   public void setUseMaskedPassword(boolean useMaskedPassword)
   {
      this.useMaskedPassword = useMaskedPassword;
   }

   public String getPasswordCodec()
   {
      return passwordCodec;
   }

   public void setPasswordCodec(String codecs)
   {
      passwordCodec = codecs;
   }

   @Override
   public String toString()
   {
      return "HornetQRAProperties[localTx=" + localTx +
         ", userName=" + userName + ", password=****]";
   }

   public synchronized void init() throws HornetQException
   {
      if (initialized)
         return;

      if (useMaskedPassword)
      {
         codecInstance = new DefaultSensitiveStringCodec();

         if (passwordCodec != null)
         {
            codecInstance = PasswordMaskingUtil.getCodec(passwordCodec);
         }

         try
         {
            if (password != null)
            {
               password = codecInstance.decode(password);
            }
         }
         catch (Exception e)
         {
            throw HornetQRABundle.BUNDLE.errorDecodingPassword(e);
         }

      }
      initialized = true;
   }

   public SensitiveDataCodec<String> getCodecInstance()
   {
      return codecInstance;
   }

   public String getJgroupsChannelLocatorClass()
   {
      return jgroupsChannelLocatorClass;
   }

   public void setJgroupsChannelLocatorClass(String jgroupsChannelLocatorClass)
   {
      this.jgroupsChannelLocatorClass = jgroupsChannelLocatorClass;
   }

   public String getJgroupsChannelRefName()
   {
      return jgroupsChannelRefName;
   }

   public void setJgroupsChannelRefName(String jgroupsChannelRefName)
   {
      this.jgroupsChannelRefName = jgroupsChannelRefName;
   }

}
