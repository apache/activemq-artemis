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
package org.apache.activemq.rest.queue.push;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.rest.HornetQRestLogger;
import org.apache.activemq.rest.queue.push.xml.PushRegistration;
import org.apache.activemq.rest.topic.PushTopicRegistration;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class FilePushStore implements PushStore
{
   protected Map<String, PushRegistration> map = new HashMap<String, PushRegistration>();
   protected File dir;
   protected JAXBContext ctx;

   public FilePushStore(String dirname) throws Exception
   {
      this.dir = new File(dirname);
      this.ctx = JAXBContext.newInstance(PushRegistration.class, PushTopicRegistration.class);
      if (this.dir.exists())
      {
         HornetQRestLogger.LOGGER.loadingRestStore(dir.getAbsolutePath());
         for (File file : this.dir.listFiles())
         {
            if (!file.isFile()) continue;
            PushRegistration reg = null;
            try
            {
               reg = (PushRegistration)ctx.createUnmarshaller().unmarshal(file);
               reg.setLoadedFrom(file);
               HornetQRestLogger.LOGGER.addingPushRegistration(reg.getId());
               map.put(reg.getId(), reg);
            }
            catch (Exception e)
            {
               HornetQRestLogger.LOGGER.errorLoadingStore(e, file.getName());
            }
         }
      }
   }

   public synchronized List<PushRegistration> getRegistrations()
   {
      List<PushRegistration> list = new ArrayList<PushRegistration>(map.values());
      return list;
   }

   public synchronized List<PushRegistration> getByDestination(String destination)
   {
      List<PushRegistration> list = new ArrayList<PushRegistration>();
      for (PushRegistration reg : map.values())
      {
         if (reg.getDestination().equals(destination))
         {
            list.add(reg);
         }
      }
      return list;
   }

   public synchronized void update(PushRegistration reg) throws Exception
   {
      if (reg.getLoadedFrom() == null) return;
      save(reg);
   }

   protected void save(PushRegistration reg) throws JAXBException
   {
      Marshaller marshaller = ctx.createMarshaller();
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
      marshaller.marshal(reg, (File)reg.getLoadedFrom());
   }

   public synchronized void add(PushRegistration reg) throws Exception
   {
      map.put(reg.getId(), reg);
      if (!this.dir.exists()) this.dir.mkdirs();
      File fp = new File(dir, "reg-" + reg.getId() + ".xml");
      reg.setLoadedFrom(fp);
      //System.out.println("******* Saving: " + fp.getAbsolutePath());
      save(reg);
   }

   public synchronized void remove(PushRegistration reg) throws Exception
   {
      map.remove(reg.getId());
      if (reg.getLoadedFrom() == null) return;
      File fp = (File)reg.getLoadedFrom();
      fp.delete();
   }

   public synchronized void removeAll() throws Exception
   {
      ArrayList<PushRegistration> copy = new ArrayList<PushRegistration>(map.values());
      for (PushRegistration reg : copy) remove(reg);
      this.dir.delete();
   }
}
