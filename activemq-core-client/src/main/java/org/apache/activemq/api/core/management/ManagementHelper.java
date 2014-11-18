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
package org.apache.activemq.api.core.management;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.activemq.api.core.Message;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.client.ActiveMQClientMessageBundle;
import org.apache.activemq.utils.json.JSONArray;
import org.apache.activemq.utils.json.JSONObject;

/**
 * Helper class to use ActiveMQ Core messages to manage server resources.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public final class ManagementHelper
{
   // Constants -----------------------------------------------------

   public static final SimpleString HDR_RESOURCE_NAME = new SimpleString("_HQ_ResourceName");

   public static final SimpleString HDR_ATTRIBUTE = new SimpleString("_HQ_Attribute");

   public static final SimpleString HDR_OPERATION_NAME = new SimpleString("_HQ_OperationName");

   public static final SimpleString HDR_OPERATION_SUCCEEDED = new SimpleString("_HQ_OperationSucceeded");

   public static final SimpleString HDR_NOTIFICATION_TYPE = new SimpleString("_HQ_NotifType");

   public static final SimpleString HDR_NOTIFICATION_TIMESTAMP = new SimpleString("_HQ_NotifTimestamp");

   public static final SimpleString HDR_ROUTING_NAME = new SimpleString("_HQ_RoutingName");

   public static final SimpleString HDR_CLUSTER_NAME = new SimpleString("_HQ_ClusterName");

   public static final SimpleString HDR_ADDRESS = new SimpleString("_HQ_Address");

   public static final SimpleString HDR_BINDING_ID = new SimpleString("_HQ_Binding_ID");

   public static final SimpleString HDR_BINDING_TYPE = new SimpleString("_HQ_Binding_Type");

   public static final SimpleString HDR_FILTERSTRING = new SimpleString("_HQ_FilterString");

   public static final SimpleString HDR_DISTANCE = new SimpleString("_HQ_Distance");

   public static final SimpleString HDR_CONSUMER_COUNT = new SimpleString("_HQ_ConsumerCount");

   public static final SimpleString HDR_USER = new SimpleString("_HQ_User");

   public static final SimpleString HDR_CHECK_TYPE = new SimpleString("_HQ_CheckType");

   public static final SimpleString HDR_SESSION_NAME = new SimpleString("_HQ_SessionName");

   public static final SimpleString HDR_REMOTE_ADDRESS = new SimpleString("_HQ_RemoteAddress");

   public static final SimpleString HDR_PROPOSAL_GROUP_ID = new SimpleString("_JBM_ProposalGroupId");

   public static final SimpleString HDR_PROPOSAL_VALUE = new SimpleString("_JBM_ProposalValue");

   public static final SimpleString HDR_PROPOSAL_ALT_VALUE = new SimpleString("_JBM_ProposalAltValue");

   public static final SimpleString HDR_CONSUMER_NAME = new SimpleString("_HQ_ConsumerName");

   public static final SimpleString HDR_CONNECTION_NAME = new SimpleString("_HQ_ConnectionName");

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   /**
    * Stores a resource attribute in a message to retrieve the value from the server resource.
    *
    * @param message      message
    * @param resourceName the name of the resource
    * @param attribute    the name of the attribute
    * @see ResourceNames
    */
   public static void putAttribute(final Message message, final String resourceName, final String attribute)
   {
      message.putStringProperty(ManagementHelper.HDR_RESOURCE_NAME, new SimpleString(resourceName));
      message.putStringProperty(ManagementHelper.HDR_ATTRIBUTE, new SimpleString(attribute));
   }

   /**
    * Stores a operation invocation in a message to invoke the corresponding operation the value from the server resource.
    *
    * @param message       message
    * @param resourceName  the name of the resource
    * @param operationName the name of the operation to invoke on the resource
    * @see ResourceNames
    */
   public static void putOperationInvocation(final Message message,
                                             final String resourceName,
                                             final String operationName) throws Exception
   {
      ManagementHelper.putOperationInvocation(message, resourceName, operationName, (Object[]) null);
   }

   /**
    * Stores a operation invocation in a  message to invoke the corresponding operation the value from the server resource.
    *
    * @param message       message
    * @param resourceName  the name of the server resource
    * @param operationName the name of the operation to invoke on the server resource
    * @param parameters    the parameters to use to invoke the server resource
    * @see ResourceNames
    */
   public static void putOperationInvocation(final Message message,
                                             final String resourceName,
                                             final String operationName,
                                             final Object... parameters) throws Exception
   {
      // store the name of the operation in the headers
      message.putStringProperty(ManagementHelper.HDR_RESOURCE_NAME, new SimpleString(resourceName));
      message.putStringProperty(ManagementHelper.HDR_OPERATION_NAME, new SimpleString(operationName));

      // and the params go in the body, since might be too large for header

      String paramString;

      if (parameters != null)
      {
         JSONArray jsonArray = ManagementHelper.toJSONArray(parameters);

         paramString = jsonArray.toString();
      }
      else
      {
         paramString = null;
      }

      message.getBodyBuffer().writeNullableSimpleString(SimpleString.toSimpleString(paramString));
   }

   private static JSONArray toJSONArray(final Object[] array) throws Exception
   {
      JSONArray jsonArray = new JSONArray();

      for (Object parameter : array)
      {
         if (parameter instanceof Map)
         {
            Map<String, Object> map = (Map<String, Object>) parameter;

            JSONObject jsonObject = new JSONObject();

            for (Map.Entry<String, Object> entry : map.entrySet())
            {
               String key = entry.getKey();

               Object val = entry.getValue();

               if (val != null)
               {
                  if (val.getClass().isArray())
                  {
                     val = ManagementHelper.toJSONArray((Object[]) val);
                  }
                  else
                  {
                     ManagementHelper.checkType(val);
                  }
               }

               jsonObject.put(key, val);
            }

            jsonArray.put(jsonObject);
         }
         else
         {
            if (parameter != null)
            {
               Class<?> clz = parameter.getClass();

               if (clz.isArray())
               {
                  Object[] innerArray = (Object[]) parameter;

                  jsonArray.put(ManagementHelper.toJSONArray(innerArray));
               }
               else
               {
                  ManagementHelper.checkType(parameter);

                  jsonArray.put(parameter);
               }
            }
            else
            {
               jsonArray.put((Object) null);
            }
         }
      }

      return jsonArray;
   }

   private static Object[] fromJSONArray(final JSONArray jsonArray) throws Exception
   {
      Object[] array = new Object[jsonArray.length()];

      for (int i = 0; i < jsonArray.length(); i++)
      {
         Object val = jsonArray.get(i);

         if (val instanceof JSONArray)
         {
            Object[] inner = ManagementHelper.fromJSONArray((JSONArray) val);

            array[i] = inner;
         }
         else if (val instanceof JSONObject)
         {
            JSONObject jsonObject = (JSONObject) val;

            Map<String, Object> map = new HashMap<String, Object>();

            Iterator<String> iter = jsonObject.keys();

            while (iter.hasNext())
            {
               String key = iter.next();

               Object innerVal = jsonObject.get(key);

               if (innerVal instanceof JSONArray)
               {
                  innerVal = ManagementHelper.fromJSONArray(((JSONArray) innerVal));
               }
               else if (innerVal instanceof JSONObject)
               {
                  Map<String, Object> innerMap = new HashMap<String, Object>();
                  JSONObject o = (JSONObject) innerVal;
                  Iterator it = o.keys();
                  while (it.hasNext())
                  {
                     String k = (String) it.next();
                     innerMap.put(k, o.get(k));
                  }
                  innerVal = innerMap;
               }
               else if (innerVal instanceof Integer)
               {
                  innerVal = ((Integer) innerVal).longValue();
               }

               map.put(key, innerVal);
            }

            array[i] = map;
         }
         else
         {
            if (val == JSONObject.NULL)
            {
               array[i] = null;
            }
            else
            {
               array[i] = val;
            }
         }
      }

      return array;
   }

   private static void checkType(final Object param)
   {
      if (param instanceof Integer == false && param instanceof Long == false &&
         param instanceof Double == false &&
         param instanceof String == false &&
         param instanceof Boolean == false &&
         param instanceof Map == false &&
         param instanceof Byte == false &&
         param instanceof Short == false)
      {
         throw ActiveMQClientMessageBundle.BUNDLE.invalidManagementParam(param.getClass().getName());
      }
   }

   /**
    * Used by ActiveMQ management service.
    */
   public static Object[] retrieveOperationParameters(final Message message) throws Exception
   {
      SimpleString sstring = message.getBodyBuffer().readNullableSimpleString();
      String jsonString = (sstring == null) ? null : sstring.toString();

      if (jsonString != null)
      {
         JSONArray jsonArray = new JSONArray(jsonString);

         return ManagementHelper.fromJSONArray(jsonArray);
      }
      else
      {
         return null;
      }
   }

   /**
    * Returns whether the JMS message corresponds to the result of a management operation invocation.
    */
   public static boolean isOperationResult(final Message message)
   {
      return message.containsProperty(ManagementHelper.HDR_OPERATION_SUCCEEDED);
   }

   /**
    * Returns whether the JMS message corresponds to the result of a management attribute value.
    */
   public static boolean isAttributesResult(final Message message)
   {
      return !ManagementHelper.isOperationResult(message);
   }

   /**
    * Used by ActiveMQ management service.
    */
   public static void storeResult(final Message message, final Object result) throws Exception
   {
      String resultString;

      if (result != null)
      {
         // Result is stored in body, also encoded as JSON array of length 1

         JSONArray jsonArray = ManagementHelper.toJSONArray(new Object[]{result});

         resultString = jsonArray.toString();
      }
      else
      {
         resultString = null;
      }

      message.getBodyBuffer().writeNullableSimpleString(SimpleString.toSimpleString(resultString));
   }

   /**
    * Returns the result of an operation invocation or an attribute value.
    * <br>
    * If an error occurred on the server, {@link #hasOperationSucceeded(Message)} will return {@code false}.
    * and the result will be a String corresponding to the server exception.
    */
   public static Object[] getResults(final Message message) throws Exception
   {
      SimpleString sstring = message.getBodyBuffer().readNullableSimpleString();
      String jsonString = (sstring == null) ? null : sstring.toString();

      if (jsonString != null)
      {
         JSONArray jsonArray = new JSONArray(jsonString);

         Object[] res = ManagementHelper.fromJSONArray(jsonArray);

         return res;
      }
      else
      {
         return null;
      }
   }

   /**
    * Returns the result of an operation invocation or an attribute value.
    * <br>
    * If an error occurred on the server, {@link #hasOperationSucceeded(Message)} will return {@code false}.
    * and the result will be a String corresponding to the server exception.
    */
   public static Object getResult(final Message message) throws Exception
   {
      Object[] res = ManagementHelper.getResults(message);

      if (res != null)
      {
         return res[0];
      }
      else
      {
         return null;
      }
   }

   /**
    * Returns whether the invocation of the management operation on the server resource succeeded.
    */
   public static boolean hasOperationSucceeded(final Message message)
   {
      if (!ManagementHelper.isOperationResult(message))
      {
         return false;
      }
      if (message.containsProperty(ManagementHelper.HDR_OPERATION_SUCCEEDED))
      {
         return message.getBooleanProperty(ManagementHelper.HDR_OPERATION_SUCCEEDED);
      }
      return false;
   }

   /**
    * Used by ActiveMQ management service.
    */
   public static Map<String, Object> fromCommaSeparatedKeyValues(final String str) throws Exception
   {
      if (str == null || str.trim().length() == 0)
      {
         return Collections.emptyMap();
      }

      // create a JSON array with 1 object:
      JSONArray array = new JSONArray("[{" + str + "}]");
      Map<String, Object> params = (Map<String, Object>) ManagementHelper.fromJSONArray(array)[0];
      return params;
   }

   /**
    * Used by ActiveMQ management service.
    */
   public static Object[] fromCommaSeparatedArrayOfCommaSeparatedKeyValues(final String str) throws Exception
   {
      if (str == null || str.trim().length() == 0)
      {
         return new Object[0];
      }

      String s = str;

      // if there is a single item, we wrap it in to make it a JSON object
      if (!s.trim().startsWith("{"))
      {
         s = "{" + s + "}";
      }
      JSONArray array = new JSONArray("[" + s + "]");
      return ManagementHelper.fromJSONArray(array);
   }

   private ManagementHelper()
   {
   }
}
