/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.utils.critical;

import org.apache.activemq.artemis.utils.ArtemisCloseable;

public interface CriticalCloseable extends ArtemisCloseable {


   /** This will set something to be called right before closing.
    *
    * The use case that drove this call was a ReadWriteLock on the journal.
    * Imagine that you need to call enterCritical, readWrite.lock() and then unlock and leaveCritical.
    * By using this call I could reuse the same instance on the readWriteLock. */
   void beforeClose(ArtemisCloseable otherCloseable);
}
