/*
 * <p>
 * All rights reserved. Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.jms.example;

import java.io.File;

import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;

public class TestServer {
	public static void main(String[] args) throws Exception {
		File configFolder = new File(args.length > 0 ? args[0] : "src/main/resources/").getAbsoluteFile();
		System.setProperty("java.security.auth.login.config", new File(configFolder, "login.conf").getAbsolutePath());
		EmbeddedActiveMQ embedded = new EmbeddedActiveMQ();
		embedded.setConfigResourcePath(new File(configFolder, "broker.xml").getAbsoluteFile().toURI().toASCIIString());
		embedded.start();
		while (true)
			;

	}
}
