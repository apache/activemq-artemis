package org.apache.activemq.artemis.tests.servercompatibility.base;

import java.io.File;

@SuppressWarnings("unused")
public interface CompatibilityTestScriptInterface {

    File getWorkingDir();

    String getSide();

}
