/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.asterix.graphix.test;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.hyracks.test.support.TestUtils;
import org.apache.hyracks.util.file.FileUtil;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class GraphixIntegrationUtil extends AsterixHyracksIntegrationUtil {
    private static final String FILE_DEFAULT_CONF =
            FileUtil.joinPath("asterixdb", "asterix-opt", "asterix-graphix", "src", "main", "resources", "cc.conf");

    public static void main(String[] args) {
        TestUtils.redirectLoggingToConsole();
        GraphixIntegrationUtil graphixIntegrationUtil = new GraphixIntegrationUtil();
        try {
            boolean cleanupOnStart = Boolean.getBoolean("cleanup.start");
            boolean cleanupOnShutdown = Boolean.getBoolean("cleanup.shutdown");
            String confFile = System.getProperty("conf.path", FILE_DEFAULT_CONF);
            graphixIntegrationUtil.run(cleanupOnStart, cleanupOnShutdown, confFile);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
