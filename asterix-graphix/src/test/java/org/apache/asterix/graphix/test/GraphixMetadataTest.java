/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.graphix.test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.hyracks.util.file.FileUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class GraphixMetadataTest {
    private static final String PATH_ACTUAL = FileUtil.joinPath("target", "mdtest");
    private static final String PATH_BASE = FileUtil.joinPath("src", "test", "resources", "metadatats");
    private static final String FILE_TEST_CONFIG = FileUtil.joinPath("src", "test", "resources", "cc.conf");

    private static final TestExecutor testExecutor = new TestExecutor();
    private static final GraphixIntegrationUtil integrationUtil = new GraphixIntegrationUtil();

    private final TestCaseContext tcCtx;

    public GraphixMetadataTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        //noinspection ResultOfMethodCallIgnored
        new File(PATH_ACTUAL).mkdirs();
        integrationUtil.init(true, FILE_TEST_CONFIG);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        integrationUtil.deinit(true);
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            //noinspection ResultOfMethodCallIgnored
            outdir.delete();
        }
    }

    @Parameters(name = "MetadataTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = new ArrayList<>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE))) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;
    }

    @Test
    public void test() throws Exception {
        testExecutor.executeTest(PATH_ACTUAL, tcCtx, null, false);
    }
}
