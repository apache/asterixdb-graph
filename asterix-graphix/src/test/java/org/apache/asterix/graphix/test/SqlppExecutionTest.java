/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.graphix.test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.runtime.ExecutionTestUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.xml.TestGroup;
import org.apache.hyracks.util.file.FileUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SqlppExecutionTest {
    protected static final String PATH_ACTUAL = FileUtil.joinPath("target", "srttest");
    protected static final String PATH_BASE = FileUtil.joinPath("src", "test", "resources", "runtimets");
    private static final String TEST_SUITE_FILE = "testsuite_sqlpp.xml";
    private static final String ONLY_SUITE_FILE = "only_sqlpp.xml";

    // We should be in the "asterix-app" directory.
    protected static final String FILE_TEST_CONFIG =
            FileUtil.joinPath("..", "asterix-opt", "asterix-graphix", "src", "main", "resources", "cc.conf");

    private static final GraphixIntegrationUtil integrationUtil = new GraphixIntegrationUtil();
    private static final TestExecutor testExecutor = new TestExecutor();

    protected static TestGroup FailedGroup;
    protected TestCaseContext tcCtx;

    public SqlppExecutionTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        //noinspection ResultOfMethodCallIgnored
        new File(PATH_ACTUAL).mkdirs();
        ExecutionTestUtil.setUp(true, FILE_TEST_CONFIG, integrationUtil, false, null);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        ExecutionTestUtil.tearDown(true, integrationUtil, true);
        integrationUtil.removeTestStorageFiles();
    }

    @Parameters(name = "SqlppExecutionTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        // TODO (GLENN): Find out the SQLPP tests we can run here.
        // Collection<Object[]> test_cases = buildTestsInXml(ONLY_SUITE_FILE);
        // if (test_cases.size() == 0) {
        //    test_cases = buildTestsInXml(TEST_SUITE_FILE);
        // }
        // return test_cases;
        return Collections.emptyList();
    }

    protected static Collection<Object[]> buildTestsInXml(String xmlfile) throws Exception {
        Collection<Object[]> testArgs = new ArrayList<>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE), xmlfile)) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;
    }

    @Test
    public void test() throws Exception {
        testExecutor.executeTest(PATH_ACTUAL, tcCtx, null, false, FailedGroup);
    }
}
