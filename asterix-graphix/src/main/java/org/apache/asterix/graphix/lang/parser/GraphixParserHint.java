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
package org.apache.asterix.graphix.lang.parser;

import org.apache.asterix.graphix.algebra.compiler.option.ElementEvaluationOption;

/**
 * Graphix SQL++ specific hints. Note that this is not an extension of the SQL++ hint class:
 * {@link org.apache.asterix.lang.sqlpp.parser.SqlppHint}, so callers must use their own facilities for hint finding.
 */
public enum GraphixParserHint {
    EXPAND_AND_UNION_HINT(ElementEvaluationOption.EXPAND_AND_UNION.getOptionValue()),
    SWITCH_AND_CYCLE_HINT(ElementEvaluationOption.SWITCH_AND_CYCLE.getOptionValue());

    private final String id;

    GraphixParserHint(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return getId();
    }
}
