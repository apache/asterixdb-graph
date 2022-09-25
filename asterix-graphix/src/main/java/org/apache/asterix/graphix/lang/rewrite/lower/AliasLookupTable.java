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
package org.apache.asterix.graphix.lang.rewrite.lower;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.rewrite.visitor.GraphixDeepCopyVisitor;
import org.apache.asterix.lang.common.expression.VariableExpr;

/**
 * Lookup table for JOIN and ITERATION aliases, indexed by their representative (i.e. element) variables.
 */
public class AliasLookupTable {
    private final GraphixDeepCopyVisitor deepCopyVisitor = new GraphixDeepCopyVisitor();
    private final Map<VariableExpr, VariableExpr> joinAliasMap = new HashMap<>();
    private final Map<VariableExpr, VariableExpr> iterationAliasMap = new HashMap<>();

    public void addJoinAlias(VariableExpr elementVariable, VariableExpr aliasVariable) {
        joinAliasMap.put(elementVariable, aliasVariable);
    }

    public void addIterationAlias(VariableExpr elementVariable, VariableExpr aliasVariable) {
        iterationAliasMap.put(elementVariable, aliasVariable);
    }

    public VariableExpr getJoinAlias(VariableExpr elementVariable) throws CompilationException {
        if (joinAliasMap.containsKey(elementVariable)) {
            return deepCopyVisitor.visit(joinAliasMap.get(elementVariable), null);
        }
        return null;
    }

    public VariableExpr getIterationAlias(VariableExpr elementVariable) throws CompilationException {
        if (iterationAliasMap.containsKey(elementVariable)) {
            return deepCopyVisitor.visit(iterationAliasMap.get(elementVariable), null);
        }
        return null;
    }

    public void reset() {
        joinAliasMap.clear();
        iterationAliasMap.clear();
    }
}
