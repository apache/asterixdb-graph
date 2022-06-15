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
package org.apache.asterix.graphix.lang.rewrites.lower;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.lang.common.struct.VarIdentifier;

/**
 * Lookup table for JOIN and ITERATION aliases, indexed by their representative (i.e. element) variable identifiers.
 */
public class LoweringAliasLookupTable {
    private final Map<VarIdentifier, VarIdentifier> joinAliasMap = new HashMap<>();
    private final Map<VarIdentifier, VarIdentifier> iterationAliasMap = new HashMap<>();

    public void putJoinAlias(VarIdentifier elementVariable, VarIdentifier aliasVariable) {
        joinAliasMap.put(elementVariable, aliasVariable);
    }

    public void putIterationAlias(VarIdentifier elementVariable, VarIdentifier aliasVariable) {
        iterationAliasMap.put(elementVariable, aliasVariable);
    }

    public VarIdentifier getJoinAlias(VarIdentifier elementVariable) {
        return joinAliasMap.get(elementVariable);
    }

    public VarIdentifier getIterationAlias(VarIdentifier elementVariable) {
        return iterationAliasMap.get(elementVariable);
    }

    public void reset() {
        joinAliasMap.clear();
        iterationAliasMap.clear();
    }
}
