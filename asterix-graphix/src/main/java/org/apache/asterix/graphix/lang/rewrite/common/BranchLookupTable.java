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
package org.apache.asterix.graphix.lang.rewrite.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;

/**
 * Lookup table for branch (i.e. partial {@link EdgePatternExpr} instances that define FA transition functions)--
 * indexed by {@link EdgePatternExpr} instances.
 */
public class BranchLookupTable {
    private final Map<EdgePatternExpr, List<EdgePatternExpr>> branchEdgeMap = new HashMap<>();

    public void putBranch(EdgePatternExpr associatedEdge, EdgePatternExpr branch) {
        branchEdgeMap.putIfAbsent(associatedEdge, new ArrayList<>());
        branchEdgeMap.get(associatedEdge).add(branch);
    }

    public void putBranch(EdgePatternExpr associatedEdge, PathPatternExpr branch) {
        branchEdgeMap.putIfAbsent(associatedEdge, new ArrayList<>());
        branchEdgeMap.get(associatedEdge).addAll(branch.getEdgeExpressions());
    }

    public List<EdgePatternExpr> getBranches(EdgePatternExpr associatedEdge) {
        return branchEdgeMap.get(associatedEdge);
    }
}
