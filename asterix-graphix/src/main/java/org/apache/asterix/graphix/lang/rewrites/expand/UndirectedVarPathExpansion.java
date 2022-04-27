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
package org.apache.asterix.graphix.lang.rewrites.expand;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;

/**
 * Given a path of variable length, there are {@link EdgeDescriptor#getMaximumHops()} -
 * {@link EdgeDescriptor#getMinimumHops()} valid paths that could appear in our result. Generate a fixed sub-path for
 * each of the aforementioned paths and decompose according to {@link UndirectedFixedPathExpansion}.
 *
 * @see UndirectedFixedPathExpansion
 */
public class UndirectedVarPathExpansion implements IEdgePatternExpansion {
    private final PathEnumerationEnvironment pathEnumerationEnvironment;

    public UndirectedVarPathExpansion(PathEnumerationEnvironment pathEnumerationEnvironment) {
        this.pathEnumerationEnvironment = pathEnumerationEnvironment;
    }

    @Override
    public Iterable<Iterable<EdgePatternExpr>> expand(EdgePatternExpr edgePatternExpr) {
        // Ensure we have been given the correct type of sub-path.
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        if (edgeDescriptor.getPatternType() != EdgeDescriptor.PatternType.PATH
                || edgeDescriptor.getEdgeDirection() != EdgeDescriptor.EdgeDirection.UNDIRECTED
                || Objects.equals(edgeDescriptor.getMinimumHops(), edgeDescriptor.getMaximumHops())) {
            throw new IllegalArgumentException("Expected an undirected var sub-path, but given a " + edgeDescriptor);
        }

        List<List<EdgePatternExpr>> decomposedEdgeList = new ArrayList<>();
        for (int i = edgeDescriptor.getMinimumHops(); i <= edgeDescriptor.getMaximumHops(); i++) {
            EdgeDescriptor fixedEdgeDescriptor =
                    new EdgeDescriptor(EdgeDescriptor.EdgeDirection.UNDIRECTED, EdgeDescriptor.PatternType.PATH,
                            edgeDescriptor.getEdgeLabels(), edgeDescriptor.getVariableExpr(), i, i);
            EdgePatternExpr fixedEdgePattern = new EdgePatternExpr(edgePatternExpr.getLeftVertex(),
                    edgePatternExpr.getRightVertex(), fixedEdgeDescriptor);
            fixedEdgePattern.replaceInternalVertices(edgePatternExpr.getInternalVertices());

            // We defer the decomposition of each individual path to UndirectedFixedPathExpansion.
            new UndirectedFixedPathExpansion(pathEnumerationEnvironment).expand(fixedEdgePattern).forEach(e -> {
                List<EdgePatternExpr> decompositionIntermediate = new ArrayList<>();
                e.forEach(decompositionIntermediate::add);
                decomposedEdgeList.add(decompositionIntermediate);
            });
        }

        return new ArrayList<>(decomposedEdgeList);
    }
}
