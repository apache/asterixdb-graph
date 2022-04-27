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
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;

/**
 * Given a sub-path that is **not** {@link EdgeDescriptor.EdgeDirection#UNDIRECTED} but is fixed in the number of hops
 * (denoted N), we build a single set of N directed simple edges.
 */
public class DirectedFixedPathExpansion implements IEdgePatternExpansion {
    private final PathEnumerationEnvironment pathEnumerationEnvironment;

    public DirectedFixedPathExpansion(PathEnumerationEnvironment pathEnumerationEnvironment) {
        this.pathEnumerationEnvironment = pathEnumerationEnvironment;
    }

    @Override
    public Iterable<Iterable<EdgePatternExpr>> expand(EdgePatternExpr edgePatternExpr) {
        // Ensure we have been given the correct type of sub-path.
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        if (edgeDescriptor.getPatternType() != EdgeDescriptor.PatternType.PATH
                || edgeDescriptor.getEdgeDirection() == EdgeDescriptor.EdgeDirection.UNDIRECTED
                || !Objects.equals(edgeDescriptor.getMinimumHops(), edgeDescriptor.getMaximumHops())) {
            throw new IllegalArgumentException("Expected a directed fixed sub-path, but given a " + edgeDescriptor);
        }

        pathEnumerationEnvironment.reset();
        List<EdgePatternExpr> decomposedEdgeList = new ArrayList<>();
        VertexPatternExpr workingLeftVertex = edgePatternExpr.getLeftVertex();
        for (int i = 0; i < edgePatternExpr.getEdgeDescriptor().getMinimumHops(); i++) {
            VertexPatternExpr rightVertex;
            if (i == edgePatternExpr.getEdgeDescriptor().getMinimumHops() - 1) {
                // This is the final vertex in our path.
                rightVertex = edgePatternExpr.getRightVertex();

            } else {
                // We need to generate an intermediate vertex.
                rightVertex = new VertexPatternExpr(pathEnumerationEnvironment.getNewVertexVar(),
                        edgePatternExpr.getInternalVertices().get(i).getLabels());
            }

            // Build our EDGE-PATTERN-EXPR.
            EdgeDescriptor newEdgeDescriptor =
                    new EdgeDescriptor(edgePatternExpr.getEdgeDescriptor().getEdgeDirection(),
                            EdgeDescriptor.PatternType.EDGE, edgePatternExpr.getEdgeDescriptor().getEdgeLabels(),
                            pathEnumerationEnvironment.getNewEdgeVar(), null, null);
            EdgePatternExpr newEdgePattern = new EdgePatternExpr(workingLeftVertex, rightVertex, newEdgeDescriptor);
            decomposedEdgeList.add(newEdgePattern);

            // Build the associated path record (for our environment to hold).
            pathEnumerationEnvironment.buildPathRecord(workingLeftVertex.getVariableExpr(),
                    newEdgeDescriptor.getVariableExpr(), rightVertex.getVariableExpr());

            // Move our "vertex cursor".
            workingLeftVertex = rightVertex;
        }

        // We do not have any undirected edges. Return a singleton list with the EDGE-PATTERN-EXPRs above.
        pathEnumerationEnvironment.publishIsomorphism();
        pathEnumerationEnvironment.publishRebindings();
        return List.of(decomposedEdgeList);
    }
}
