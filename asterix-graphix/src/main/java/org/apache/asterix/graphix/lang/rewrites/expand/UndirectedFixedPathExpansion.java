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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.IGraphExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.lang.common.expression.VariableExpr;

/**
 * Given a sub-path that is {@link EdgeDescriptor.EdgeType#UNDIRECTED} but is fixed in the number of hops (denoted N),
 * we generate N sets of directed simple edges (the total of which is equal to 2^N).
 */
public class UndirectedFixedPathExpansion implements IEdgePatternExpansion {
    private final PathEnumerationEnvironment pathEnumerationEnvironment;

    public UndirectedFixedPathExpansion(PathEnumerationEnvironment pathEnumerationEnvironment) {
        this.pathEnumerationEnvironment = pathEnumerationEnvironment;
    }

    @Override
    public Iterable<Iterable<EdgePatternExpr>> expand(EdgePatternExpr edgePatternExpr) {
        // Ensure we have been given the correct type of sub-path.
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        if (edgeDescriptor.getEdgeClass() != IGraphExpr.GraphExprKind.PATH_PATTERN
                || edgeDescriptor.getEdgeType() != EdgeDescriptor.EdgeType.UNDIRECTED
                || !Objects.equals(edgeDescriptor.getMinimumHops(), edgeDescriptor.getMaximumHops())) {
            throw new IllegalArgumentException("Expected an undirected fixed sub-path, but given a " + edgeDescriptor);
        }
        pathEnumerationEnvironment.reset();

        // The number of items in the final state of our queue will be 2^{number of hops}.
        Deque<List<EdgePatternExpr>> decompositionQueue = new ArrayDeque<>();
        decompositionQueue.addLast(new ArrayList<>());
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

            VariableExpr graphEdgeVar = pathEnumerationEnvironment.getNewEdgeVar();
            List<List<EdgePatternExpr>> visitedEdgeLists = new ArrayList<>();
            while (!decompositionQueue.isEmpty()) {
                // Pull from our queue...
                List<EdgePatternExpr> workingEdgePatternList = decompositionQueue.removeLast();

                // ...and generate two new variants that have an additional LEFT_TO_RIGHT edge...
                EdgeDescriptor leftToRightEdgeDescriptor =
                        new EdgeDescriptor(EdgeDescriptor.EdgeType.LEFT_TO_RIGHT, IGraphExpr.GraphExprKind.EDGE_PATTERN,
                                edgePatternExpr.getEdgeDescriptor().getEdgeLabels(), graphEdgeVar, null, null);
                List<EdgePatternExpr> leftToRightList = new ArrayList<>(workingEdgePatternList);
                leftToRightList.add(new EdgePatternExpr(workingLeftVertex, rightVertex, leftToRightEdgeDescriptor));
                visitedEdgeLists.add(leftToRightList);

                // ...and a RIGHT_TO_LEFT edge.
                EdgeDescriptor rightToLeftEdgeDescriptor =
                        new EdgeDescriptor(EdgeDescriptor.EdgeType.RIGHT_TO_LEFT, IGraphExpr.GraphExprKind.EDGE_PATTERN,
                                edgePatternExpr.getEdgeDescriptor().getEdgeLabels(), graphEdgeVar, null, null);
                List<EdgePatternExpr> rightToLeftList = new ArrayList<>(workingEdgePatternList);
                rightToLeftList.add(new EdgePatternExpr(workingLeftVertex, rightVertex, rightToLeftEdgeDescriptor));
                visitedEdgeLists.add(rightToLeftList);
            }
            decompositionQueue.addAll(visitedEdgeLists);

            // Build the associated path record (for our environment to hold).
            pathEnumerationEnvironment.buildPathRecord(workingLeftVertex.getVariableExpr(), graphEdgeVar,
                    rightVertex.getVariableExpr());

            // Move our "vertex cursor".
            workingLeftVertex = rightVertex;
        }

        // Publish our isomorphism and rebindings.
        for (List<EdgePatternExpr> ignored : decompositionQueue) {
            pathEnumerationEnvironment.publishIsomorphism();
            pathEnumerationEnvironment.publishRebindings();
        }
        return new ArrayList<>(decompositionQueue);
    }
}
