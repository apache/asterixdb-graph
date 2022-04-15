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

import java.util.List;

import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.IGraphExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;

/**
 * Given an undirected edge, generate two edges (to feed to the caller's UNION): one edge directed from
 * {@link EdgeDescriptor.EdgeType#LEFT_TO_RIGHT},and another edge directed from
 * {@link EdgeDescriptor.EdgeType#RIGHT_TO_LEFT}.
 */
public class UndirectedEdgeExpansion implements IEdgePatternExpansion {
    @Override
    public Iterable<Iterable<EdgePatternExpr>> expand(EdgePatternExpr edgePatternExpr) {
        // Ensure we have been given the correct type of edge.
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        if (edgeDescriptor.getEdgeClass() != IGraphExpr.GraphExprKind.EDGE_PATTERN
                || edgeDescriptor.getEdgeType() != EdgeDescriptor.EdgeType.UNDIRECTED) {
            throw new IllegalArgumentException("Expected an undirected edge, but given a " + edgeDescriptor);
        }
        VertexPatternExpr leftVertex = edgePatternExpr.getLeftVertex();
        VertexPatternExpr rightVertex = edgePatternExpr.getRightVertex();

        // Build our LEFT_TO_RIGHT edge...
        EdgeDescriptor leftToRightEdgeDescriptor =
                new EdgeDescriptor(EdgeDescriptor.EdgeType.LEFT_TO_RIGHT, IGraphExpr.GraphExprKind.EDGE_PATTERN,
                        edgeDescriptor.getEdgeLabels(), edgeDescriptor.getVariableExpr(), null, null);
        EdgePatternExpr leftToRightEdge = new EdgePatternExpr(leftVertex, rightVertex, leftToRightEdgeDescriptor);

        // ...and our RIGHT_TO_LEFT edge...
        EdgeDescriptor rightToLeftEdgeDescriptor =
                new EdgeDescriptor(EdgeDescriptor.EdgeType.RIGHT_TO_LEFT, IGraphExpr.GraphExprKind.EDGE_PATTERN,
                        edgeDescriptor.getEdgeLabels(), edgeDescriptor.getVariableExpr(), null, null);
        EdgePatternExpr rightToLeftEdge = new EdgePatternExpr(leftVertex, rightVertex, rightToLeftEdgeDescriptor);

        // Return two singleton lists (these should be UNION-ALLed).
        return List.of(List.of(leftToRightEdge), List.of(rightToLeftEdge));
    }
}
