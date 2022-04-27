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
package org.apache.asterix.graphix.lang.rewrites.resolve;

import java.util.Set;
import java.util.function.BiFunction;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.visitor.LabelConsistencyVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.QueryKnowledgeVisitor;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;

/**
 * Recursively attempt to resolve any element labels / edge directions in a FROM-GRAPH-CLAUSE.
 */
public class InferenceBasedResolver implements IGraphElementResolver {
    public static final String METADATA_CONFIG_NAME = "inference-based";

    private final QueryKnowledgeVisitor queryKnowledgeVisitor;
    private final SchemaKnowledgeTable schemaKnowledgeTable;
    private boolean isAtFixedPoint = false;

    public InferenceBasedResolver(SchemaKnowledgeTable schemaKnowledgeTable) {
        this.queryKnowledgeVisitor = new QueryKnowledgeVisitor();
        this.schemaKnowledgeTable = schemaKnowledgeTable;
    }

    @Override
    public void resolve(FromGraphClause fromGraphClause) throws CompilationException {
        isAtFixedPoint = true;

        // Update our query knowledge, then unify vertices across our FROM-GRAPH-CLAUSE.
        queryKnowledgeVisitor.visit(fromGraphClause, null);
        new LabelConsistencyVisitor(queryKnowledgeVisitor.getQueryKnowledgeTable()).visit(fromGraphClause, null);

        // Perform our resolution.
        for (MatchClause matchClause : fromGraphClause.getMatchClauses()) {
            for (PathPatternExpr pathExpression : matchClause.getPathExpressions()) {
                // We can only infer labels on edges. We ignore dangling vertices.
                for (EdgePatternExpr edgeExpression : pathExpression.getEdgeExpressions()) {
                    isAtFixedPoint &= resolveEdge(edgeExpression);
                }
            }
        }
    }

    private boolean resolveEdge(EdgePatternExpr edgePatternExpr) {
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        if (edgeDescriptor.getPatternType() == EdgeDescriptor.PatternType.PATH) {
            VertexPatternExpr workingLeftVertex = edgePatternExpr.getLeftVertex();

            // We have a sub-path. Recurse with the edges of this sub-path.
            boolean intermediateResult = true;
            for (int i = 0; i < edgeDescriptor.getMaximumHops(); i++) {
                VertexPatternExpr rightVertex;
                if (i == edgeDescriptor.getMaximumHops() - 1) {
                    // This is the final vertex in our path.
                    rightVertex = edgePatternExpr.getRightVertex();

                } else {
                    // We need to get an intermediate vertex.
                    rightVertex = edgePatternExpr.getInternalVertices().get(i);
                }

                // Build our EDGE-PATTERN-EXPR and recurse.
                EdgeDescriptor newDescriptor = new EdgeDescriptor(edgeDescriptor.getEdgeDirection(),
                        EdgeDescriptor.PatternType.EDGE, edgeDescriptor.getEdgeLabels(), null, null, null);
                intermediateResult &= resolveEdge(new EdgePatternExpr(workingLeftVertex, rightVertex, newDescriptor));

                // Update the labels of our edge and our internal vertex.
                edgeDescriptor.getEdgeLabels().addAll(newDescriptor.getEdgeLabels());
                if (i != edgeDescriptor.getMaximumHops() - 1) {
                    for (ElementLabel label : rightVertex.getLabels()) {
                        // Mark our labels as not inferred to prevent invalidation of what we just found.
                        label.markInferred(false);
                    }
                }
                workingLeftVertex = rightVertex;
            }

            return intermediateResult;
        }

        if (edgeDescriptor.getEdgeDirection() == EdgeDescriptor.EdgeDirection.UNDIRECTED) {
            // We have an undirected edge. Recurse with a LEFT_TO_RIGHT edge...
            edgeDescriptor.setEdgeDirection(EdgeDescriptor.EdgeDirection.LEFT_TO_RIGHT);
            boolean isLeftToRightModified = !resolveEdge(edgePatternExpr);

            // ...and a RIGHT_TO_LEFT edge.
            edgeDescriptor.setEdgeDirection(EdgeDescriptor.EdgeDirection.RIGHT_TO_LEFT);
            boolean isRightToLeftModified = !resolveEdge(edgePatternExpr);

            // Determine the direction of our edge, if possible.
            if (isLeftToRightModified && !isRightToLeftModified) {
                edgeDescriptor.setEdgeDirection(EdgeDescriptor.EdgeDirection.LEFT_TO_RIGHT);

            } else if (!isLeftToRightModified && isRightToLeftModified) {
                edgeDescriptor.setEdgeDirection(EdgeDescriptor.EdgeDirection.RIGHT_TO_LEFT);

            } else {
                edgeDescriptor.setEdgeDirection(EdgeDescriptor.EdgeDirection.UNDIRECTED);
            }
            return !(isLeftToRightModified || isRightToLeftModified);
        }

        // We have a _directed_ *edge*. Determine our source and destination vertex.
        VertexPatternExpr sourceVertexPattern, destinationVertexPattern;
        if (edgeDescriptor.getEdgeDirection() == EdgeDescriptor.EdgeDirection.LEFT_TO_RIGHT) {
            sourceVertexPattern = edgePatternExpr.getLeftVertex();
            destinationVertexPattern = edgePatternExpr.getRightVertex();

        } else { // edgeDescriptor.getEdgeDirection() == EdgeDescriptor.EdgeDirection.RIGHT_TO_LEFT
            sourceVertexPattern = edgePatternExpr.getRightVertex();
            destinationVertexPattern = edgePatternExpr.getLeftVertex();
        }

        // An unknown is valid iff the element is not contained within the label set and **every** label is inferred.
        final BiFunction<Set<ElementLabel>, ElementLabel, Boolean> validUnknownPredicate =
                (s, e) -> !s.contains(e) && s.stream().allMatch(ElementLabel::isInferred);
        Set<ElementLabel> sourceLabels = sourceVertexPattern.getLabels();
        Set<ElementLabel> destLabels = destinationVertexPattern.getLabels();
        Set<ElementLabel> edgeLabels = edgeDescriptor.getEdgeLabels();

        // Iterate through our knowledge table. Attempt to fill in unknowns.
        boolean isUnknownFilled = false;
        for (SchemaKnowledgeTable.KnowledgeRecord knowledgeRecord : schemaKnowledgeTable) {
            ElementLabel recordSourceLabel = knowledgeRecord.getSourceVertexLabel();
            ElementLabel recordDestLabel = knowledgeRecord.getDestVertexLabel();
            ElementLabel recordEdgeLabel = knowledgeRecord.getEdgeLabel();
            boolean isSourceValidUnknown = validUnknownPredicate.apply(sourceLabels, recordSourceLabel);
            boolean isDestValidUnknown = validUnknownPredicate.apply(destLabels, recordDestLabel);
            boolean isEdgeValidUnknown = validUnknownPredicate.apply(edgeLabels, recordEdgeLabel);
            if (sourceLabels.contains(recordSourceLabel)) {
                if (edgeLabels.contains(recordEdgeLabel) && isDestValidUnknown) {
                    // Source is known, edge is known, dest is unknown.
                    destLabels.add(recordDestLabel.asInferred());
                    isUnknownFilled = true;

                } else if (isEdgeValidUnknown && destLabels.contains(recordDestLabel)) {
                    // Source is known, edge is unknown, dest is known.
                    edgeLabels.add(recordEdgeLabel.asInferred());
                    isUnknownFilled = true;

                } else if (isEdgeValidUnknown && isDestValidUnknown) {
                    // Source is known, edge is unknown, dest is unknown.
                    destLabels.add(recordDestLabel.asInferred());
                    edgeLabels.add(recordEdgeLabel.asInferred());
                    isUnknownFilled = true;
                }

            } else if (edgeLabels.contains(recordEdgeLabel)) {
                if (isSourceValidUnknown && destLabels.contains(recordDestLabel)) {
                    // Source is unknown, edge is known, dest is known.
                    sourceLabels.add(recordSourceLabel.asInferred());
                    isUnknownFilled = true;

                } else if (isSourceValidUnknown && isDestValidUnknown) {
                    // Source is unknown, edge is known, dest is unknown.
                    sourceLabels.add(recordSourceLabel.asInferred());
                    destLabels.add(recordDestLabel.asInferred());
                    isUnknownFilled = true;
                }

            } else if (destLabels.contains(recordDestLabel) && isSourceValidUnknown && isEdgeValidUnknown) {
                // Source is unknown, edge is unknown, dest is known.
                sourceLabels.add(recordSourceLabel.asInferred());
                edgeLabels.add(recordEdgeLabel.asInferred());
                isUnknownFilled = true;
            }
        }
        return !isUnknownFilled;
    }

    @Override
    public boolean isAtFixedPoint() {
        return isAtFixedPoint;
    }
}
