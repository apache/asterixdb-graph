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
package org.apache.asterix.graphix.lang.rewrite.canonical;

import static org.apache.asterix.graphix.lang.rewrite.util.CanonicalElementUtil.deepCopyPathPattern;
import static org.apache.asterix.graphix.lang.rewrite.util.CanonicalElementUtil.expandEdgeDirection;
import static org.apache.asterix.graphix.lang.rewrite.util.CanonicalElementUtil.expandFixedPathPattern;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.resolve.IInternalVertexSupplier;
import org.apache.asterix.graphix.lang.rewrite.resolve.SchemaKnowledgeTable;
import org.apache.asterix.graphix.lang.rewrite.visitor.GraphixDeepCopyVisitor;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor.EdgeDirection;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.lang.common.expression.VariableExpr;

/**
 * Generate a list of canonical {@link VertexPatternExpr}, {@link EdgePatternExpr}, or {@link PathPatternExpr}
 * instances, given input {@link VertexPatternExpr} or {@link EdgePatternExpr}. We expect the input elements to have
 * all unknowns resolved-- this pass is to generate canonical elements with the knowledge that each label / direction
 * is possible according to our graph schema.
 */
public class CanonicalElementGeneratorFactory {
    private final GraphixDeepCopyVisitor deepCopyVisitor;
    private final SchemaKnowledgeTable schemaKnowledgeTable;
    private final GraphixRewritingContext graphixRewritingContext;

    public CanonicalElementGeneratorFactory(GraphixRewritingContext graphixRewritingContext,
            SchemaKnowledgeTable schemaKnowledgeTable) {
        this.deepCopyVisitor = new GraphixDeepCopyVisitor();
        this.graphixRewritingContext = graphixRewritingContext;
        this.schemaKnowledgeTable = schemaKnowledgeTable;
    }

    public List<VertexPatternExpr> generateCanonicalVertices(VertexPatternExpr vertexPatternExpr)
            throws CompilationException {
        List<VertexPatternExpr> canonicalVertexList = new ArrayList<>();
        for (ElementLabel elementLabel : vertexPatternExpr.getLabels()) {
            VertexPatternExpr vertexCopy = deepCopyVisitor.visit(vertexPatternExpr, null);
            vertexCopy.getLabels().clear();
            vertexCopy.getLabels().add(elementLabel);
            canonicalVertexList.add(vertexCopy);
        }
        return canonicalVertexList;
    }

    public List<EdgePatternExpr> generateCanonicalEdges(EdgePatternExpr edgePatternExpr) throws CompilationException {
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        VertexPatternExpr leftVertex = edgePatternExpr.getLeftVertex();
        VertexPatternExpr rightVertex = edgePatternExpr.getRightVertex();

        // Each variable below represents a loop nesting.
        Set<ElementLabel> edgeLabelList = edgeDescriptor.getEdgeLabels();
        Set<ElementLabel> leftLabelList = leftVertex.getLabels();
        Set<ElementLabel> rightLabelList = rightVertex.getLabels();
        Set<EdgeDirection> directionList = expandEdgeDirection(edgeDescriptor);

        List<EdgePatternExpr> canonicalEdgeList = new ArrayList<>();
        for (ElementLabel edgeLabel : edgeLabelList) {
            for (ElementLabel leftLabel : leftLabelList) {
                for (ElementLabel rightLabel : rightLabelList) {
                    for (EdgeDirection edgeDirection : directionList) {

                        // Generate an edge according to our loop parameters.
                        EdgePatternExpr edgeCopy = deepCopyVisitor.visit(edgePatternExpr, null);
                        edgeCopy.getLeftVertex().getLabels().clear();
                        edgeCopy.getRightVertex().getLabels().clear();
                        edgeCopy.getEdgeDescriptor().getEdgeLabels().clear();
                        edgeCopy.getLeftVertex().getLabels().add(leftLabel);
                        edgeCopy.getRightVertex().getLabels().add(rightLabel);
                        edgeCopy.getEdgeDescriptor().getEdgeLabels().add(edgeLabel);
                        edgeCopy.getEdgeDescriptor().setEdgeDirection(edgeDirection);

                        // If we have a valid edge, insert this into our list.
                        if (schemaKnowledgeTable.isValidEdge(edgeCopy)) {
                            canonicalEdgeList.add(edgeCopy);
                        }
                    }
                }
            }
        }
        return canonicalEdgeList;
    }

    public List<PathPatternExpr> generateCanonicalPaths(EdgePatternExpr edgePatternExpr,
            boolean minimizeExpansionLength) throws CompilationException {
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        VertexPatternExpr leftVertex = edgePatternExpr.getLeftVertex();
        VertexPatternExpr rightVertex = edgePatternExpr.getRightVertex();
        VertexPatternExpr internalVertex = edgePatternExpr.getInternalVertex();

        // Each variable below represents a loop nesting.
        Set<ElementLabel> edgeLabelList = edgeDescriptor.getEdgeLabels();
        Set<ElementLabel> leftLabelList = leftVertex.getLabels();
        Set<ElementLabel> rightLabelList = rightVertex.getLabels();
        Set<EdgeDirection> directionList = expandEdgeDirection(edgeDescriptor);
        Set<ElementLabel> internalLabelList = internalVertex.getLabels();

        // Determine the length of **expanded** path. For {N,M} w/ E labels... MIN(M, (1 + 2(E-1))).
        int minimumExpansionLength, expansionLength;
        if (minimizeExpansionLength) {
            int maximumExpansionLength = 1 + 2 * (edgeLabelList.size() - 1);
            minimumExpansionLength = Objects.requireNonNullElse(edgeDescriptor.getMinimumHops(), 1);
            expansionLength = Objects.requireNonNullElse(edgeDescriptor.getMaximumHops(), maximumExpansionLength);
            if (minimumExpansionLength > expansionLength) {
                minimumExpansionLength = expansionLength;
            }

        } else {
            minimumExpansionLength = Objects.requireNonNullElse(edgeDescriptor.getMinimumHops(), 1);
            expansionLength = edgeDescriptor.getMaximumHops();
        }

        // Generate all valid paths, from minimumExpansionLength to maximumExpansionLength.
        List<List<EdgePatternExpr>> canonicalPathList = new ArrayList<>();
        IInternalVertexSupplier internalVertexSupplier = () -> {
            VertexPatternExpr internalVertexCopy = deepCopyVisitor.visit(internalVertex, null);
            VariableExpr internalVariable = internalVertex.getVariableExpr();
            VariableExpr internalVariableCopy = graphixRewritingContext.getGraphixVariableCopy(internalVariable);
            internalVertexCopy.setVariableExpr(internalVariableCopy);
            return internalVertexCopy;
        };
        for (int pathLength = minimumExpansionLength; pathLength <= expansionLength; pathLength++) {
            List<List<EdgePatternExpr>> expandedPathPattern = expandFixedPathPattern(pathLength, edgePatternExpr,
                    edgeLabelList, internalLabelList, directionList, deepCopyVisitor, internalVertexSupplier);
            for (List<EdgePatternExpr> pathPattern : expandedPathPattern) {
                for (ElementLabel leftLabel : leftLabelList) {
                    for (ElementLabel rightLabel : rightLabelList) {
                        List<EdgePatternExpr> pathCopy = deepCopyPathPattern(pathPattern, deepCopyVisitor);

                        // Set the labels of our leftmost vertex...
                        pathCopy.get(0).getLeftVertex().getLabels().clear();
                        pathCopy.get(0).getLeftVertex().getLabels().add(leftLabel);

                        // ...and our rightmost vertex.
                        pathCopy.get(pathCopy.size() - 1).getRightVertex().getLabels().clear();
                        pathCopy.get(pathCopy.size() - 1).getRightVertex().getLabels().add(rightLabel);

                        // Add our path if all edges are valid.
                        if (pathCopy.stream().allMatch(schemaKnowledgeTable::isValidEdge)) {
                            canonicalPathList.add(pathCopy);
                        }
                    }
                }
            }
        }

        // Wrap our edge lists into path patterns.
        List<PathPatternExpr> pathPatternList = new ArrayList<>();
        for (List<EdgePatternExpr> edgeList : canonicalPathList) {
            List<VertexPatternExpr> vertexList = new ArrayList<>();
            edgeList.forEach(e -> {
                vertexList.add(e.getLeftVertex());
                vertexList.add(e.getRightVertex());
            });
            VariableExpr variableExpr = deepCopyVisitor.visit(edgeDescriptor.getVariableExpr(), null);
            PathPatternExpr pathPatternExpr = new PathPatternExpr(vertexList, edgeList, variableExpr);
            pathPatternExpr.setSourceLocation(edgePatternExpr.getSourceLocation());
            pathPatternList.add(pathPatternExpr);
        }
        return pathPatternList;
    }
}
