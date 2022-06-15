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
package org.apache.asterix.graphix.lang.rewrites.canonical;

import static org.apache.asterix.graphix.lang.struct.EdgeDescriptor.EdgeDirection;
import static org.apache.asterix.graphix.lang.struct.EdgeDescriptor.PatternType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.clause.GraphSelectBlock;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.lower.action.PathPatternAction;
import org.apache.asterix.graphix.lang.rewrites.resolve.SchemaKnowledgeTable;
import org.apache.asterix.graphix.lang.rewrites.visitor.GraphixDeepCopyVisitor;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * Expand an edge and its connected vertices into an edge in canonical form.
 * 1. An edge's left vertex must only have one label.
 * 2. An edge's right vertex must only have one label.
 * 3. An edge must only have one label.
 * 4. An edge must be directed.
 * 5. An edge must not be a sub-path.
 */
public class EdgeSubPathExpander implements ICanonicalExpander<EdgePatternExpr> {
    private final Supplier<VariableExpr> newVariableSupplier;
    private final GraphixDeepCopyVisitor deepCopyVisitor;

    // To avoid "over-expanding", we need to keep knowledge about our FROM-GRAPH-CLAUSE.
    private SchemaKnowledgeTable schemaKnowledgeTable;

    public EdgeSubPathExpander(Supplier<VarIdentifier> getNewVariable) {
        this.deepCopyVisitor = new GraphixDeepCopyVisitor();
        this.newVariableSupplier = () -> new VariableExpr(getNewVariable.get());
    }

    public void resetSchema(SchemaKnowledgeTable schemaKnowledgeTable) {
        this.schemaKnowledgeTable = schemaKnowledgeTable;
    }

    private static class CanonicalEdgeContext {
        private final VariableExpr leftVertexVar;
        private final VariableExpr rightVertexVar;
        private final VariableExpr edgeVar;

        private final ElementLabel leftVertexLabel;
        private final ElementLabel rightVertexLabel;
        private final ElementLabel edgeLabel;

        private final EdgeDirection edgeDirection;

        private CanonicalEdgeContext(VariableExpr leftVertexVar, VariableExpr rightVertexVar, VariableExpr edgeVar,
                ElementLabel leftVertexLabel, ElementLabel rightVertexLabel, ElementLabel edgeLabel,
                EdgeDirection edgeDirection) {
            this.leftVertexVar = leftVertexVar;
            this.rightVertexVar = rightVertexVar;
            this.leftVertexLabel = leftVertexLabel;
            this.rightVertexLabel = rightVertexLabel;
            this.edgeDirection = edgeDirection;
            this.edgeVar = edgeVar;
            this.edgeLabel = edgeLabel;
        }
    }

    @Override
    public void apply(EdgePatternExpr edgePatternExpr, List<GraphSelectBlock> inputSelectBlocks)
            throws CompilationException {
        boolean doesLeftVertexRequireExpansion = edgePatternExpr.getLeftVertex().getLabels().size() > 1;
        boolean doesRightVertexRequireExpansion = edgePatternExpr.getRightVertex().getLabels().size() > 1;
        boolean doesEdgeRequireExpansion = edgePatternExpr.getEdgeDescriptor().getEdgeLabels().size() > 1
                || edgePatternExpr.getEdgeDescriptor().getPatternType() != PatternType.EDGE
                || edgePatternExpr.getEdgeDescriptor().getEdgeDirection() == EdgeDirection.UNDIRECTED;
        if (!doesLeftVertexRequireExpansion && !doesRightVertexRequireExpansion && !doesEdgeRequireExpansion) {
            // Our edge and both of our vertices are in canonical form.
            return;
        }

        // Expand all possibilities that our edge pattern could represent.
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        VertexPatternExpr workingLeftVertex = edgePatternExpr.getLeftVertex();
        VertexPatternExpr workingRightVertex = edgePatternExpr.getRightVertex();
        List<List<CanonicalEdgeContext>> pathCandidates = new ArrayList<>();
        if (edgeDescriptor.getPatternType() == PatternType.EDGE) {
            expandEdgePattern(workingLeftVertex, workingRightVertex, edgeDescriptor, edgeDescriptor::getVariableExpr)
                    .forEach(e -> pathCandidates.add(List.of(e)));

        } else { // edgeDescriptor.getPatternType() == EdgeDescriptor.PatternType.PATH
            Deque<List<CanonicalEdgeContext>> pathCandidateStack = new ArrayDeque<>(List.of(List.of()));
            for (int i = 0; i < edgeDescriptor.getMaximumHops(); i++) {
                VertexPatternExpr nextInternalVertex = (i != edgeDescriptor.getMaximumHops() - 1)
                        ? edgePatternExpr.getInternalVertices().get(i) : edgePatternExpr.getRightVertex();

                // We will only yield a path if we have generated the minimum number of hops.
                Deque<List<CanonicalEdgeContext>> generatedCandidateStack = new ArrayDeque<>();
                boolean isYieldPath = i >= edgeDescriptor.getMinimumHops() - 1;
                while (!pathCandidateStack.isEmpty()) {
                    List<CanonicalEdgeContext> pathCandidate = pathCandidateStack.removeLast();
                    if (isYieldPath) {
                        VertexPatternExpr rightVertex = edgePatternExpr.getRightVertex();
                        expandEdgePattern(workingLeftVertex, rightVertex, edgeDescriptor, newVariableSupplier)
                                .forEach(e -> {
                                    List<CanonicalEdgeContext> pathCopy = new ArrayList<>(pathCandidate);
                                    pathCopy.add(e);
                                    pathCandidates.add(pathCopy);
                                });
                    }
                    expandEdgePattern(workingLeftVertex, nextInternalVertex, edgeDescriptor, newVariableSupplier)
                            .forEach(e -> {
                                List<CanonicalEdgeContext> pathCopy = new ArrayList<>(pathCandidate);
                                pathCopy.add(e);
                                generatedCandidateStack.addLast(pathCopy);
                            });
                }
                pathCandidateStack.addAll(generatedCandidateStack);

                // Move our vertex cursor.
                workingLeftVertex = nextInternalVertex;
            }
        }

        // We must now prune all paths that contain an edge that does not adhere to our schema.
        pathCandidates.removeIf(this::isPathCandidateInvalid);

        // Perform the expansion into GRAPH-SELECT-BLOCKs.
        List<GraphSelectBlock> selectBlockList = expandSelectBlocks(edgePatternExpr, inputSelectBlocks, pathCandidates);
        inputSelectBlocks.clear();
        inputSelectBlocks.addAll(selectBlockList);
    }

    private List<GraphSelectBlock> expandSelectBlocks(EdgePatternExpr edgePatternExpr,
            List<GraphSelectBlock> inputSelectBlocks, List<List<CanonicalEdgeContext>> pathCandidates)
            throws CompilationException {
        List<GraphSelectBlock> generatedGraphSelectBlocks = new ArrayList<>();
        for (GraphSelectBlock oldGraphSelectBlock : inputSelectBlocks) {
            for (List<CanonicalEdgeContext> pathCandidate : pathCandidates) {
                GraphSelectBlock clonedSelectBlock = deepCopyVisitor.visit(oldGraphSelectBlock, null);
                for (MatchClause matchClause : clonedSelectBlock.getFromGraphClause().getMatchClauses()) {
                    Pair<PathPatternExpr, PathPatternExpr> pathPatternReplacePair = null;

                    // We are only interested in the path that contains our already expanded edge.
                    for (PathPatternExpr pathPatternExpr : matchClause.getPathExpressions()) {
                        List<EdgePatternExpr> edgeExpressions = pathPatternExpr.getEdgeExpressions();
                        int edgeExprIndex = edgeExpressions.indexOf(edgePatternExpr);
                        if (edgeExprIndex < 0) {
                            continue;
                        }

                        // Note: in this case, we do not need to worry about dangling vertices.
                        List<LetClause> reboundSubPathList = new ArrayList<>(pathPatternExpr.getReboundSubPathList());
                        List<EdgePatternExpr> newEdgePatternExprList = new ArrayList<>();
                        List<VertexPatternExpr> newVertexPatternExprList = new ArrayList<>();
                        for (int i = 0; i < edgeExpressions.size(); i++) {
                            EdgePatternExpr existingEdgeExpression = edgeExpressions.get(i);
                            if (i != edgeExprIndex) {
                                newEdgePatternExprList.add(existingEdgeExpression);
                                newVertexPatternExprList.add(existingEdgeExpression.getLeftVertex());
                                newVertexPatternExprList.add(existingEdgeExpression.getRightVertex());
                                continue;
                            }

                            // Note: this will produce duplicate vertex expressions.
                            List<VertexPatternExpr> subPathVertexList = new ArrayList<>();
                            List<EdgePatternExpr> subPathEdgeList = new ArrayList<>();
                            for (CanonicalEdgeContext edgeContext : pathCandidate) {
                                VariableExpr newEdgeVar = deepCopyVisitor.visit(edgeContext.edgeVar, null);
                                VariableExpr newLeftVar = deepCopyVisitor.visit(edgeContext.leftVertexVar, null);
                                VariableExpr newRightVar = deepCopyVisitor.visit(edgeContext.rightVertexVar, null);
                                EdgeDescriptor newDescriptor = new EdgeDescriptor(edgeContext.edgeDirection,
                                        PatternType.EDGE, Set.of(edgeContext.edgeLabel), newEdgeVar, 1, 1);
                                Set<ElementLabel> leftLabelSingleton = Set.of(edgeContext.leftVertexLabel);
                                Set<ElementLabel> rightLabelSingleton = Set.of(edgeContext.rightVertexLabel);
                                VertexPatternExpr leftVertex = new VertexPatternExpr(newLeftVar, leftLabelSingleton);
                                VertexPatternExpr rightVertex = new VertexPatternExpr(newRightVar, rightLabelSingleton);
                                EdgePatternExpr newEdge = new EdgePatternExpr(leftVertex, rightVertex, newDescriptor);

                                // Update our path and our sub-path.
                                newEdgePatternExprList.add(newEdge);
                                newVertexPatternExprList.add(leftVertex);
                                newVertexPatternExprList.add(rightVertex);
                                subPathEdgeList.add(newEdge);
                                subPathVertexList.add(leftVertex);
                                subPathVertexList.add(rightVertex);
                            }

                            // Bind our sub-path to a path record.
                            if (existingEdgeExpression.getEdgeDescriptor().getPatternType() == PatternType.PATH) {
                                RecordConstructor pathRecord = new RecordConstructor();
                                EdgeDescriptor edgeDescriptor = existingEdgeExpression.getEdgeDescriptor();
                                PathPatternAction.buildPathRecord(subPathVertexList, subPathEdgeList, pathRecord);
                                reboundSubPathList.add(new LetClause(edgeDescriptor.getVariableExpr(), pathRecord));
                            }
                        }

                        // Build a new path pattern to replace our old path pattern.
                        VariableExpr newPathVariable = null;
                        if (pathPatternExpr.getVariableExpr() != null) {
                            newPathVariable = deepCopyVisitor.visit(pathPatternExpr.getVariableExpr(), null);
                        }
                        PathPatternExpr newPathPatternExpr =
                                new PathPatternExpr(newVertexPatternExprList, newEdgePatternExprList, newPathVariable);
                        newPathPatternExpr.getReboundSubPathList().addAll(reboundSubPathList);
                        pathPatternReplacePair = new Pair<>(pathPatternExpr, newPathPatternExpr);
                    }

                    // Finalize our expansion by replacing our path in our MATCH-CLAUSE.
                    if (pathPatternReplacePair == null) {
                        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                                "Edge was expanded, but edge was not found in given SELECT-BLOCK!");
                    }
                    PathPatternExpr oldPathPattern = pathPatternReplacePair.first;
                    PathPatternExpr newPathPattern = pathPatternReplacePair.second;
                    matchClause.getPathExpressions().replaceAll(p -> (p == oldPathPattern) ? newPathPattern : p);
                }
                generatedGraphSelectBlocks.add(clonedSelectBlock);
            }
        }
        return generatedGraphSelectBlocks;
    }

    private List<CanonicalEdgeContext> expandEdgePattern(VertexPatternExpr leftVertex, VertexPatternExpr rightVertex,
            EdgeDescriptor edgeDescriptor, Supplier<VariableExpr> edgeVarSupplier) throws CompilationException {
        List<CanonicalEdgeContext> edgeCandidates = new ArrayList<>();
        for (ElementLabel leftVertexLabel : leftVertex.getLabels()) {
            for (ElementLabel rightVertexLabel : rightVertex.getLabels()) {
                for (ElementLabel edgeLabel : edgeDescriptor.getEdgeLabels()) {
                    if (edgeDescriptor.getEdgeDirection() != EdgeDirection.LEFT_TO_RIGHT) {
                        VariableExpr edgeVar = deepCopyVisitor.visit(edgeVarSupplier.get(), null);
                        VariableExpr leftVertexVar = deepCopyVisitor.visit(leftVertex.getVariableExpr(), null);
                        VariableExpr rightVertexVar = deepCopyVisitor.visit(rightVertex.getVariableExpr(), null);
                        CanonicalEdgeContext rightToLeftContext =
                                new CanonicalEdgeContext(leftVertexVar, rightVertexVar, edgeVar, leftVertexLabel,
                                        rightVertexLabel, edgeLabel, EdgeDirection.RIGHT_TO_LEFT);
                        edgeCandidates.add(rightToLeftContext);
                    }
                    if (edgeDescriptor.getEdgeDirection() != EdgeDirection.RIGHT_TO_LEFT) {
                        VariableExpr edgeVar = deepCopyVisitor.visit(edgeVarSupplier.get(), null);
                        VariableExpr leftVertexVar = deepCopyVisitor.visit(leftVertex.getVariableExpr(), null);
                        VariableExpr rightVertexVar = deepCopyVisitor.visit(rightVertex.getVariableExpr(), null);
                        CanonicalEdgeContext leftToRightContext =
                                new CanonicalEdgeContext(leftVertexVar, rightVertexVar, edgeVar, leftVertexLabel,
                                        rightVertexLabel, edgeLabel, EdgeDirection.LEFT_TO_RIGHT);
                        edgeCandidates.add(leftToRightContext);
                    }
                }
            }
        }
        return edgeCandidates;
    }

    private boolean isPathCandidateInvalid(List<CanonicalEdgeContext> pathCandidate) {
        for (CanonicalEdgeContext edgeCandidate : pathCandidate) {
            for (SchemaKnowledgeTable.KnowledgeRecord knowledgeRecord : schemaKnowledgeTable) {
                if (edgeCandidate.edgeDirection == EdgeDirection.LEFT_TO_RIGHT) {
                    if (edgeCandidate.leftVertexLabel.equals(knowledgeRecord.getSourceVertexLabel())
                            && edgeCandidate.edgeLabel.equals(knowledgeRecord.getEdgeLabel())
                            && edgeCandidate.rightVertexLabel.equals(knowledgeRecord.getDestVertexLabel())) {
                        return false;
                    }

                } else { // edgeCandidate.edgeDirection == EdgeDescriptor.EdgeDirection.RIGHT_TO_LEFT
                    if (edgeCandidate.leftVertexLabel.equals(knowledgeRecord.getDestVertexLabel())
                            && edgeCandidate.edgeLabel.equals(knowledgeRecord.getEdgeLabel())
                            && edgeCandidate.rightVertexLabel.equals(knowledgeRecord.getSourceVertexLabel())) {
                        return false;
                    }
                }
            }
            return true;
        }
        return true;
    }
}
