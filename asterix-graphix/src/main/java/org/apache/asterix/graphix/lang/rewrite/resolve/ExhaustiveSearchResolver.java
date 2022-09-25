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
package org.apache.asterix.graphix.lang.rewrite.resolve;

import static org.apache.asterix.graphix.lang.rewrite.util.CanonicalElementUtil.expandElementLabels;
import static org.apache.asterix.graphix.lang.rewrite.util.CanonicalElementUtil.expandFixedPathPattern;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.annotation.SubqueryVertexJoinAnnotation;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.visitor.GraphixDeepCopyVisitor;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor.EdgeDirection;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.struct.PatternGroup;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class ExhaustiveSearchResolver implements IPatternGroupResolver {
    private final GraphixDeepCopyVisitor deepCopyVisitor;
    private final SchemaKnowledgeTable schemaKnowledgeTable;

    // Vertex / edge expression map to a list of canonical vertex / edge expressions.
    private final Map<VariableExpr, List<AbstractExpression>> canonicalExpressionsMap;
    private final Map<VariableExpr, AbstractExpression> originalExpressionMap;

    public ExhaustiveSearchResolver(SchemaKnowledgeTable schemaKnowledgeTable) {
        this.schemaKnowledgeTable = schemaKnowledgeTable;
        this.deepCopyVisitor = new GraphixDeepCopyVisitor();
        this.canonicalExpressionsMap = new LinkedHashMap<>();
        this.originalExpressionMap = new LinkedHashMap<>();
    }

    private void expandVertexPattern(VertexPatternExpr unexpandedVertexPattern, Deque<PatternGroup> inputPatternGroups,
            Deque<PatternGroup> outputPatternGroups) throws CompilationException {
        VariableExpr vertexVariable = unexpandedVertexPattern.getVariableExpr();
        Set<ElementLabel> vertexLabelSet = unexpandedVertexPattern.getLabels();
        Set<ElementLabel> expandedLabelSet =
                expandElementLabels(unexpandedVertexPattern, vertexLabelSet, schemaKnowledgeTable);
        while (!inputPatternGroups.isEmpty()) {
            PatternGroup unexpandedPatternGroup = inputPatternGroups.pop();
            for (ElementLabel vertexLabel : expandedLabelSet) {
                PatternGroup expandedPatternGroup = new PatternGroup();
                for (VertexPatternExpr vertexPatternExpr : unexpandedPatternGroup.getVertexPatternSet()) {
                    VertexPatternExpr clonedVertexPattern = deepCopyVisitor.visit(vertexPatternExpr, null);
                    SubqueryVertexJoinAnnotation hint = vertexPatternExpr.findHint(SubqueryVertexJoinAnnotation.class);
                    if (vertexPatternExpr.getVariableExpr().equals(vertexVariable)
                            || (hint != null && hint.getSourceVertexVariable().equals(vertexVariable))) {
                        clonedVertexPattern.getLabels().clear();
                        clonedVertexPattern.getLabels().add(vertexLabel);
                    }
                    expandedPatternGroup.getVertexPatternSet().add(clonedVertexPattern);
                }
                for (EdgePatternExpr unexpandedEdgePattern : unexpandedPatternGroup.getEdgePatternSet()) {
                    EdgePatternExpr clonedEdgePattern = deepCopyVisitor.visit(unexpandedEdgePattern, null);
                    VertexPatternExpr clonedLeftVertex = clonedEdgePattern.getLeftVertex();
                    SubqueryVertexJoinAnnotation hint1 = clonedLeftVertex.findHint(SubqueryVertexJoinAnnotation.class);
                    if (clonedLeftVertex.getVariableExpr().equals(vertexVariable)
                            || (hint1 != null && hint1.getSourceVertexVariable().equals(vertexVariable))) {
                        clonedLeftVertex.getLabels().clear();
                        clonedLeftVertex.getLabels().add(vertexLabel);
                    }

                    VertexPatternExpr clonedRightVertex = clonedEdgePattern.getRightVertex();
                    SubqueryVertexJoinAnnotation hint2 = clonedLeftVertex.findHint(SubqueryVertexJoinAnnotation.class);
                    if (clonedRightVertex.getVariableExpr().equals(vertexVariable)
                            || (hint2 != null && hint2.getSourceVertexVariable().equals(vertexVariable))) {
                        clonedRightVertex.getLabels().clear();
                        clonedRightVertex.getLabels().add(vertexLabel);
                    }
                    expandedPatternGroup.getEdgePatternSet().add(clonedEdgePattern);
                }
                unifyVertexLabels(expandedPatternGroup);
                outputPatternGroups.push(expandedPatternGroup);
            }
        }
    }

    private void expandEdgePattern(EdgePatternExpr unexpandedEdgePattern, Deque<PatternGroup> inputPatternGroups,
            Deque<PatternGroup> outputPatternGroups) throws CompilationException {
        EdgeDescriptor unexpandedEdgeDescriptor = unexpandedEdgePattern.getEdgeDescriptor();
        VariableExpr edgeVariable = unexpandedEdgeDescriptor.getVariableExpr();
        Set<ElementLabel> edgeLabelSet = unexpandedEdgeDescriptor.getEdgeLabels();
        Set<ElementLabel> expandedLabelSet =
                expandElementLabels(unexpandedEdgePattern, edgeLabelSet, schemaKnowledgeTable);

        // Determine the direction of our edge.
        Set<EdgeDirection> edgeDirectionSet = new HashSet<>();
        if (unexpandedEdgeDescriptor.getEdgeDirection() != EdgeDirection.RIGHT_TO_LEFT) {
            edgeDirectionSet.add(EdgeDirection.LEFT_TO_RIGHT);
        }
        if (unexpandedEdgeDescriptor.getEdgeDirection() != EdgeDirection.LEFT_TO_RIGHT) {
            edgeDirectionSet.add(EdgeDirection.RIGHT_TO_LEFT);
        }

        // Expand an edge pattern.
        while (!inputPatternGroups.isEmpty()) {
            PatternGroup unexpandedPatternGroup = inputPatternGroups.pop();
            for (ElementLabel edgeLabel : expandedLabelSet) {
                for (EdgeDirection edgeDirection : edgeDirectionSet) {
                    PatternGroup expandedPatternGroup = new PatternGroup();
                    expandedPatternGroup.getVertexPatternSet().addAll(unexpandedPatternGroup.getVertexPatternSet());
                    for (EdgePatternExpr edgePatternExpr : unexpandedPatternGroup.getEdgePatternSet()) {
                        // Update our edge descriptor, if necessary.
                        EdgePatternExpr clonedEdgePattern = deepCopyVisitor.visit(edgePatternExpr, null);
                        if (edgeVariable.equals(edgePatternExpr.getEdgeDescriptor().getVariableExpr())) {
                            clonedEdgePattern.getEdgeDescriptor().getEdgeLabels().clear();
                            clonedEdgePattern.getEdgeDescriptor().getEdgeLabels().add(edgeLabel);
                            clonedEdgePattern.getEdgeDescriptor().setEdgeDirection(edgeDirection);
                        }
                        expandedPatternGroup.getEdgePatternSet().add(clonedEdgePattern);
                    }
                    unifyVertexLabels(expandedPatternGroup);
                    outputPatternGroups.push(expandedPatternGroup);
                }
            }
        }
    }

    private void expandPathPattern(EdgePatternExpr unexpandedPathPattern, Deque<PatternGroup> inputPatternGroups,
            Deque<PatternGroup> outputPatternGroups) throws CompilationException {
        EdgeDescriptor unexpandedEdgeDescriptor = unexpandedPathPattern.getEdgeDescriptor();
        VariableExpr edgeVariable = unexpandedEdgeDescriptor.getVariableExpr();
        Set<ElementLabel> edgeLabelSet = unexpandedEdgeDescriptor.getEdgeLabels();
        Set<ElementLabel> expandedEdgeLabelSet =
                expandElementLabels(unexpandedPathPattern, edgeLabelSet, schemaKnowledgeTable);

        // Determine our candidates for internal vertex labels.
        Set<ElementLabel> vertexLabelSet = schemaKnowledgeTable.getVertexLabels();

        // Determine the direction of our edges.
        Set<EdgeDirection> edgeDirectionSet = new HashSet<>();
        if (unexpandedEdgeDescriptor.getEdgeDirection() != EdgeDirection.RIGHT_TO_LEFT) {
            edgeDirectionSet.add(EdgeDirection.LEFT_TO_RIGHT);
        }
        if (unexpandedEdgeDescriptor.getEdgeDirection() != EdgeDirection.LEFT_TO_RIGHT) {
            edgeDirectionSet.add(EdgeDirection.RIGHT_TO_LEFT);
        }

        // Determine the length of **expanded** path. For {N,M} w/ E labels... MIN(M, (1 + 2(E-1))).
        int minimumExpansionLength, expansionLength;
        Integer minimumHops = unexpandedEdgeDescriptor.getMinimumHops();
        Integer maximumHops = unexpandedEdgeDescriptor.getMaximumHops();
        if (Objects.equals(minimumHops, maximumHops) && minimumHops != null && minimumHops == 1) {
            // Special case: we have a path disguised as an edge.
            minimumExpansionLength = 1;
            expansionLength = 1;

        } else {
            int maximumExpansionLength = 1 + 2 * (expandedEdgeLabelSet.size() - 1);
            minimumExpansionLength = Objects.requireNonNullElse(minimumHops, 1);
            expansionLength = Objects.requireNonNullElse(maximumHops, maximumExpansionLength);
            if (minimumExpansionLength > expansionLength) {
                minimumExpansionLength = expansionLength;
            }
        }

        // Generate all possible paths, from minimumExpansionLength to expansionLength.
        List<List<EdgePatternExpr>> candidatePaths = new ArrayList<>();
        for (int pathLength = minimumExpansionLength; pathLength <= expansionLength; pathLength++) {
            List<List<EdgePatternExpr>> expandedPathPattern = expandFixedPathPattern(pathLength, unexpandedPathPattern,
                    expandedEdgeLabelSet, vertexLabelSet, edgeDirectionSet, deepCopyVisitor,
                    () -> deepCopyVisitor.visit(unexpandedPathPattern.getInternalVertex(), null));
            candidatePaths.addAll(expandedPathPattern);
        }

        // Push the labels of our pattern group to our edge.
        final BiConsumer<PatternGroup, EdgePatternExpr> vertexLabelUpdater = (p, e) -> {
            VariableExpr leftVariable = e.getLeftVertex().getVariableExpr();
            VariableExpr rightVariable = e.getRightVertex().getVariableExpr();
            p.getVertexPatternSet().forEach(v -> {
                VariableExpr vertexVariable = v.getVariableExpr();
                if (leftVariable.equals(vertexVariable)) {
                    e.getLeftVertex().getLabels().clear();
                    e.getLeftVertex().getLabels().addAll(v.getLabels());
                }
                if (rightVariable.equals(vertexVariable)) {
                    e.getRightVertex().getLabels().clear();
                    e.getRightVertex().getLabels().addAll(v.getLabels());
                }
            });
        };

        // Push our expansion to our pattern groups.
        while (!inputPatternGroups.isEmpty()) {
            PatternGroup unexpandedPatternGroup = inputPatternGroups.pop();
            for (List<EdgePatternExpr> candidatePath : candidatePaths) {
                PatternGroup expandedPatternGroup = new PatternGroup();
                expandedPatternGroup.getVertexPatternSet().addAll(unexpandedPatternGroup.getVertexPatternSet());
                for (EdgePatternExpr edgePatternExpr : unexpandedPatternGroup.getEdgePatternSet()) {
                    if (edgePatternExpr.getEdgeDescriptor().getVariableExpr().equals(edgeVariable)) {
                        for (EdgePatternExpr candidateEdge : candidatePath) {
                            EdgePatternExpr copyEdgePattern = deepCopyVisitor.visit(candidateEdge, null);
                            vertexLabelUpdater.accept(expandedPatternGroup, copyEdgePattern);
                            expandedPatternGroup.getEdgePatternSet().add(copyEdgePattern);
                        }

                    } else {
                        EdgePatternExpr copyEdgePattern = deepCopyVisitor.visit(edgePatternExpr, null);
                        vertexLabelUpdater.accept(expandedPatternGroup, copyEdgePattern);
                        expandedPatternGroup.getEdgePatternSet().add(copyEdgePattern);
                    }
                }
                outputPatternGroups.push(expandedPatternGroup);
            }
        }
    }

    private List<PatternGroup> expandPatternGroups(PatternGroup patternGroup) throws CompilationException {
        // First pass: unify our vertex labels.
        unifyVertexLabels(patternGroup);

        // Second pass: collect all ambiguous graph elements.
        Set<AbstractExpression> ambiguousExpressionSet = new LinkedHashSet<>();
        for (AbstractExpression originalExpr : patternGroup) {
            if (originalExpr instanceof VertexPatternExpr) {
                VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) originalExpr;
                if (vertexPatternExpr.getLabels().size() != 1
                        || vertexPatternExpr.getLabels().iterator().next().isNegated()) {
                    ambiguousExpressionSet.add(vertexPatternExpr);
                }

            } else { // originalExpr instanceof EdgePatternExpr
                EdgePatternExpr edgePatternExpr = (EdgePatternExpr) originalExpr;
                EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
                if (edgeDescriptor.getEdgeLabels().size() != 1
                        || edgeDescriptor.getEdgeLabels().iterator().next().isNegated()
                        || edgeDescriptor.getPatternType() == EdgeDescriptor.PatternType.PATH
                        || edgeDescriptor.getEdgeDirection() == EdgeDirection.UNDIRECTED) {
                    ambiguousExpressionSet.add(edgePatternExpr);
                }
            }
        }
        if (ambiguousExpressionSet.isEmpty()) {
            // Our list must always be mutable.
            return new ArrayList<>(List.of(patternGroup));
        }

        // Third pass: expand the ambiguous expressions.
        Deque<PatternGroup> redPatternGroups = new ArrayDeque<>();
        Deque<PatternGroup> blackPatternGroups = new ArrayDeque<>();
        redPatternGroups.add(patternGroup);
        for (AbstractExpression ambiguousExpression : ambiguousExpressionSet) {
            // Determine our read and write pattern groups.
            Deque<PatternGroup> readPatternGroups, writePatternGroups;
            if (redPatternGroups.isEmpty()) {
                readPatternGroups = blackPatternGroups;
                writePatternGroups = redPatternGroups;

            } else {
                readPatternGroups = redPatternGroups;
                writePatternGroups = blackPatternGroups;
            }

            // Expand our vertex / edge patterns.
            if (ambiguousExpression instanceof VertexPatternExpr) {
                VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) ambiguousExpression;
                expandVertexPattern(vertexPatternExpr, readPatternGroups, writePatternGroups);

            } else { // ambiguousExpression instance EdgePatternExpr
                EdgePatternExpr edgePatternExpr = (EdgePatternExpr) ambiguousExpression;
                if (edgePatternExpr.getEdgeDescriptor().getPatternType() == EdgeDescriptor.PatternType.EDGE) {
                    expandEdgePattern(edgePatternExpr, readPatternGroups, writePatternGroups);

                } else { // edgePatternExpr.getEdgeDescriptor().getPatternType() == EdgeDescriptor.PatternType.PATH
                    expandPathPattern(edgePatternExpr, readPatternGroups, writePatternGroups);
                }
            }
        }
        return new ArrayList<>(redPatternGroups.isEmpty() ? blackPatternGroups : redPatternGroups);
    }

    private List<PatternGroup> pruneInvalidPatternGroups(List<PatternGroup> sourceGroups) {
        ListIterator<PatternGroup> sourceGroupIterator = sourceGroups.listIterator();
        while (sourceGroupIterator.hasNext()) {
            PatternGroup workingPatternGroup = sourceGroupIterator.next();
            for (AbstractExpression abstractExpression : workingPatternGroup) {
                if (!(abstractExpression instanceof EdgePatternExpr)) {
                    continue;
                }

                // Prune any groups with bad edges.
                EdgePatternExpr edgePatternExpr = (EdgePatternExpr) abstractExpression;
                if (!schemaKnowledgeTable.isValidEdge(edgePatternExpr)) {
                    sourceGroupIterator.remove();
                    break;
                }
            }
        }
        return sourceGroups;
    }

    private void unifyVertexLabels(PatternGroup patternGroup) {
        // Pass #1: Collect all vertex variables and their labels.
        Map<VariableExpr, Set<ElementLabel>> vertexVariableMap = new HashMap<>();
        for (AbstractExpression abstractExpression : patternGroup) {
            if (abstractExpression instanceof VertexPatternExpr) {
                VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) abstractExpression;
                VariableExpr vertexVariable = vertexPatternExpr.getVariableExpr();
                vertexVariableMap.putIfAbsent(vertexVariable, new HashSet<>());
                vertexVariableMap.get(vertexVariable).addAll(vertexPatternExpr.getLabels());
                SubqueryVertexJoinAnnotation hint = vertexPatternExpr.findHint(SubqueryVertexJoinAnnotation.class);
                if (hint != null) {
                    VariableExpr sourceVertexVariable = hint.getSourceVertexVariable();
                    vertexVariableMap.putIfAbsent(sourceVertexVariable, new HashSet<>());
                    vertexVariableMap.get(sourceVertexVariable).addAll(vertexPatternExpr.getLabels());
                }

            } else { // abstractExpression instanceof EdgePatternExpr
                EdgePatternExpr edgePatternExpr = (EdgePatternExpr) abstractExpression;
                VertexPatternExpr leftVertexExpr = edgePatternExpr.getLeftVertex();
                VertexPatternExpr rightVertexExpr = edgePatternExpr.getRightVertex();
                VariableExpr leftVariable = leftVertexExpr.getVariableExpr();
                VariableExpr rightVariable = rightVertexExpr.getVariableExpr();

                // Visit the vertices of our edges and their labels as well.
                vertexVariableMap.putIfAbsent(leftVariable, new HashSet<>());
                vertexVariableMap.putIfAbsent(rightVariable, new HashSet<>());
                vertexVariableMap.get(leftVariable).addAll(leftVertexExpr.getLabels());
                vertexVariableMap.get(rightVariable).addAll(rightVertexExpr.getLabels());
                SubqueryVertexJoinAnnotation leftHint = leftVertexExpr.findHint(SubqueryVertexJoinAnnotation.class);
                SubqueryVertexJoinAnnotation rightHint = rightVertexExpr.findHint(SubqueryVertexJoinAnnotation.class);
                if (leftHint != null) {
                    VariableExpr sourceVertexVariable = leftHint.getSourceVertexVariable();
                    vertexVariableMap.putIfAbsent(sourceVertexVariable, new HashSet<>());
                    vertexVariableMap.get(sourceVertexVariable).addAll(leftVertexExpr.getLabels());
                }
                if (rightHint != null) {
                    VariableExpr sourceVertexVariable = rightHint.getSourceVertexVariable();
                    vertexVariableMap.putIfAbsent(sourceVertexVariable, new HashSet<>());
                    vertexVariableMap.get(sourceVertexVariable).addAll(rightVertexExpr.getLabels());
                }
            }
        }

        // Pass #2: Copy the vertex label sets to all vertices of the same variable.
        for (AbstractExpression abstractExpression : patternGroup) {
            if (abstractExpression instanceof VertexPatternExpr) {
                VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) abstractExpression;
                VariableExpr vertexVariable = vertexPatternExpr.getVariableExpr();
                vertexPatternExpr.getLabels().addAll(vertexVariableMap.get(vertexVariable));
                SubqueryVertexJoinAnnotation hint = vertexPatternExpr.findHint(SubqueryVertexJoinAnnotation.class);
                if (hint != null) {
                    vertexPatternExpr.getLabels().addAll(vertexVariableMap.get(hint.getSourceVertexVariable()));
                }

            } else { // abstractExpression instanceof EdgePatternExpr
                EdgePatternExpr edgePatternExpr = (EdgePatternExpr) abstractExpression;
                VertexPatternExpr leftVertexExpr = edgePatternExpr.getLeftVertex();
                VertexPatternExpr rightVertexExpr = edgePatternExpr.getRightVertex();

                // Visit the vertices of our edge as well.
                VariableExpr leftVariable = leftVertexExpr.getVariableExpr();
                VariableExpr rightVariable = rightVertexExpr.getVariableExpr();
                leftVertexExpr.getLabels().addAll(vertexVariableMap.get(leftVariable));
                rightVertexExpr.getLabels().addAll(vertexVariableMap.get(rightVariable));
                SubqueryVertexJoinAnnotation leftHint = leftVertexExpr.findHint(SubqueryVertexJoinAnnotation.class);
                SubqueryVertexJoinAnnotation rightHint = rightVertexExpr.findHint(SubqueryVertexJoinAnnotation.class);
                if (leftHint != null) {
                    leftVertexExpr.getLabels().addAll(vertexVariableMap.get(leftHint.getSourceVertexVariable()));
                }
                if (rightHint != null) {
                    rightVertexExpr.getLabels().addAll(vertexVariableMap.get(rightHint.getSourceVertexVariable()));
                }
            }
        }

    }

    @Override
    public void resolve(PatternGroup patternGroup) throws CompilationException {
        // Populate our vertex / edge expression map.
        for (VertexPatternExpr vertexPatternExpr : patternGroup.getVertexPatternSet()) {
            VariableExpr vertexVariable = vertexPatternExpr.getVariableExpr();
            canonicalExpressionsMap.putIfAbsent(vertexVariable, new ArrayList<>());
            originalExpressionMap.put(vertexVariable, vertexPatternExpr);
        }
        for (EdgePatternExpr edgePatternExpr : patternGroup.getEdgePatternSet()) {
            EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
            VariableExpr edgeVariable = edgeDescriptor.getVariableExpr();
            canonicalExpressionsMap.putIfAbsent(edgeVariable, new ArrayList<>());
            originalExpressionMap.put(edgeVariable, edgePatternExpr);
        }

        // Expand the given pattern group and then prune the invalid pattern groups.
        List<PatternGroup> validPatternGroups = pruneInvalidPatternGroups(expandPatternGroups(patternGroup));
        for (PatternGroup validPatternGroup : validPatternGroups) {
            for (AbstractExpression canonicalExpr : validPatternGroup) {
                if (canonicalExpr instanceof VertexPatternExpr) {
                    VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) canonicalExpr;
                    VariableExpr vertexVariable = vertexPatternExpr.getVariableExpr();
                    canonicalExpressionsMap.get(vertexVariable).add(deepCopyVisitor.visit(vertexPatternExpr, null));

                } else { // canonicalExpr instanceof EdgePatternExpr
                    EdgePatternExpr edgePatternExpr = (EdgePatternExpr) canonicalExpr;
                    EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
                    VariableExpr edgeVariable = edgeDescriptor.getVariableExpr();
                    canonicalExpressionsMap.get(edgeVariable).add(deepCopyVisitor.visit(edgePatternExpr, null));

                    // We must also visit the vertices of our edge (to find internal vertices).
                    VertexPatternExpr leftVertex = edgePatternExpr.getLeftVertex();
                    VertexPatternExpr rightVertex = edgePatternExpr.getRightVertex();
                    VariableExpr leftVariable = leftVertex.getVariableExpr();
                    VariableExpr rightVariable = rightVertex.getVariableExpr();
                    canonicalExpressionsMap.putIfAbsent(leftVariable, new ArrayList<>());
                    canonicalExpressionsMap.putIfAbsent(rightVariable, new ArrayList<>());
                    canonicalExpressionsMap.get(leftVariable).add(deepCopyVisitor.visit(leftVertex, null));
                    canonicalExpressionsMap.get(rightVariable).add(deepCopyVisitor.visit(rightVertex, null));
                }
            }
        }

        // Propagate the facts of our expression map to the original expressions.
        final Function<VariableExpr, Stream<VertexPatternExpr>> canonicalVertexStreamProvider =
                v -> canonicalExpressionsMap.get(v).stream().map(t -> (VertexPatternExpr) t);
        final Function<VariableExpr, Stream<EdgePatternExpr>> canonicalEdgeStreamProvider =
                v -> canonicalExpressionsMap.get(v).stream().map(t -> (EdgePatternExpr) t);
        for (Map.Entry<VariableExpr, AbstractExpression> mapEntry : originalExpressionMap.entrySet()) {
            if (canonicalExpressionsMap.get(mapEntry.getKey()).isEmpty()) {
                SourceLocation sourceLocation = mapEntry.getValue().getSourceLocation();
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLocation,
                        "Encountered graph element that does not conform the queried graph schema!");
            }

            if (mapEntry.getValue() instanceof VertexPatternExpr) {
                VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) mapEntry.getValue();
                vertexPatternExpr.getLabels().clear();
                Stream<VertexPatternExpr> vertexStream = canonicalVertexStreamProvider.apply(mapEntry.getKey());
                vertexStream.forEach(v -> vertexPatternExpr.getLabels().addAll(v.getLabels()));

            } else { // originalExpr instanceof EdgePatternExpr
                EdgePatternExpr edgePatternExpr = (EdgePatternExpr) mapEntry.getValue();
                EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
                Integer maximumHopLength = edgeDescriptor.getMaximumHops();

                // Update our edge labels.
                edgeDescriptor.getEdgeLabels().clear();
                Stream<EdgePatternExpr> edgeStream = canonicalEdgeStreamProvider.apply(mapEntry.getKey());
                edgeStream.forEach(e -> edgeDescriptor.getEdgeLabels().addAll(e.getEdgeDescriptor().getEdgeLabels()));

                // Update our direction, if we have resolved it.
                Set<EdgeDirection> resolvedDirections = canonicalEdgeStreamProvider.apply(mapEntry.getKey())
                        .map(e -> e.getEdgeDescriptor().getEdgeDirection()).collect(Collectors.toSet());
                if (resolvedDirections.size() == 1) {
                    edgeDescriptor.setEdgeDirection(resolvedDirections.iterator().next());
                }

                // Update the vertex references of our edge.
                VariableExpr leftVariable = edgePatternExpr.getLeftVertex().getVariableExpr();
                VariableExpr rightVariable = edgePatternExpr.getRightVertex().getVariableExpr();
                edgePatternExpr.getLeftVertex().getLabels().clear();
                edgePatternExpr.getRightVertex().getLabels().clear();
                Stream<VertexPatternExpr> leftStream = canonicalVertexStreamProvider.apply(leftVariable);
                Stream<VertexPatternExpr> rightStream = canonicalVertexStreamProvider.apply(rightVariable);
                leftStream.forEach(v -> edgePatternExpr.getLeftVertex().getLabels().addAll(v.getLabels()));
                rightStream.forEach(v -> edgePatternExpr.getRightVertex().getLabels().addAll(v.getLabels()));
                if (edgeDescriptor.getPatternType() == EdgeDescriptor.PatternType.PATH && maximumHopLength != 1) {
                    VariableExpr internalVariable = edgePatternExpr.getInternalVertex().getVariableExpr();
                    edgePatternExpr.getInternalVertex().getLabels().clear();
                    Stream<VertexPatternExpr> internalStream = canonicalVertexStreamProvider.apply(internalVariable);
                    internalStream.forEach(v -> edgePatternExpr.getInternalVertex().getLabels().addAll(v.getLabels()));
                }
            }
        }
    }
}
