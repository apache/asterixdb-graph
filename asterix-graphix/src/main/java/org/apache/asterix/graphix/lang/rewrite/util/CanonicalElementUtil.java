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
package org.apache.asterix.graphix.lang.rewrite.util;

import static org.apache.asterix.graphix.lang.struct.EdgeDescriptor.EdgeDirection;
import static org.apache.asterix.graphix.lang.struct.EdgeDescriptor.PatternType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.resolve.IInternalVertexSupplier;
import org.apache.asterix.graphix.lang.rewrite.resolve.SchemaKnowledgeTable;
import org.apache.asterix.graphix.lang.rewrite.visitor.GraphixDeepCopyVisitor;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.paukov.combinatorics3.Generator;

public final class CanonicalElementUtil {
    public static Set<ElementLabel> expandElementLabels(AbstractExpression originalExpr,
            Set<ElementLabel> elementLabels, SchemaKnowledgeTable schemaKnowledgeTable) {
        Set<ElementLabel> schemaLabels = (originalExpr instanceof VertexPatternExpr)
                ? schemaKnowledgeTable.getVertexLabels() : schemaKnowledgeTable.getEdgeLabels();
        if (elementLabels.isEmpty()) {
            // If our set is empty, then return all the element labels associated with our schema.
            return schemaLabels;
        }

        // If we have any negated element labels, then we return the difference between our schema and this set.
        Set<String> negatedElementLabels = elementLabels.stream().filter(ElementLabel::isNegated)
                .map(ElementLabel::getLabelName).collect(Collectors.toSet());
        if (!negatedElementLabels.isEmpty()) {
            return schemaLabels.stream().filter(s -> !negatedElementLabels.contains(s.getLabelName()))
                    .collect(Collectors.toSet());
        }

        // Otherwise, we return our input set of labels.
        return elementLabels;
    }

    public static Set<EdgeDirection> expandEdgeDirection(EdgeDescriptor edgeDescriptor) {
        Set<EdgeDirection> edgeDirectionList = new HashSet<>();
        if (edgeDescriptor.getEdgeDirection() != EdgeDirection.RIGHT_TO_LEFT) {
            edgeDirectionList.add(EdgeDirection.LEFT_TO_RIGHT);
        }
        if (edgeDescriptor.getEdgeDirection() != EdgeDirection.LEFT_TO_RIGHT) {
            edgeDirectionList.add(EdgeDirection.RIGHT_TO_LEFT);
        }
        return edgeDirectionList;
    }

    public static List<List<EdgePatternExpr>> expandFixedPathPattern(int pathLength, EdgePatternExpr edgePattern,
            Set<ElementLabel> edgeLabels, Set<ElementLabel> vertexLabels, Set<EdgeDirection> edgeDirections,
            GraphixDeepCopyVisitor deepCopyVisitor, IInternalVertexSupplier internalVertexSupplier)
            throws CompilationException {
        VertexPatternExpr leftVertex = edgePattern.getLeftVertex();
        VertexPatternExpr rightVertex = edgePattern.getRightVertex();
        VariableExpr edgeVariableExpr = edgePattern.getEdgeDescriptor().getVariableExpr();

        // Generate all possible edge label K-permutations w/ repetitions.
        List<List<ElementLabel>> edgeLabelPermutations = new ArrayList<>();
        Generator.combination(edgeLabels).multi(pathLength).stream()
                .forEach(c -> Generator.permutation(c).simple().forEach(edgeLabelPermutations::add));

        // Generate all possible direction K-permutations w/ repetitions.
        List<List<EdgeDirection>> directionPermutations = new ArrayList<>();
        Generator.combination(edgeDirections).multi(pathLength).stream()
                .forEach(c -> Generator.permutation(c).simple().forEach(directionPermutations::add));

        // Special case: if we have a path of length one, we do not need to permute our vertices.
        if (pathLength == 1) {
            List<List<EdgePatternExpr>> expandedPathPatterns = new ArrayList<>();
            for (List<ElementLabel> edgeLabelPermutation : edgeLabelPermutations) {
                ElementLabel edgeLabel = edgeLabelPermutation.get(0);
                for (List<EdgeDirection> directionPermutation : directionPermutations) {
                    EdgeDirection edgeDirection = directionPermutation.get(0);
                    EdgeDescriptor edgeDescriptor = new EdgeDescriptor(edgeDirection, PatternType.EDGE,
                            Set.of(edgeLabel), null, deepCopyVisitor.visit(edgeVariableExpr, null), null, null);
                    expandedPathPatterns.add(List.of(new EdgePatternExpr(leftVertex, rightVertex, edgeDescriptor)));
                }
            }
            return expandedPathPatterns;
        }

        // Otherwise... generate all possible (K-1)-permutations w/ repetitions.
        List<List<ElementLabel>> vertexLabelPermutations = new ArrayList<>();
        Generator.combination(vertexLabels).multi(pathLength - 1).stream()
                .forEach(c -> Generator.permutation(c).simple().forEach(vertexLabelPermutations::add));

        // ... and perform a cartesian product of all three sets above.
        List<List<EdgePatternExpr>> expandedPathPatterns = new ArrayList<>();
        for (List<ElementLabel> edgeLabelPermutation : edgeLabelPermutations) {
            for (List<EdgeDirection> directionPermutation : directionPermutations) {
                for (List<ElementLabel> vertexLabelPermutation : vertexLabelPermutations) {
                    Iterator<ElementLabel> edgeLabelIterator = edgeLabelPermutation.iterator();
                    Iterator<EdgeDirection> directionIterator = directionPermutation.iterator();
                    Iterator<ElementLabel> vertexLabelIterator = vertexLabelPermutation.iterator();

                    // Build one path.
                    List<EdgePatternExpr> pathPattern = new ArrayList<>();
                    VertexPatternExpr previousRightVertex = null;
                    for (int i = 0; edgeLabelIterator.hasNext(); i++) {
                        Set<ElementLabel> edgeLabelSet = Set.of(edgeLabelIterator.next());
                        EdgeDirection edgeDirection = directionIterator.next();

                        // Determine our left vertex.
                        VertexPatternExpr workingLeftVertex;
                        if (i == 0) {
                            workingLeftVertex = deepCopyVisitor.visit(leftVertex, null);
                        } else {
                            workingLeftVertex = deepCopyVisitor.visit(previousRightVertex, null);
                        }

                        // Determine our right vertex.
                        VertexPatternExpr workingRightVertex;
                        if (!edgeLabelIterator.hasNext()) {
                            workingRightVertex = deepCopyVisitor.visit(rightVertex, null);

                        } else {
                            workingRightVertex = internalVertexSupplier.get();
                            workingRightVertex.getLabels().clear();
                            workingRightVertex.getLabels().add(vertexLabelIterator.next());
                        }
                        previousRightVertex = workingRightVertex;

                        // And build the edge to add to our path.
                        EdgeDescriptor edgeDescriptor = new EdgeDescriptor(edgeDirection, PatternType.EDGE,
                                edgeLabelSet, null, deepCopyVisitor.visit(edgeVariableExpr, null), null, null);
                        pathPattern.add(new EdgePatternExpr(workingLeftVertex, workingRightVertex, edgeDescriptor));
                    }
                    expandedPathPatterns.add(pathPattern);
                }
            }
        }
        return expandedPathPatterns;
    }

    public static List<EdgePatternExpr> deepCopyPathPattern(List<EdgePatternExpr> pathPattern,
            GraphixDeepCopyVisitor deepCopyVisitor) throws CompilationException {
        List<EdgePatternExpr> deepCopyEdgeList = new ArrayList<>();
        for (EdgePatternExpr edgePatternExpr : pathPattern) {
            deepCopyEdgeList.add(deepCopyVisitor.visit(edgePatternExpr, null));
        }
        return deepCopyEdgeList;
    }

    public static void replaceVertexInIterator(Map<VertexPatternExpr, VertexPatternExpr> replaceMap,
            ListIterator<VertexPatternExpr> elementIterator, GraphixDeepCopyVisitor deepCopyVisitor)
            throws CompilationException {
        while (elementIterator.hasNext()) {
            VertexPatternExpr nextElement = elementIterator.next();
            if (replaceMap.containsKey(nextElement)) {
                VertexPatternExpr replacementElement = replaceMap.get(nextElement);
                elementIterator.set((VertexPatternExpr) replacementElement.accept(deepCopyVisitor, null));
            }
        }
    }

    public static void replaceEdgeInIterator(Map<EdgePatternExpr, EdgePatternExpr> replaceMap,
            ListIterator<EdgePatternExpr> elementIterator, GraphixDeepCopyVisitor deepCopyVisitor)
            throws CompilationException {
        // Build a map of vertices to vertices from our replace map.
        Map<VertexPatternExpr, VertexPatternExpr> replaceVertexMap = new HashMap<>();
        for (Map.Entry<EdgePatternExpr, EdgePatternExpr> mapEntry : replaceMap.entrySet()) {
            VertexPatternExpr keyLeftVertex = mapEntry.getKey().getLeftVertex();
            VertexPatternExpr keyRightVertex = mapEntry.getKey().getRightVertex();
            VertexPatternExpr valueLeftVertex = mapEntry.getValue().getLeftVertex();
            VertexPatternExpr valueRightVertex = mapEntry.getValue().getRightVertex();
            replaceVertexMap.put(keyLeftVertex, valueLeftVertex);
            replaceVertexMap.put(keyRightVertex, valueRightVertex);
        }

        while (elementIterator.hasNext()) {
            EdgePatternExpr nextElement = elementIterator.next();
            if (replaceMap.containsKey(nextElement)) {
                EdgePatternExpr replacementElement = replaceMap.get(nextElement);
                elementIterator.set((EdgePatternExpr) replacementElement.accept(deepCopyVisitor, null));

            } else {
                if (replaceVertexMap.containsKey(nextElement.getLeftVertex())) {
                    VertexPatternExpr replaceLeftVertex = replaceVertexMap.get(nextElement.getLeftVertex());
                    nextElement.setLeftVertex(deepCopyVisitor.visit(replaceLeftVertex, null));
                }
                if (replaceVertexMap.containsKey(nextElement.getRightVertex())) {
                    VertexPatternExpr replaceRightVertex = replaceVertexMap.get(nextElement.getRightVertex());
                    nextElement.setRightVertex(deepCopyVisitor.visit(replaceRightVertex, null));
                }
            }
        }
    }

    private CanonicalElementUtil() {
    }
}
