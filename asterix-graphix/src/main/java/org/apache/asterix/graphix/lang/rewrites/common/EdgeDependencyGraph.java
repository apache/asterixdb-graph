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
package org.apache.asterix.graphix.lang.rewrites.common;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.lang.common.struct.Identifier;

/**
 * A two-level ordered list of {@link EdgePatternExpr} instances, whose order depends on the input edge-dependency
 * graphs (represented as an adjacency list). Ideally, we want to return the smallest set of Hamilton
 * {@link EdgePatternExpr} paths, but this is an NP-hard problem. Instead, we find the **first** set of paths that
 * visit each {@link EdgePatternExpr} once. This inner-order is dependent on the order of the adjacency map iterators.
 *
 * @see org.apache.asterix.graphix.lang.rewrites.visitor.ElementAnalysisVisitor
 */
public class EdgeDependencyGraph implements Iterable<Iterable<EdgePatternExpr>> {
    private final List<Map<Identifier, List<Identifier>>> adjacencyMaps;
    private final Map<Identifier, EdgePatternExpr> edgePatternMap;

    public EdgeDependencyGraph(List<Map<Identifier, List<Identifier>>> adjacencyMaps,
            Map<Identifier, EdgePatternExpr> edgePatternMap) {
        this.adjacencyMaps = adjacencyMaps;
        this.edgePatternMap = edgePatternMap;
    }

    private Iterator<EdgePatternExpr> buildEdgeOrdering(Map<Identifier, List<Identifier>> adjacencyMap) {
        Set<Identifier> visitedSet = new HashSet<>();
        Deque<Identifier> edgeStack = new ArrayDeque<>();

        if (!adjacencyMap.entrySet().isEmpty()) {
            // We start with the first inserted edge into our graph.
            Iterator<Map.Entry<Identifier, List<Identifier>>> adjacencyMapIterator = adjacencyMap.entrySet().iterator();
            Map.Entry<Identifier, List<Identifier>> seedEdge = adjacencyMapIterator.next();
            edgeStack.addFirst(seedEdge.getKey());
            edgeStack.addAll(seedEdge.getValue());

            // Continue until all entries in our adjacency map have been inserted.
            while (adjacencyMapIterator.hasNext()) {
                Map.Entry<Identifier, List<Identifier>> followingEdge = adjacencyMapIterator.next();
                for (Identifier followingEdgeDependent : followingEdge.getValue()) {
                    if (!edgeStack.contains(followingEdgeDependent)) {
                        edgeStack.addLast(followingEdgeDependent);
                    }
                }
            }

            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    // We are done once we have visited every node.
                    return visitedSet.size() != adjacencyMap.size();
                }

                @Override
                public EdgePatternExpr next() {
                    Identifier edgeIdentifier = edgeStack.removeFirst();
                    for (Identifier dependency : adjacencyMap.get(edgeIdentifier)) {
                        if (!visitedSet.contains(dependency)) {
                            edgeStack.push(dependency);
                        }
                    }
                    visitedSet.add(edgeIdentifier);
                    return edgePatternMap.get(edgeIdentifier);
                }
            };

        } else {
            return Collections.emptyIterator();
        }
    }

    @Override
    public Iterator<Iterable<EdgePatternExpr>> iterator() {
        return adjacencyMaps.stream().map(m -> (Iterable<EdgePatternExpr>) () -> buildEdgeOrdering(m)).iterator();
    }
}
