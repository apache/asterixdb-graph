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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
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
 */
public class EdgeDependencyGraph implements Iterable<Iterable<EdgePatternExpr>> {
    private final List<Map<Identifier, List<Identifier>>> adjacencyMaps;
    private final Map<Identifier, EdgePatternExpr> edgePatternMap;

    public EdgeDependencyGraph(List<Map<Identifier, List<Identifier>>> adjacencyMaps,
            Map<Identifier, EdgePatternExpr> edgePatternMap) {
        this.adjacencyMaps = adjacencyMaps;
        this.edgePatternMap = edgePatternMap;
    }

    @Override
    public Iterator<Iterable<EdgePatternExpr>> iterator() {
        List<Iterable<EdgePatternExpr>> edgePatternIterables = new ArrayList<>();
        Set<Identifier> globalVisitedSet = new HashSet<>();

        for (Map<Identifier, List<Identifier>> adjacencyMap : adjacencyMaps) {
            Deque<Identifier> edgeStack = new ArrayDeque<>();
            Set<Identifier> localVisitedSet = new HashSet<>();

            if (!adjacencyMap.entrySet().isEmpty()) {
                if (globalVisitedSet.isEmpty()) {
                    // We start with the first inserted edge inserted into our graph, and continue from there.
                    Iterator<Map.Entry<Identifier, List<Identifier>>> mapIterator = adjacencyMap.entrySet().iterator();
                    Map.Entry<Identifier, List<Identifier>> seedEdge = mapIterator.next();
                    edgeStack.addFirst(seedEdge.getKey());
                    edgeStack.addAll(seedEdge.getValue());
                    while (mapIterator.hasNext()) {
                        Map.Entry<Identifier, List<Identifier>> followingEdge = mapIterator.next();
                        for (Identifier followingEdgeDependent : followingEdge.getValue()) {
                            if (!edgeStack.contains(followingEdgeDependent)) {
                                edgeStack.addLast(followingEdgeDependent);
                            }
                        }
                    }

                } else {
                    // This is not our first pass. Find a connecting edge to seed our stack.
                    Set<Identifier> disconnectedExploredEdgeSet = new LinkedHashSet<>();
                    for (Map.Entry<Identifier, List<Identifier>> workingEdge : adjacencyMap.entrySet()) {
                        for (Identifier workingEdgeDependent : workingEdge.getValue()) {
                            if (globalVisitedSet.contains(workingEdgeDependent) && edgeStack.isEmpty()) {
                                edgeStack.addFirst(workingEdgeDependent);
                                localVisitedSet.add(workingEdgeDependent);

                            } else if (!globalVisitedSet.contains(workingEdgeDependent)) {
                                disconnectedExploredEdgeSet.add(workingEdgeDependent);

                            } else {
                                localVisitedSet.add(workingEdgeDependent);
                            }
                        }
                        if (workingEdge.getValue().isEmpty() && globalVisitedSet.contains(workingEdge.getKey())) {
                            localVisitedSet.add(workingEdge.getKey());

                        } else if (workingEdge.getValue().isEmpty()) {
                            disconnectedExploredEdgeSet.add(workingEdge.getKey());
                        }
                    }
                    disconnectedExploredEdgeSet.forEach(edgeStack::addLast);
                }

                // Build our iterable.
                globalVisitedSet.addAll(edgeStack);
                edgePatternIterables.add(() -> new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        // We are done once we have visited every node.
                        return localVisitedSet.size() != adjacencyMap.size();
                    }

                    @Override
                    public EdgePatternExpr next() {
                        Identifier edgeIdentifier = edgeStack.removeFirst();
                        for (Identifier dependency : adjacencyMap.get(edgeIdentifier)) {
                            if (!localVisitedSet.contains(dependency)) {
                                edgeStack.addFirst(dependency);
                            }
                        }
                        localVisitedSet.add(edgeIdentifier);
                        return edgePatternMap.get(edgeIdentifier);
                    }
                });

            } else {
                edgePatternIterables.add(Collections.emptyList());
            }
        }

        return edgePatternIterables.iterator();
    }
}
