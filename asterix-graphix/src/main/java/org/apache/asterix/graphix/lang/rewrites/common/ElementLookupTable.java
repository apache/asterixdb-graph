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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.graphix.lang.statement.GraphElementDecl;
import org.apache.asterix.graphix.lang.struct.ElementLabel;

/**
 * Lookup table for {@link GraphElementDecl} instances, vertex keys, edge destination keys / labels, and edge source
 * keys / labels-- indexed by {@link T} instances.
 */
public class ElementLookupTable<T> implements Iterable<GraphElementDecl> {
    private final Map<T, GraphElementDecl> graphElementDeclMap = new HashMap<>();
    private final Map<T, List<List<String>>> vertexKeyMap = new HashMap<>();
    private final Map<T, List<List<String>>> edgeDestKeysMap = new HashMap<>();
    private final Map<T, List<List<String>>> edgeSourceKeysMap = new HashMap<>();

    // In addition to keys, maintain the source and destination labels associated with each edge.
    private final Map<T, ElementLabel> edgeDestLabelMap = new HashMap<>();
    private final Map<T, ElementLabel> edgeSourceLabelMap = new HashMap<>();

    public void put(T identifier, GraphElementDecl graphElementDecl) {
        if (graphElementDeclMap.containsKey(identifier)) {
            graphElementDeclMap.get(identifier).getBodies().addAll(graphElementDecl.getBodies());

        } else {
            graphElementDeclMap.put(identifier, graphElementDecl);
        }
    }

    public void putVertexKey(T identifier, List<List<String>> primaryKey) {
        vertexKeyMap.put(identifier, primaryKey);
    }

    public void putEdgeKeys(T identifier, List<List<String>> sourceKey, List<List<String>> destinationKey) {
        // We know that a source key and destination key are the same for all edge definitions of the same label.
        edgeSourceKeysMap.put(identifier, sourceKey);
        edgeDestKeysMap.put(identifier, destinationKey);
    }

    public void putEdgeLabels(T identifier, ElementLabel sourceLabel, ElementLabel destinationLabel) {
        // Multiple edge definitions for the same label will always have the same source and destination labels.
        edgeSourceLabelMap.put(identifier, sourceLabel);
        edgeDestLabelMap.put(identifier, destinationLabel);
    }

    public GraphElementDecl getElementDecl(T identifier) {
        return graphElementDeclMap.get(identifier);
    }

    public List<List<String>> getVertexKey(T identifier) {
        return vertexKeyMap.get(identifier);
    }

    public List<List<String>> getEdgeDestKeys(T identifier) {
        return edgeDestKeysMap.get(identifier);
    }

    public List<List<String>> getEdgeSourceKeys(T identifier) {
        return edgeSourceKeysMap.get(identifier);
    }

    public ElementLabel getEdgeDestLabel(T identifier) {
        return edgeDestLabelMap.get(identifier);
    }

    public ElementLabel getEdgeSourceLabel(T identifier) {
        return edgeSourceLabelMap.get(identifier);
    }

    @Override
    public Iterator<GraphElementDecl> iterator() {
        return graphElementDeclMap.values().iterator();
    }
}
