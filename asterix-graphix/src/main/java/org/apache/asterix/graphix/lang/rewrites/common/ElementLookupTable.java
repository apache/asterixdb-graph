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

import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;

/**
 * Lookup table for {@link GraphElementDeclaration} instances, vertex keys, edge destination keys, and edge source
 * keys-- indexed by {@link GraphElementIdentifier} instances.
 */
public class ElementLookupTable implements Iterable<GraphElementDeclaration> {
    private final Map<GraphElementIdentifier, GraphElementDeclaration> graphElementDeclMap = new HashMap<>();
    private final Map<GraphElementIdentifier, List<List<String>>> vertexKeyMap = new HashMap<>();
    private final Map<GraphElementIdentifier, List<List<String>>> edgeDestKeysMap = new HashMap<>();
    private final Map<GraphElementIdentifier, List<List<String>>> edgeSourceKeysMap = new HashMap<>();

    public void put(GraphElementIdentifier identifier, GraphElementDeclaration graphElementDeclaration) {
        graphElementDeclMap.put(identifier, graphElementDeclaration);
    }

    public void putVertexKey(GraphElementIdentifier identifier, List<List<String>> primaryKey) {
        vertexKeyMap.put(identifier, primaryKey);
    }

    public void putEdgeKeys(GraphElementIdentifier identifier, List<List<String>> sourceKey,
            List<List<String>> destinationKey) {
        edgeSourceKeysMap.put(identifier, sourceKey);
        edgeDestKeysMap.put(identifier, destinationKey);
    }

    public GraphElementDeclaration getElementDecl(GraphElementIdentifier identifier) {
        return graphElementDeclMap.get(identifier);
    }

    public List<List<String>> getVertexKey(GraphElementIdentifier identifier) {
        return vertexKeyMap.get(identifier);
    }

    public List<List<String>> getEdgeDestKey(GraphElementIdentifier identifier) {
        return edgeDestKeysMap.get(identifier);
    }

    public List<List<String>> getEdgeSourceKey(GraphElementIdentifier identifier) {
        return edgeSourceKeysMap.get(identifier);
    }

    @Override
    public Iterator<GraphElementDeclaration> iterator() {
        return graphElementDeclMap.values().iterator();
    }
}
