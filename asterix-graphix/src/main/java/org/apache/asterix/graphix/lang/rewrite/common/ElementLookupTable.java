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
package org.apache.asterix.graphix.lang.rewrite.common;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.graphix.common.metadata.EdgeIdentifier;
import org.apache.asterix.graphix.common.metadata.IElementIdentifier;
import org.apache.asterix.graphix.common.metadata.VertexIdentifier;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;

/**
 * Lookup table for {@link GraphElementDeclaration} instances, vertex keys, edge destination keys, and edge source
 * keys-- indexed by {@link IElementIdentifier} instances.
 */
public class ElementLookupTable implements Iterable<GraphElementDeclaration> {
    private final Map<IElementIdentifier, GraphElementDeclaration> graphElementDeclMap = new HashMap<>();
    private final Map<VertexIdentifier, List<List<String>>> vertexKeyMap = new HashMap<>();
    private final Map<EdgeIdentifier, List<List<String>>> edgeDestKeysMap = new HashMap<>();
    private final Map<EdgeIdentifier, List<List<String>>> edgeSourceKeysMap = new HashMap<>();

    public void put(IElementIdentifier identifier, GraphElementDeclaration graphElementDeclaration) {
        graphElementDeclMap.put(identifier, graphElementDeclaration);
    }

    public void putVertexKey(VertexIdentifier identifier, List<List<String>> primaryKey) {
        vertexKeyMap.put(identifier, primaryKey);
    }

    public void putEdgeKeys(EdgeIdentifier identifier, List<List<String>> sourceKey,
            List<List<String>> destinationKey) {
        edgeSourceKeysMap.put(identifier, sourceKey);
        edgeDestKeysMap.put(identifier, destinationKey);
    }

    public GraphElementDeclaration getElementDecl(IElementIdentifier identifier) {
        return graphElementDeclMap.get(identifier);
    }

    public List<List<String>> getVertexKey(VertexIdentifier identifier) {
        return vertexKeyMap.get(identifier);
    }

    public List<List<String>> getEdgeDestKey(EdgeIdentifier identifier) {
        return edgeDestKeysMap.get(identifier);
    }

    public List<List<String>> getEdgeSourceKey(EdgeIdentifier identifier) {
        return edgeSourceKeysMap.get(identifier);
    }

    @Override
    public Iterator<GraphElementDeclaration> iterator() {
        return graphElementDeclMap.values().iterator();
    }
}
