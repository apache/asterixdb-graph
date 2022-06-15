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
package org.apache.asterix.graphix.function;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class GraphixFunctionIdentifiers {
    private static final Map<String, FunctionIdentifier> functionIdentifierMap;
    private static final Set<FunctionIdentifier> vertexFunctionSet;
    private static final Set<FunctionIdentifier> edgeFunctionSet;
    private static final Set<FunctionIdentifier> pathFunctionSet;

    // Graphix functions should exist separate from the "ASTERIX_DV" dataverse.
    public static final DataverseName GRAPHIX_DV = DataverseName.createBuiltinDataverseName("graphix");

    /**
     * @return {@link FunctionIdentifier} associated with the given function name. Null otherwise.
     */
    public static FunctionIdentifier getFunctionIdentifier(String functionName) {
        return functionIdentifierMap.getOrDefault(functionName, null);
    }

    // Functions that can be called on vertices and edges.
    public static final FunctionIdentifier ELEMENT_LABEL =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "element-label", 1);

    // Functions that can be called on vertices.
    public static final FunctionIdentifier VERTEX_DETAIL =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "vertex-detail", 1);

    // Functions that can be called on edges.
    public static final FunctionIdentifier EDGE_DETAIL =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "edge-detail", 1);
    public static final FunctionIdentifier EDGE_DIRECTION =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "edge-direction", 1);
    public static final FunctionIdentifier EDGE_SOURCE_VERTEX =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "edge-source-vertex", 1);
    public static final FunctionIdentifier EDGE_DEST_VERTEX =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "edge-dest-vertex", 1);

    // Functions that can be called on paths.
    public static final FunctionIdentifier PATH_HOP_COUNT =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "path-hop-count", 1);
    public static final FunctionIdentifier PATH_VERTICES =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "path-vertices", 1);
    public static final FunctionIdentifier PATH_EDGES =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "path-edges", 1);

    static {
        // Register all the functions above.
        functionIdentifierMap = new HashMap<>();
        Consumer<FunctionIdentifier> functionRegister = f -> functionIdentifierMap.put(f.getName(), f);
        functionRegister.accept(ELEMENT_LABEL);
        functionRegister.accept(VERTEX_DETAIL);
        functionRegister.accept(EDGE_DETAIL);
        functionRegister.accept(EDGE_DIRECTION);
        functionRegister.accept(EDGE_SOURCE_VERTEX);
        functionRegister.accept(EDGE_DEST_VERTEX);
        functionRegister.accept(PATH_HOP_COUNT);
        functionRegister.accept(PATH_VERTICES);
        functionRegister.accept(PATH_EDGES);

        // Divide our functions into their respective input types.
        vertexFunctionSet = new HashSet<>();
        edgeFunctionSet = new HashSet<>();
        pathFunctionSet = new HashSet<>();
        vertexFunctionSet.add(ELEMENT_LABEL);
        vertexFunctionSet.add(VERTEX_DETAIL);
        edgeFunctionSet.add(ELEMENT_LABEL);
        edgeFunctionSet.add(EDGE_DETAIL);
        edgeFunctionSet.add(EDGE_DIRECTION);
        edgeFunctionSet.add(EDGE_SOURCE_VERTEX);
        edgeFunctionSet.add(EDGE_DEST_VERTEX);
        pathFunctionSet.add(PATH_HOP_COUNT);
        pathFunctionSet.add(PATH_VERTICES);
        pathFunctionSet.add(PATH_EDGES);
    }

    public static boolean isVertexFunction(FunctionIdentifier functionIdentifier) {
        return vertexFunctionSet.contains(functionIdentifier);
    }

    public static boolean isEdgeFunction(FunctionIdentifier functionIdentifier) {
        return edgeFunctionSet.contains(functionIdentifier);
    }

    public static boolean isPathFunction(FunctionIdentifier functionIdentifier) {
        return pathFunctionSet.contains(functionIdentifier);
    }
}
