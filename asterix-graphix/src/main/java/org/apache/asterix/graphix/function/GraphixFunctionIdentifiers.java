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
import java.util.Map;
import java.util.function.Consumer;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class GraphixFunctionIdentifiers {
    private static final Map<String, FunctionIdentifier> functionIdentifierMap;

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
    public static final FunctionIdentifier VERTEX_PROPERTIES =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "vertex-properties", 1);
    public static final FunctionIdentifier VERTEX_DETAIL =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "vertex-detail", 1);
    public static final FunctionIdentifier VERTEX_KEY =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "vertex-key", 1);

    // Functions that can be called on edges.
    public static final FunctionIdentifier EDGE_PROPERTIES =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "edge-properties", 1);
    public static final FunctionIdentifier EDGE_DETAIL =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "edge-detail", 1);
    public static final FunctionIdentifier EDGE_SOURCE_KEY =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "edge-source-key", 1);
    public static final FunctionIdentifier EDGE_DEST_KEY =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "edge-dest-key", 1);
    public static final FunctionIdentifier EDGE_DIRECTION =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "edge-direction", 1);
    public static final FunctionIdentifier EDGE_LEFT_TO_RIGHT_IF =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "edge-left-to-right-if", 3);
    public static final FunctionIdentifier EDGE_RIGHT_TO_LEFT_IF =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "edge-right-to-left-if", 3);

    // Functions that can be called on paths.
    public static final FunctionIdentifier PATH_HOP_COUNT =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "path-hop-count", 1);
    public static final FunctionIdentifier PATH_VERTICES =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "path-vertices", 1);
    public static final FunctionIdentifier PATH_EDGES =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "path-edges", 1);
    public static final FunctionIdentifier PATH_LABELS =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "path-labels", 1);

    static {
        functionIdentifierMap = new HashMap<>();

        // Register all the functions above.
        Consumer<FunctionIdentifier> functionRegister = f -> functionIdentifierMap.put(f.getName(), f);
        functionRegister.accept(ELEMENT_LABEL);
        functionRegister.accept(VERTEX_PROPERTIES);
        functionRegister.accept(VERTEX_DETAIL);
        functionRegister.accept(VERTEX_KEY);
        functionRegister.accept(EDGE_PROPERTIES);
        functionRegister.accept(EDGE_DETAIL);
        functionRegister.accept(EDGE_SOURCE_KEY);
        functionRegister.accept(EDGE_DEST_KEY);
        functionRegister.accept(EDGE_DIRECTION);
        functionRegister.accept(EDGE_LEFT_TO_RIGHT_IF);
        functionRegister.accept(EDGE_RIGHT_TO_LEFT_IF);
        functionRegister.accept(PATH_HOP_COUNT);
        functionRegister.accept(PATH_VERTICES);
        functionRegister.accept(PATH_EDGES);
        functionRegister.accept(PATH_LABELS);
    }
}
