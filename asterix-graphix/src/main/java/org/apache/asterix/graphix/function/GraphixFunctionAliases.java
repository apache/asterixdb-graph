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

import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class GraphixFunctionAliases {
    private static final Map<String, FunctionIdentifier> functionAliasMap;

    static {
        functionAliasMap = new HashMap<>();

        // Build aliases for our vertex + edge functions.
        functionAliasMap.put("label", GraphixFunctionIdentifiers.ELEMENT_LABEL);

        // Build aliases for edge functions.
        functionAliasMap.put("source-key", GraphixFunctionIdentifiers.EDGE_SOURCE_KEY);
        functionAliasMap.put("dest-key", GraphixFunctionIdentifiers.EDGE_DEST_KEY);
        functionAliasMap.put("edge-destination-key", GraphixFunctionIdentifiers.EDGE_DEST_KEY);
        functionAliasMap.put("destination-key", GraphixFunctionIdentifiers.EDGE_DEST_KEY);
        functionAliasMap.put("dir", GraphixFunctionIdentifiers.EDGE_DIRECTION);
        functionAliasMap.put("direction", GraphixFunctionIdentifiers.EDGE_DIRECTION);
        functionAliasMap.put("edge-source-vertex", GraphixFunctionIdentifiers.EDGE_SOURCE_VERTEX);
        functionAliasMap.put("source-vertex", GraphixFunctionIdentifiers.EDGE_SOURCE_VERTEX);
        functionAliasMap.put("edge-dest-vertex", GraphixFunctionIdentifiers.EDGE_DEST_VERTEX);
        functionAliasMap.put("dest-vertex", GraphixFunctionIdentifiers.EDGE_DEST_VERTEX);
        functionAliasMap.put("edge-destination-vertex", GraphixFunctionIdentifiers.EDGE_DEST_VERTEX);
        functionAliasMap.put("destination-vertex", GraphixFunctionIdentifiers.EDGE_DEST_VERTEX);

        // Build aliases for path functions.
        functionAliasMap.put("hop-count", GraphixFunctionIdentifiers.PATH_HOP_COUNT);
        functionAliasMap.put("edge-count", GraphixFunctionIdentifiers.PATH_HOP_COUNT);
        functionAliasMap.put("labels", GraphixFunctionIdentifiers.PATH_LABELS);
        functionAliasMap.put("vertices", GraphixFunctionIdentifiers.PATH_VERTICES);
        functionAliasMap.put("edges", GraphixFunctionIdentifiers.PATH_EDGES);
    }

    /**
     * @return {@link FunctionIdentifier} associated with the given function alias. Null otherwise.
     */
    public static FunctionIdentifier getFunctionIdentifier(String aliasName) {
        return functionAliasMap.getOrDefault(aliasName, null);
    }
}
