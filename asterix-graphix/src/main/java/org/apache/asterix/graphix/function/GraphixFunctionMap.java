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
import java.util.function.BiConsumer;

import org.apache.asterix.graphix.function.prepare.EdgeDestVertexPrepare;
import org.apache.asterix.graphix.function.prepare.EdgeDetailPrepare;
import org.apache.asterix.graphix.function.prepare.EdgeDirectionPrepare;
import org.apache.asterix.graphix.function.prepare.EdgeSourceVertexPrepare;
import org.apache.asterix.graphix.function.prepare.ElementLabelPrepare;
import org.apache.asterix.graphix.function.prepare.IFunctionPrepare;
import org.apache.asterix.graphix.function.prepare.VertexDetailPrepare;
import org.apache.asterix.graphix.function.rewrite.IFunctionRewrite;
import org.apache.asterix.graphix.function.rewrite.PathEdgesRewrite;
import org.apache.asterix.graphix.function.rewrite.PathHopCountRewrite;
import org.apache.asterix.graphix.function.rewrite.PathVerticesRewrite;
import org.apache.asterix.graphix.function.rewrite.SchemaAccessRewrite;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * @see org.apache.asterix.graphix.lang.rewrites.visitor.SchemaEnrichmentVisitor
 * @see org.apache.asterix.graphix.lang.rewrites.visitor.GraphixFunctionCallVisitor
 */
public class GraphixFunctionMap {
    private final static Map<FunctionIdentifier, IFunctionPrepare> graphixFunctionPrepareMap;
    private final static Map<FunctionIdentifier, IFunctionRewrite> graphixFunctionRewriteMap;

    static {
        graphixFunctionRewriteMap = new HashMap<>();
        graphixFunctionPrepareMap = new HashMap<>();

        // Add all of our function rewrites.
        BiConsumer<FunctionIdentifier, Identifier> rewriteInserter =
                (f, i) -> graphixFunctionRewriteMap.put(f, new SchemaAccessRewrite(f, i));
        rewriteInserter.accept(GraphixFunctionIdentifiers.ELEMENT_LABEL, ElementLabelPrepare.IDENTIFIER);
        rewriteInserter.accept(GraphixFunctionIdentifiers.VERTEX_DETAIL, VertexDetailPrepare.IDENTIFIER);
        rewriteInserter.accept(GraphixFunctionIdentifiers.EDGE_DETAIL, EdgeDetailPrepare.IDENTIFIER);
        rewriteInserter.accept(GraphixFunctionIdentifiers.EDGE_DIRECTION, EdgeDirectionPrepare.IDENTIFIER);
        rewriteInserter.accept(GraphixFunctionIdentifiers.EDGE_SOURCE_VERTEX, EdgeSourceVertexPrepare.IDENTIFIER);
        rewriteInserter.accept(GraphixFunctionIdentifiers.EDGE_DEST_VERTEX, EdgeDestVertexPrepare.IDENTIFIER);
        graphixFunctionRewriteMap.put(GraphixFunctionIdentifiers.PATH_HOP_COUNT, new PathHopCountRewrite());
        graphixFunctionRewriteMap.put(GraphixFunctionIdentifiers.PATH_EDGES, new PathEdgesRewrite());
        graphixFunctionRewriteMap.put(GraphixFunctionIdentifiers.PATH_VERTICES, new PathVerticesRewrite());

        // Add all of our function prepares (schema enrichment functions).
        graphixFunctionPrepareMap.put(GraphixFunctionIdentifiers.ELEMENT_LABEL, new ElementLabelPrepare());
        graphixFunctionPrepareMap.put(GraphixFunctionIdentifiers.VERTEX_DETAIL, new VertexDetailPrepare());
        graphixFunctionPrepareMap.put(GraphixFunctionIdentifiers.EDGE_DETAIL, new EdgeDetailPrepare());
        graphixFunctionPrepareMap.put(GraphixFunctionIdentifiers.EDGE_DIRECTION, new EdgeDirectionPrepare());
        graphixFunctionPrepareMap.put(GraphixFunctionIdentifiers.EDGE_SOURCE_VERTEX, new EdgeSourceVertexPrepare());
        graphixFunctionPrepareMap.put(GraphixFunctionIdentifiers.EDGE_DEST_VERTEX, new EdgeDestVertexPrepare());
    }

    public static IFunctionRewrite getFunctionRewrite(FunctionIdentifier functionIdentifier) {
        return graphixFunctionRewriteMap.getOrDefault(functionIdentifier, null);
    }

    public static IFunctionPrepare getFunctionPrepare(FunctionIdentifier functionIdentifier) {
        return graphixFunctionPrepareMap.getOrDefault(functionIdentifier, null);
    }
}
