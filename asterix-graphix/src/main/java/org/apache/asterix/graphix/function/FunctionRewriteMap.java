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
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.function.rewrite.EdgeVertexFunctionRewrite;
import org.apache.asterix.graphix.function.rewrite.IFunctionRewrite;
import org.apache.asterix.graphix.function.rewrite.PathEdgesFunctionRewrite;
import org.apache.asterix.graphix.function.rewrite.PathHopCountFunctionRewrite;
import org.apache.asterix.graphix.function.rewrite.PathLabelsFunctionRewrite;
import org.apache.asterix.graphix.function.rewrite.PathVerticesFunctionRewrite;
import org.apache.asterix.graphix.lang.rewrites.record.EdgeRecord;
import org.apache.asterix.graphix.lang.rewrites.record.IElementRecord;
import org.apache.asterix.graphix.lang.rewrites.record.VertexRecord;
import org.apache.asterix.graphix.lang.rewrites.visitor.GraphixFunctionCallVisitor;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * @see GraphixFunctionCallVisitor
 */
public class FunctionRewriteMap {
    private final static Map<FunctionIdentifier, IFunctionRewrite> graphixFunctionMap;

    static {
        graphixFunctionMap = new HashMap<>();

        // Add our element function rewrites.
        graphixFunctionMap.put(GraphixFunctionIdentifiers.ELEMENT_LABEL, (c, a) -> {
            final Identifier detailSuffix = new Identifier(IElementRecord.ELEMENT_DETAIL_NAME);
            final Identifier labelSuffix = new Identifier(IElementRecord.ELEMENT_LABEL_FIELD_NAME);
            FieldAccessor detailAccess = new FieldAccessor(a.get(0), detailSuffix);
            return new FieldAccessor(detailAccess, labelSuffix);
        });

        // Add our vertex function rewrites.
        graphixFunctionMap.put(GraphixFunctionIdentifiers.VERTEX_PROPERTIES, (c, a) -> {
            FunctionSignature functionSignature = new FunctionSignature(BuiltinFunctions.RECORD_REMOVE);
            LiteralExpr elementDetailName = new LiteralExpr(new StringLiteral(IElementRecord.ELEMENT_DETAIL_NAME));
            LiteralExpr vertexDetailName = new LiteralExpr(new StringLiteral(VertexRecord.VERTEX_DETAIL_NAME));
            CallExpr removeElementDetailExpr = new CallExpr(functionSignature, List.of(a.get(0), elementDetailName));
            return new CallExpr(functionSignature, List.of(removeElementDetailExpr, vertexDetailName));
        });
        graphixFunctionMap.put(GraphixFunctionIdentifiers.VERTEX_KEY, (c, a) -> {
            final Identifier detailSuffix = new Identifier(VertexRecord.VERTEX_DETAIL_NAME);
            final Identifier keySuffix = new Identifier(VertexRecord.PRIMARY_KEY_FIELD_NAME);
            FieldAccessor detailAccess = new FieldAccessor(a.get(0), detailSuffix);
            return new FieldAccessor(detailAccess, keySuffix);
        });
        graphixFunctionMap.put(GraphixFunctionIdentifiers.VERTEX_DETAIL, (c, a) -> {
            final Identifier elementDetailSuffix = new Identifier(IElementRecord.ELEMENT_DETAIL_NAME);
            final Identifier vertexDetailSuffix = new Identifier(VertexRecord.VERTEX_DETAIL_NAME);
            FieldAccessor elementDetailAccess = new FieldAccessor(a.get(0), elementDetailSuffix);
            FieldAccessor vertexDetailAccess = new FieldAccessor(a.get(0), vertexDetailSuffix);
            FunctionSignature functionSignature = new FunctionSignature(BuiltinFunctions.RECORD_MERGE);
            return new CallExpr(functionSignature, List.of(elementDetailAccess, vertexDetailAccess));
        });

        // Add our edge function rewrites.
        graphixFunctionMap.put(GraphixFunctionIdentifiers.EDGE_PROPERTIES, (c, a) -> {
            FunctionSignature functionSignature = new FunctionSignature(BuiltinFunctions.RECORD_REMOVE);
            LiteralExpr elementDetailName = new LiteralExpr(new StringLiteral(IElementRecord.ELEMENT_DETAIL_NAME));
            LiteralExpr edgeDetailName = new LiteralExpr(new StringLiteral(EdgeRecord.EDGE_DETAIL_NAME));
            CallExpr removeElementDetailExpr = new CallExpr(functionSignature, List.of(a.get(0), elementDetailName));
            return new CallExpr(functionSignature, List.of(removeElementDetailExpr, edgeDetailName));
        });
        graphixFunctionMap.put(GraphixFunctionIdentifiers.EDGE_SOURCE_KEY, (c, a) -> {
            final Identifier detailSuffix = new Identifier(EdgeRecord.EDGE_DETAIL_NAME);
            final Identifier sourceKeySuffix = new Identifier(EdgeRecord.SOURCE_KEY_FIELD_NAME);
            FieldAccessor detailAccess = new FieldAccessor(a.get(0), detailSuffix);
            return new FieldAccessor(detailAccess, sourceKeySuffix);
        });
        graphixFunctionMap.put(GraphixFunctionIdentifiers.EDGE_DEST_KEY, (c, a) -> {
            final Identifier detailSuffix = new Identifier(EdgeRecord.EDGE_DETAIL_NAME);
            final Identifier destKeySuffix = new Identifier(EdgeRecord.DEST_KEY_FIELD_NAME);
            FieldAccessor detailAccess = new FieldAccessor(a.get(0), detailSuffix);
            return new FieldAccessor(detailAccess, destKeySuffix);
        });
        graphixFunctionMap.put(GraphixFunctionIdentifiers.EDGE_DIRECTION, (c, a) -> {
            final Identifier detailSuffix = new Identifier(EdgeRecord.EDGE_DETAIL_NAME);
            final Identifier directionSuffix = new Identifier(EdgeRecord.DIRECTION_FIELD_NAME);
            FieldAccessor detailAccess = new FieldAccessor(a.get(0), detailSuffix);
            return new FieldAccessor(detailAccess, directionSuffix);
        });
        graphixFunctionMap.put(GraphixFunctionIdentifiers.EDGE_DETAIL, (c, a) -> {
            final Identifier elementDetailSuffix = new Identifier(IElementRecord.ELEMENT_DETAIL_NAME);
            final Identifier edgeDetailSuffix = new Identifier(EdgeRecord.EDGE_DETAIL_NAME);
            FieldAccessor elementDetailAccess = new FieldAccessor(a.get(0), elementDetailSuffix);
            FieldAccessor edgeDetailAccess = new FieldAccessor(a.get(0), edgeDetailSuffix);
            FunctionSignature functionSignature = new FunctionSignature(BuiltinFunctions.RECORD_MERGE);
            return new CallExpr(functionSignature, List.of(elementDetailAccess, edgeDetailAccess));
        });
        graphixFunctionMap.put(GraphixFunctionIdentifiers.EDGE_SOURCE_VERTEX, new EdgeVertexFunctionRewrite(true));
        graphixFunctionMap.put(GraphixFunctionIdentifiers.EDGE_DEST_VERTEX, new EdgeVertexFunctionRewrite(false));

        // Add our path function rewrites.
        graphixFunctionMap.put(GraphixFunctionIdentifiers.PATH_HOP_COUNT, new PathHopCountFunctionRewrite());
        graphixFunctionMap.put(GraphixFunctionIdentifiers.PATH_LABELS, new PathLabelsFunctionRewrite());
        graphixFunctionMap.put(GraphixFunctionIdentifiers.PATH_VERTICES, new PathVerticesFunctionRewrite());
        graphixFunctionMap.put(GraphixFunctionIdentifiers.PATH_EDGES, new PathEdgesFunctionRewrite());
    }

    public static IFunctionRewrite getFunctionRewrite(FunctionIdentifier functionIdentifier) {
        return graphixFunctionMap.getOrDefault(functionIdentifier, null);
    }
}
