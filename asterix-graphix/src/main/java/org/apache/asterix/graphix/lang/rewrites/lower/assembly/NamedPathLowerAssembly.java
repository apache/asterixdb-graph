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
package org.apache.asterix.graphix.lang.rewrites.lower.assembly;

import static org.apache.asterix.lang.common.expression.ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.assembly.IExprAssembly;
import org.apache.asterix.graphix.lang.rewrites.lower.LowerSupplierNode;
import org.apache.asterix.graphix.lang.rewrites.record.PathRecord;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;

/**
 * Introduce named paths into the given assembly node. If a sub-path is found within a {@link PathPatternExpr}, we need
 * to merge the sub-path into the full-path. If the pattern only consists of a vertex, then a pseudo-path is formed with
 * special "dangling-vertex" edge.
 */
public class NamedPathLowerAssembly implements IExprAssembly<LowerSupplierNode> {
    private final List<PathPatternExpr> pathPatternExprList;

    public NamedPathLowerAssembly(List<PathPatternExpr> pathPatternExprList) {
        this.pathPatternExprList = pathPatternExprList;
    }

    @Override
    public LowerSupplierNode apply(LowerSupplierNode inputLowerNode) throws CompilationException {
        Map<String, Expression> qualifiedProjectionMap = inputLowerNode.getProjectionList().stream()
                .collect(Collectors.toMap(Projection::getName, Projection::getExpression));
        for (PathPatternExpr pathPatternExpr : pathPatternExprList) {
            if (pathPatternExpr.getVariableExpr() == null) {
                continue;
            }
            if (!pathPatternExpr.getEdgeExpressions().isEmpty()) {
                List<Expression> pathExpressionParts = new ArrayList<>();
                List<Expression> workingSimplePath = new ArrayList<>();

                for (EdgePatternExpr edgePatternExpr : pathPatternExpr.getEdgeExpressions()) {
                    EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
                    if (edgeDescriptor.getPatternType() == EdgeDescriptor.PatternType.EDGE) {
                        // "Raw" edges are first put into a list of path records.
                        VariableExpr leftVertexVar = edgePatternExpr.getLeftVertex().getVariableExpr();
                        VariableExpr rightVertexVar = edgePatternExpr.getRightVertex().getVariableExpr();
                        VariableExpr edgeVar = edgeDescriptor.getVariableExpr();

                        // Get the name of our path variables.
                        String edgeVarName = SqlppVariableUtil.toUserDefinedName(edgeVar.getVar().getValue());
                        String leftVarName = SqlppVariableUtil.toUserDefinedName(leftVertexVar.getVar().getValue());
                        String rightVarName = SqlppVariableUtil.toUserDefinedName(rightVertexVar.getVar().getValue());

                        // We must qualify each of our expressions.
                        Expression leftExpr = qualifiedProjectionMap.get(leftVarName);
                        Expression rightExpr = qualifiedProjectionMap.get(rightVarName);
                        Expression edgeExpr = qualifiedProjectionMap.get(edgeVarName);
                        workingSimplePath.add(new PathRecord(leftExpr, edgeExpr, rightExpr).getExpression());

                    } else { // edgeDescriptor.getEdgeClass() == EdgeDescriptor.PatternType.PATH
                        // If we encounter a sub-path, ARRAY_CONCAT our existing path and sub-path.
                        pathExpressionParts.add(new ListConstructor(ORDERED_LIST_CONSTRUCTOR, workingSimplePath));
                        VariableExpr subPathVar = edgeDescriptor.getVariableExpr();
                        String subPathVarName = SqlppVariableUtil.toUserDefinedName(subPathVar.getVar().getValue());
                        pathExpressionParts.add(qualifiedProjectionMap.get(subPathVarName));
                        workingSimplePath = new ArrayList<>();
                    }
                }

                // If did not encounter a sub-path, build our simple path here.
                if (!workingSimplePath.isEmpty()) {
                    pathExpressionParts.add(new ListConstructor(ORDERED_LIST_CONSTRUCTOR, workingSimplePath));
                }

                String pathVariableName = pathPatternExpr.getVariableExpr().getVar().getValue();
                if (pathExpressionParts.size() == 1) {
                    // If we only have one expression, we do not need to ARRAY_CONCAT.
                    inputLowerNode.getProjectionList().add(new Projection(Projection.Kind.NAMED_EXPR,
                            pathExpressionParts.get(0), SqlppVariableUtil.toUserDefinedName(pathVariableName)));

                } else {
                    // ...otherwise, ARRAY_CONCAT (we do this to preserve the order of our path records).
                    FunctionSignature functionSignature = new FunctionSignature(BuiltinFunctions.ARRAY_CONCAT);
                    Projection projection = new Projection(Projection.Kind.NAMED_EXPR,
                            new CallExpr(functionSignature, pathExpressionParts),
                            SqlppVariableUtil.toUserDefinedName(pathVariableName));
                    inputLowerNode.getProjectionList().add(projection);
                }

            } else {
                // We only have one vertex (no edges). Attach a singleton list of a single-field record.
                VariableExpr danglingVertexVar = pathPatternExpr.getVertexExpressions().get(0).getVariableExpr();
                String danglingVarName = SqlppVariableUtil.toUserDefinedName(danglingVertexVar.getVar().getValue());
                Expression danglingExpr = qualifiedProjectionMap.get(danglingVarName);
                List<Expression> pathExpressions = List.of(new PathRecord(danglingExpr).getExpression());
                inputLowerNode.getProjectionList().add(new Projection(Projection.Kind.NAMED_EXPR,
                        new ListConstructor(ORDERED_LIST_CONSTRUCTOR, pathExpressions),
                        SqlppVariableUtil.toUserDefinedName(pathPatternExpr.getVariableExpr().getVar().getValue())));
            }
        }
        return inputLowerNode;
    }
}
