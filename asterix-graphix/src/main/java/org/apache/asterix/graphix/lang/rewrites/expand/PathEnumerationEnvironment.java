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
package org.apache.asterix.graphix.lang.rewrites.expand;

import static org.apache.asterix.graphix.lang.rewrites.lower.assembly.IsomorphismLowerAssembly.populateVariableMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.lower.LowerSupplierContext;
import org.apache.asterix.graphix.lang.rewrites.lower.assembly.IsomorphismLowerAssembly;
import org.apache.asterix.graphix.lang.rewrites.record.PathRecord;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.VarIdentifier;

public class PathEnumerationEnvironment {
    private final LowerSupplierContext lowerSupplierContext;
    private final EdgePatternExpr edgePatternExpr;

    // We build the following as we decompose a path instance.
    private final Map<ElementLabel, List<VariableExpr>> collectedVertexVariables = new HashMap<>();
    private final Map<ElementLabel, List<VariableExpr>> collectedEdgeVariables = new HashMap<>();
    private final Set<VariableExpr> nonTerminalVertexVariables = new HashSet<>();
    private final List<PathRecord> generatedPathRecords = new ArrayList<>();

    // The following callbacks are invoked on publish.
    private final Consumer<List<Expression>> vertexConjunctsCallback;
    private final Consumer<List<Expression>> edgeConjunctsCallback;
    private final Consumer<List<LetClause>> reboundExpressionsCallback;
    private int internalVertexCursor;

    public PathEnumerationEnvironment(EdgePatternExpr edgePatternExpr, LowerSupplierContext lowerSupplierContext,
            Consumer<List<Expression>> vertexConjunctsCallback, Consumer<List<Expression>> edgeConjunctsCallback,
            Consumer<List<LetClause>> reboundExpressionsCallback) {
        this.lowerSupplierContext = lowerSupplierContext;
        this.edgePatternExpr = edgePatternExpr;
        this.vertexConjunctsCallback = vertexConjunctsCallback;
        this.edgeConjunctsCallback = edgeConjunctsCallback;
        this.reboundExpressionsCallback = reboundExpressionsCallback;
        this.internalVertexCursor = 0;
    }

    public void reset() {
        nonTerminalVertexVariables.clear();
        collectedVertexVariables.clear();
        collectedEdgeVariables.clear();
        generatedPathRecords.clear();
        this.internalVertexCursor = 0;
    }

    public void publishIsomorphism() {
        // Ensure that isomorphism between our internal vertices and our terminal vertices is applied.
        VertexPatternExpr leftVertex = edgePatternExpr.getLeftVertex();
        VertexPatternExpr rightVertex = edgePatternExpr.getRightVertex();
        VarIdentifier leftVertexVar = leftVertex.getVariableExpr().getVar();
        VarIdentifier rightVertexVar = rightVertex.getVariableExpr().getVar();
        populateVariableMap(leftVertex.getLabels(), new VariableExpr(leftVertexVar), collectedVertexVariables);
        populateVariableMap(rightVertex.getLabels(), new VariableExpr(rightVertexVar), collectedVertexVariables);

        // Add isomorphism constraints between all vertices and all edges.
        List<Expression> vertexConjuncts = collectedVertexVariables.values().stream()
                .map(v -> v.stream().map(w -> (Expression) w).collect(Collectors.toList()))
                .map(IsomorphismLowerAssembly::generateIsomorphismConjuncts).flatMap(Collection::stream).filter(c -> {
                    // Do not include the conjunct to compare the terminal vertices. These will be handled later.
                    OperatorExpr operatorExpr = (OperatorExpr) c;
                    VariableExpr leftSourceVar = (VariableExpr) operatorExpr.getExprList().get(0);
                    VariableExpr rightSourceVar = (VariableExpr) operatorExpr.getExprList().get(1);
                    return nonTerminalVertexVariables.contains(leftSourceVar)
                            || nonTerminalVertexVariables.contains(rightSourceVar);
                }).collect(Collectors.toList());
        List<Expression> edgeConjuncts = collectedEdgeVariables.values().stream()
                .map(e -> e.stream().map(w -> (Expression) w).collect(Collectors.toList()))
                .map(IsomorphismLowerAssembly::generateIsomorphismConjuncts).flatMap(Collection::stream)
                .collect(Collectors.toList());
        vertexConjunctsCallback.accept(vertexConjuncts);
        edgeConjunctsCallback.accept(edgeConjuncts);
    }

    public void publishRebindings() {
        // Bind our previous edge variable to our list of path records.
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        VariableExpr subPathVariable = new VariableExpr(edgeDescriptor.getVariableExpr().getVar());
        Expression subPathExpr = new ListConstructor(ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR,
                generatedPathRecords.stream().map(PathRecord::getExpression).collect(Collectors.toList()));
        reboundExpressionsCallback.accept(List.of(new LetClause(subPathVariable, subPathExpr)));
    }

    public VariableExpr getNewVertexVar() {
        VariableExpr vertexVariable = new VariableExpr(lowerSupplierContext.getNewVariable());
        VertexPatternExpr workingInternalVertex = edgePatternExpr.getInternalVertices().get(internalVertexCursor);
        populateVariableMap(workingInternalVertex.getLabels(), vertexVariable, collectedVertexVariables);
        nonTerminalVertexVariables.add(vertexVariable);
        return vertexVariable;
    }

    public VariableExpr getNewEdgeVar() {
        VariableExpr edgeVariable = new VariableExpr(lowerSupplierContext.getNewVariable());
        populateVariableMap(edgePatternExpr.getEdgeDescriptor().getEdgeLabels(), edgeVariable, collectedEdgeVariables);
        return edgeVariable;
    }

    public void buildPathRecord(VariableExpr leftVertexVar, VariableExpr edgeVar, VariableExpr rightVertexVar) {
        generatedPathRecords.add(new PathRecord(leftVertexVar, edgeVar, rightVertexVar));
    }
}
