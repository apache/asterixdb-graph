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

import static org.apache.asterix.graphix.lang.rewrites.util.ClauseRewritingUtil.buildConnectedClauses;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.algebra.compiler.provider.GraphixCompilationProvider;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.assembly.IExprAssembly;
import org.apache.asterix.graphix.lang.rewrites.common.EdgeDependencyGraph;
import org.apache.asterix.graphix.lang.rewrites.lower.LowerSupplierNode;
import org.apache.asterix.graphix.lang.rewrites.visitor.ElementResolutionVisitor;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;

/**
 * Define which graph elements are *not* equal to each other. We assume that all elements are named at this point
 * (i.e. {@link ElementResolutionVisitor} must run before this). We enforce the following:
 * 1. By default, we enforce vertex isomorphism. No vertex (and consequently, no edge) can appear more than once across
 * all patterns of all {@link MatchClause} nodes.
 * 2. If edge-isomorphism is desired, then we only enforce that no edge can appear more than once across all
 * {@link MatchClause} nodes.
 * 3. If homomorphism is desired, then we enforce nothing. Edge adjacency is already implicitly preserved.
 * 4. If we have any LEFT-MATCH clauses, then we need to ensure that vertices that are only found in LEFT-MATCH clauses
 * are allowed to have a MISSING value.
 */
public class IsomorphismLowerAssembly implements IExprAssembly<LowerSupplierNode> {
    public enum MatchEvaluationKind {
        TOTAL_ISOMORPHISM("isomorphism"), // = VERTEX_ISOMORPHISM
        VERTEX_ISOMORPHISM("vertex-isomorphism"),
        EDGE_ISOMORPHISM("edge-isomorphism"),
        HOMOMORPHISM("homomorphism");

        // The user specifies the options above through "SET `graphix.match-evaluation` '...';"
        private final String metadataConfigOptionName;

        MatchEvaluationKind(String metadataConfigOptionName) {
            this.metadataConfigOptionName = metadataConfigOptionName;
        }

        @Override
        public String toString() {
            return metadataConfigOptionName;
        }
    }

    private final List<VertexPatternExpr> danglingVertices;
    private final List<VarIdentifier> optionalVertices;
    private final EdgeDependencyGraph edgeDependencyGraph;
    private final MetadataProvider metadataProvider;

    public IsomorphismLowerAssembly(List<VertexPatternExpr> danglingVertices, List<VarIdentifier> optionalVertices,
            EdgeDependencyGraph edgeDependencyGraph, MetadataProvider metadataProvider) {
        this.danglingVertices = danglingVertices;
        this.optionalVertices = optionalVertices;
        this.edgeDependencyGraph = edgeDependencyGraph;
        this.metadataProvider = metadataProvider;
    }

    public static MatchEvaluationKind getMatchEvaluationKind(MetadataProvider metadataProvider)
            throws CompilationException {
        final String metadataConfigKeyName = GraphixCompilationProvider.MATCH_EVALUATION_METADATA_CONFIG;

        if (metadataProvider.getConfig().containsKey(metadataConfigKeyName)) {
            String metadataConfigKeyValue = (String) metadataProvider.getConfig().get(metadataConfigKeyName);
            if (metadataConfigKeyValue.equalsIgnoreCase(MatchEvaluationKind.TOTAL_ISOMORPHISM.toString())) {
                return MatchEvaluationKind.TOTAL_ISOMORPHISM;

            } else if (metadataConfigKeyValue.equalsIgnoreCase(MatchEvaluationKind.VERTEX_ISOMORPHISM.toString())) {
                return MatchEvaluationKind.VERTEX_ISOMORPHISM;

            } else if (metadataConfigKeyValue.equalsIgnoreCase(MatchEvaluationKind.EDGE_ISOMORPHISM.toString())) {
                return MatchEvaluationKind.EDGE_ISOMORPHISM;

            } else if (metadataConfigKeyValue.equalsIgnoreCase(MatchEvaluationKind.HOMOMORPHISM.toString())) {
                return MatchEvaluationKind.HOMOMORPHISM;

            } else {
                throw new CompilationException(ErrorCode.ILLEGAL_SET_PARAMETER, metadataConfigKeyValue);
            }

        } else {
            return MatchEvaluationKind.VERTEX_ISOMORPHISM;
        }
    }

    public static List<Expression> generateIsomorphismConjuncts(List<Expression> variableList) {
        List<Expression> isomorphismConjuncts = new ArrayList<>();

        // Find all unique pairs from our list of variables.
        for (int i = 0; i < variableList.size(); i++) {
            for (int j = i + 1; j < variableList.size(); j++) {
                OperatorExpr inequalityConjunct = new OperatorExpr();
                inequalityConjunct.addOperator(OperatorType.NEQ);
                inequalityConjunct.addOperand(variableList.get(i));
                inequalityConjunct.addOperand(variableList.get(j));
                isomorphismConjuncts.add(inequalityConjunct);
            }
        }

        return isomorphismConjuncts;
    }

    public static void populateVariableMap(Set<ElementLabel> labels, VariableExpr variableExpr,
            Map<ElementLabel, List<VariableExpr>> labelVariableMap) {
        Function<VariableExpr, Boolean> mapMatchFinder = v -> {
            final String variableName = SqlppVariableUtil.toUserDefinedName(variableExpr.getVar().getValue());
            String inputVariableName = SqlppVariableUtil.toUserDefinedName(v.getVar().getValue());
            return variableName.equals(inputVariableName);
        };
        for (ElementLabel label : labels) {
            if (labelVariableMap.containsKey(label)) {
                if (labelVariableMap.get(label).stream().noneMatch(mapMatchFinder::apply)) {
                    labelVariableMap.get(label).add(variableExpr);
                }

            } else {
                List<VariableExpr> variableList = new ArrayList<>();
                variableList.add(variableExpr);
                labelVariableMap.put(label, variableList);
            }
        }
        if (labels.isEmpty()) {
            if (labelVariableMap.containsKey(null)) {
                if (labelVariableMap.get(null).stream().noneMatch(mapMatchFinder::apply)) {
                    labelVariableMap.get(null).add(variableExpr);
                }

            } else {
                List<VariableExpr> variableList = new ArrayList<>();
                variableList.add(variableExpr);
                labelVariableMap.put(null, variableList);
            }
        }
    }

    @Override
    public LowerSupplierNode apply(LowerSupplierNode inputLowerNode) throws CompilationException {
        Map<ElementLabel, List<VariableExpr>> vertexVariableMap = new HashMap<>();
        Map<ElementLabel, List<VariableExpr>> edgeVariableMap = new HashMap<>();
        Map<VariableExpr, Expression> qualifiedExprMap = new HashMap<>();

        // Fetch the edge and vertex variables, qualified through our input projections.
        Map<String, Expression> qualifiedProjectionMap = inputLowerNode.getProjectionList().stream()
                .collect(Collectors.toMap(Projection::getName, Projection::getExpression));
        for (VertexPatternExpr danglingVertex : danglingVertices) {
            VariableExpr vertexVarExpr = danglingVertex.getVariableExpr();
            String vertexVarName = SqlppVariableUtil.toUserDefinedName(vertexVarExpr.getVar().getValue());
            populateVariableMap(danglingVertex.getLabels(), vertexVarExpr, vertexVariableMap);
            qualifiedExprMap.put(vertexVarExpr, qualifiedProjectionMap.get(vertexVarName));
        }
        edgeDependencyGraph.forEach(p -> p.forEach(e -> {
            // Handle our left and right vertices.
            VariableExpr leftVertexVar = e.getLeftVertex().getVariableExpr();
            VariableExpr rightVertexVar = e.getRightVertex().getVariableExpr();
            populateVariableMap(e.getLeftVertex().getLabels(), leftVertexVar, vertexVariableMap);
            populateVariableMap(e.getRightVertex().getLabels(), rightVertexVar, vertexVariableMap);
            String leftVarName = SqlppVariableUtil.toUserDefinedName(leftVertexVar.getVar().getValue());
            String rightVarName = SqlppVariableUtil.toUserDefinedName(rightVertexVar.getVar().getValue());
            qualifiedExprMap.put(leftVertexVar, qualifiedProjectionMap.get(leftVarName));
            qualifiedExprMap.put(rightVertexVar, qualifiedProjectionMap.get(rightVarName));

            // If we have an edge (and not a sub-path), create a conjunct for this.
            if (e.getEdgeDescriptor().getPatternType() == EdgeDescriptor.PatternType.EDGE) {
                VariableExpr edgeVar = e.getEdgeDescriptor().getVariableExpr();
                populateVariableMap(e.getEdgeDescriptor().getEdgeLabels(), edgeVar, edgeVariableMap);
                String edgeVarName = SqlppVariableUtil.toUserDefinedName(edgeVar.getVar().getValue());
                qualifiedExprMap.put(edgeVar, qualifiedProjectionMap.get(edgeVarName));
            }
        }));

        // Construct our isomorphism conjuncts.
        List<Expression> isomorphismConjuncts = new ArrayList<>();
        switch (getMatchEvaluationKind(metadataProvider)) {
            case TOTAL_ISOMORPHISM:
            case VERTEX_ISOMORPHISM:
                vertexVariableMap.values().stream()
                        .map(v -> v.stream().map(qualifiedExprMap::get).collect(Collectors.toList()))
                        .map(IsomorphismLowerAssembly::generateIsomorphismConjuncts)
                        .forEach(isomorphismConjuncts::addAll);
                break;

            case EDGE_ISOMORPHISM:
                edgeVariableMap.values().stream()
                        .map(e -> e.stream().map(qualifiedExprMap::get).collect(Collectors.toList()))
                        .map(IsomorphismLowerAssembly::generateIsomorphismConjuncts)
                        .forEach(isomorphismConjuncts::addAll);
                break;
        }

        // Construct our IF_MISSING disjuncts.
        List<Expression> ifMissingExprList = null;
        if (!optionalVertices.isEmpty()) {
            ifMissingExprList = optionalVertices.stream().map(v -> {
                String qualifiedVarName = SqlppVariableUtil.toUserDefinedVariableName(v).getValue();
                Expression qualifiedVar = qualifiedProjectionMap.get(qualifiedVarName);
                return new CallExpr(new FunctionSignature(BuiltinFunctions.IS_MISSING), List.of(qualifiedVar));
            }).collect(Collectors.toList());
        }

        // Attach our WHERE-EXPR to the tail node.
        if (!isomorphismConjuncts.isEmpty() && ifMissingExprList == null) {
            Expression whereExpr = buildConnectedClauses(isomorphismConjuncts, OperatorType.AND);
            inputLowerNode.getWhereClauses().add(new WhereClause(whereExpr));

        } else if (isomorphismConjuncts.isEmpty() && ifMissingExprList != null) {
            Expression whereExpr = buildConnectedClauses(ifMissingExprList, OperatorType.OR);
            inputLowerNode.getWhereClauses().add(new WhereClause(whereExpr));

        } else if (!isomorphismConjuncts.isEmpty()) { // && ifMissingExprList != null
            Expression isomorphismExpr = buildConnectedClauses(isomorphismConjuncts, OperatorType.AND);
            Expression ifMissingExpr = buildConnectedClauses(ifMissingExprList, OperatorType.OR);
            List<Expression> whereExprDisjuncts = List.of(isomorphismExpr, ifMissingExpr);
            Expression whereExpr = buildConnectedClauses(whereExprDisjuncts, OperatorType.OR);
            inputLowerNode.getWhereClauses().add(new WhereClause(whereExpr));
        }
        return inputLowerNode;
    }
}
