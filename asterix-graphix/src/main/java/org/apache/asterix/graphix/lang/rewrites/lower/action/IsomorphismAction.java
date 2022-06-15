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
package org.apache.asterix.graphix.lang.rewrites.lower.action;

import static org.apache.asterix.graphix.lang.rewrites.lower.action.IsomorphismAction.MatchEvaluationKind.COMPLETE_ISOMORPHISM;
import static org.apache.asterix.graphix.lang.rewrites.lower.action.IsomorphismAction.MatchEvaluationKind.EDGE_ISOMORPHISM;
import static org.apache.asterix.graphix.lang.rewrites.lower.action.IsomorphismAction.MatchEvaluationKind.HOMOMORPHISM;
import static org.apache.asterix.graphix.lang.rewrites.lower.action.IsomorphismAction.MatchEvaluationKind.VERTEX_ISOMORPHISM;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.algebra.compiler.provider.GraphixCompilationProvider;
import org.apache.asterix.graphix.lang.clause.CorrWhereClause;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrites.lower.LoweringAliasLookupTable;
import org.apache.asterix.graphix.lang.rewrites.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrites.lower.transform.CorrelatedClauseSequence;
import org.apache.asterix.graphix.lang.rewrites.lower.transform.ISequenceTransformer;
import org.apache.asterix.graphix.lang.rewrites.visitor.AbstractGraphixQueryVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.VariableSubstitutionVisitor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;

/**
 * Define which graph elements are *not* equal to each other. We assume that all elements are named at this point and
 * that our {@link FromGraphClause} is in canonical form. We enforce the following (by default, we enforce total
 * isomorphism):
 * 1. No vertex and no edge can appear more than once across all patterns of all {@link MatchClause} nodes.
 * 2. For vertex-isomorphism, we enforce that no vertex can appear more than once across all {@link MatchClause} nodes.
 * 3. For edge-isomorphism, we enforce that no edge can appear more than once across all {@link MatchClause} nodes.
 * 4. For homomorphism, we enforce nothing. Edge adjacency is already implicitly preserved.
 */
public class IsomorphismAction implements IEnvironmentAction {
    public enum MatchEvaluationKind {
        COMPLETE_ISOMORPHISM("isomorphism"),
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

    // We will walk through our FROM-GRAPH-CLAUSE and determine our isomorphism conjuncts.
    private final MatchEvaluationKind matchEvaluationKind;
    private final FromGraphClause fromGraphClause;
    private final LoweringAliasLookupTable aliasLookupTable;
    private final GraphixRewritingContext graphixRewritingContext;

    public IsomorphismAction(GraphixRewritingContext graphixRewritingContext, FromGraphClause fromGraphClause,
            LoweringAliasLookupTable aliasLookupTable) throws CompilationException {
        final String metadataConfigKeyName = GraphixCompilationProvider.MATCH_EVALUATION_METADATA_CONFIG;
        this.graphixRewritingContext = graphixRewritingContext;
        this.fromGraphClause = fromGraphClause;
        this.aliasLookupTable = aliasLookupTable;

        MetadataProvider metadataProvider = graphixRewritingContext.getMetadataProvider();
        if (metadataProvider.getConfig().containsKey(metadataConfigKeyName)) {
            String metadataConfigKeyValue = (String) metadataProvider.getConfig().get(metadataConfigKeyName);
            if (metadataConfigKeyValue.equalsIgnoreCase(COMPLETE_ISOMORPHISM.toString())) {
                this.matchEvaluationKind = COMPLETE_ISOMORPHISM;

            } else if (metadataConfigKeyValue.equalsIgnoreCase(VERTEX_ISOMORPHISM.toString())) {
                this.matchEvaluationKind = VERTEX_ISOMORPHISM;

            } else if (metadataConfigKeyValue.equalsIgnoreCase(EDGE_ISOMORPHISM.toString())) {
                this.matchEvaluationKind = EDGE_ISOMORPHISM;

            } else if (metadataConfigKeyValue.equalsIgnoreCase(HOMOMORPHISM.toString())) {
                this.matchEvaluationKind = HOMOMORPHISM;

            } else {
                throw new CompilationException(ErrorCode.ILLEGAL_SET_PARAMETER, metadataConfigKeyValue);
            }

        } else {
            this.matchEvaluationKind = COMPLETE_ISOMORPHISM;
        }
    }

    private static List<OperatorExpr> generateIsomorphismConjuncts(List<VariableExpr> variableList) {
        List<OperatorExpr> isomorphismConjuncts = new ArrayList<>();

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

    @Override
    public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
        Map<ElementLabel, List<VariableExpr>> vertexVariableMap = new HashMap<>();
        Map<ElementLabel, List<VariableExpr>> edgeVariableMap = new HashMap<>();

        // Populate the collections above.
        fromGraphClause.accept(new AbstractGraphixQueryVisitor() {
            private void populateVariableMap(ElementLabel label, VariableExpr variableExpr,
                    Map<ElementLabel, List<VariableExpr>> labelVariableMap) {
                Function<VariableExpr, Boolean> mapMatchFinder = v -> {
                    final String variableName = SqlppVariableUtil.toUserDefinedName(variableExpr.getVar().getValue());
                    String inputVariableName = SqlppVariableUtil.toUserDefinedName(v.getVar().getValue());
                    return variableName.equals(inputVariableName);
                };
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

            @Override
            public Expression visit(FromGraphClause fromGraphClause, ILangExpression arg) throws CompilationException {
                // We only want to explore the top level of our FROM-GRAPH-CLAUSEs.
                for (MatchClause matchClause : fromGraphClause.getMatchClauses()) {
                    matchClause.accept(this, arg);
                }
                return null;
            }

            @Override
            public Expression visit(VertexPatternExpr vertexPatternExpr, ILangExpression arg) {
                VarIdentifier vertexVariableID = vertexPatternExpr.getVariableExpr().getVar();
                VarIdentifier iterationVariableID = aliasLookupTable.getIterationAlias(vertexVariableID);
                ElementLabel elementLabel = vertexPatternExpr.getLabels().iterator().next();
                populateVariableMap(elementLabel, new VariableExpr(iterationVariableID), vertexVariableMap);
                return null;
            }

            @Override
            public Expression visit(EdgePatternExpr edgePatternExpr, ILangExpression arg) {
                VarIdentifier edgeVariableID = edgePatternExpr.getEdgeDescriptor().getVariableExpr().getVar();
                VarIdentifier iterationVariableID = aliasLookupTable.getIterationAlias(edgeVariableID);
                ElementLabel elementLabel = edgePatternExpr.getEdgeDescriptor().getEdgeLabels().iterator().next();
                populateVariableMap(elementLabel, new VariableExpr(iterationVariableID), edgeVariableMap);
                return null;
            }
        }, null);

        // Construct our isomorphism conjuncts.
        List<OperatorExpr> isomorphismConjuncts = new ArrayList<>();
        if (matchEvaluationKind == COMPLETE_ISOMORPHISM || matchEvaluationKind == VERTEX_ISOMORPHISM) {
            vertexVariableMap.values().stream().map(IsomorphismAction::generateIsomorphismConjuncts)
                    .forEach(isomorphismConjuncts::addAll);
        }
        if (matchEvaluationKind == COMPLETE_ISOMORPHISM || matchEvaluationKind == EDGE_ISOMORPHISM) {
            edgeVariableMap.values().stream().map(IsomorphismAction::generateIsomorphismConjuncts)
                    .forEach(isomorphismConjuncts::addAll);
        }

        // Iterate through our clause sequence, and introduce our isomorphism conjuncts eagerly.
        VariableSubstitutionVisitor substitutionVisitor = new VariableSubstitutionVisitor(graphixRewritingContext);
        loweringEnvironment.acceptTransformer(new ISequenceTransformer() {
            private final Set<VarIdentifier> visitedVariables = new HashSet<>();

            private void acceptLeftMatchJoin(ListIterator<AbstractBinaryCorrelateClause> clauseIterator,
                    JoinClause joinClause) throws CompilationException {
                // We can make the following assumptions about our JOIN here (i.e. the casts here are valid).
                Expression rightExpression = joinClause.getRightExpression();
                SelectExpression selectExpression = (SelectExpression) rightExpression;
                SelectSetOperation selectSetOperation = selectExpression.getSelectSetOperation();
                SelectBlock selectBlock = selectSetOperation.getLeftInput().getSelectBlock();
                FromTerm fromTerm = selectBlock.getFromClause().getFromTerms().get(0);
                for (AbstractBinaryCorrelateClause workingClause : fromTerm.getCorrelateClauses()) {
                    if (workingClause.getClauseType() == Clause.ClauseType.WHERE_CLAUSE) {
                        continue;
                    }
                    VarIdentifier rightVariable = workingClause.getRightVariable().getVar();

                    // Add our isomorphism conjunct to our main iterator.
                    Set<OperatorExpr> appliedIsomorphismConjuncts = new HashSet<>();
                    for (OperatorExpr isomorphismConjunct : isomorphismConjuncts) {
                        List<Expression> operandList = isomorphismConjunct.getExprList();
                        VarIdentifier termVariable1 = ((VariableExpr) operandList.get(0)).getVar();
                        VarIdentifier termVariable2 = ((VariableExpr) operandList.get(1)).getVar();
                        if (!termVariable1.equals(rightVariable) && !termVariable2.equals(rightVariable)) {
                            continue;
                        }

                        // Add a substitution for our right variable.
                        VariableExpr nestingVariableExpr1 = new VariableExpr(joinClause.getRightVariable().getVar());
                        VariableExpr nestingVariableExpr2 = new VariableExpr(joinClause.getRightVariable().getVar());
                        FieldAccessor fieldAccessor1 = new FieldAccessor(nestingVariableExpr1, rightVariable);
                        FieldAccessor fieldAccessor2 = new FieldAccessor(nestingVariableExpr2, rightVariable);
                        substitutionVisitor.addSubstitution(rightVariable, fieldAccessor1);
                        Expression qualifiedConjunct = substitutionVisitor.visit(isomorphismConjunct, null);

                        // Our right variable can also be optional.
                        FunctionSignature functionSignature = new FunctionSignature(BuiltinFunctions.IS_MISSING);
                        CallExpr isMissingCallExpr = new CallExpr(functionSignature, List.of(fieldAccessor2));
                        OperatorExpr disjunctionExpr = new OperatorExpr();
                        disjunctionExpr.addOperator(OperatorType.OR);
                        disjunctionExpr.addOperand(isMissingCallExpr);
                        disjunctionExpr.addOperand(qualifiedConjunct);
                        clauseIterator.add(new CorrWhereClause(disjunctionExpr));
                        appliedIsomorphismConjuncts.add(isomorphismConjunct);
                        visitedVariables.add(rightVariable);
                    }
                    isomorphismConjuncts.removeAll(appliedIsomorphismConjuncts);
                }
            }

            @Override
            public void accept(CorrelatedClauseSequence clauseSequence) throws CompilationException {
                ListIterator<AbstractBinaryCorrelateClause> clauseIterator = clauseSequence.getMainIterator();
                while (clauseIterator.hasNext()) {
                    AbstractBinaryCorrelateClause workingClause = clauseIterator.next();
                    if (workingClause.getClauseType() == Clause.ClauseType.WHERE_CLAUSE) {
                        continue;
                    }
                    visitedVariables.add(workingClause.getRightVariable().getVar());

                    // If we encounter a LEFT-JOIN, then we have created a LEFT-MATCH branch.
                    if (workingClause.getClauseType() == Clause.ClauseType.JOIN_CLAUSE) {
                        JoinClause joinClause = (JoinClause) workingClause;
                        if (joinClause.getJoinType() == JoinType.LEFTOUTER) {
                            acceptLeftMatchJoin(clauseIterator, joinClause);
                        }
                    }

                    // Only introduce our conjunct if we have visited both variables.
                    Set<OperatorExpr> appliedIsomorphismConjuncts = new HashSet<>();
                    for (OperatorExpr isomorphismConjunct : isomorphismConjuncts) {
                        List<Expression> operandList = isomorphismConjunct.getExprList();
                        VarIdentifier termVariable1 = ((VariableExpr) operandList.get(0)).getVar();
                        VarIdentifier termVariable2 = ((VariableExpr) operandList.get(1)).getVar();
                        if (visitedVariables.contains(termVariable1) && visitedVariables.contains(termVariable2)) {
                            clauseIterator.add(new CorrWhereClause(isomorphismConjunct));
                            appliedIsomorphismConjuncts.add(isomorphismConjunct);
                        }
                    }
                    isomorphismConjuncts.removeAll(appliedIsomorphismConjuncts);
                }
            }
        });
    }
}
