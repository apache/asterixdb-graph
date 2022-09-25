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
package org.apache.asterix.graphix.lang.rewrite.lower.action;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.algebra.compiler.option.SemanticsNavigationOption;
import org.apache.asterix.graphix.algebra.compiler.option.SemanticsPatternOption;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.LowerListClause;
import org.apache.asterix.graphix.lang.clause.LowerSwitchClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.AliasLookupTable;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseCollection;
import org.apache.asterix.graphix.lang.rewrite.visitor.GraphixDeepCopyVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.VariableRemapCloneVisitor;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;

/**
 * Define the semantics of evaluating a basic graph pattern query (i.e. how much isomorphism do we enforce), and b)
 * b) the semantics of navigating between vertices (i.e. what type of uniqueness in the path should be enforced). We
 * assume that all elements are named at this point and that our {@link FromGraphClause} is in canonical form.
 * <p>
 * We enforce the following basic graph pattern query semantics (by default, we enforce total isomorphism):
 * <ul>
 *  <li>For total isomorphism, no vertex and no edge can appear more than once across all {@link MatchClause}
 *  nodes.</li>
 *  <li>For vertex-isomorphism, we enforce that no vertex can appear more than once across all {@link MatchClause}
 *  nodes.</li>
 *  <li>For edge-isomorphism, we enforce that no edge can appear more than once across all {@link MatchClause}
 *  nodes.</li>
 *  <li>For homomorphism, we enforce nothing. Edge adjacency is already implicitly preserved.</li>
 * </ul>
 * <p>
 * We enforce the following navigation query semantics (by default, we enforce no-repeat-anything):
 * <ul>
 *  <li>For no-repeat-vertices, no vertex instance can appear more than once in a path instance.</li>
 *  <li>For no-repeat-edges, no edge instance can appear more than once in a path instance.</li>
 *  <li>For no-repeat-anything, no vertex or edge instance can appear more than once in a path instance.</li>
 * </ul>
 */
public class MatchSemanticAction implements IEnvironmentAction {
    // We will walk through our FROM-GRAPH-CLAUSE and determine our isomorphism conjuncts.
    private final SemanticsNavigationOption navigationSemantics;
    private final SemanticsPatternOption patternSemantics;
    private final FromGraphClause fromGraphClause;
    private final AliasLookupTable aliasLookupTable;
    private final VariableRemapCloneVisitor remapCloneVisitor;
    private final GraphixDeepCopyVisitor deepCopyVisitor;

    public MatchSemanticAction(GraphixRewritingContext graphixRewritingContext, FromGraphClause fromGraphClause,
            AliasLookupTable aliasLookupTable) throws CompilationException {
        this.fromGraphClause = fromGraphClause;
        this.aliasLookupTable = aliasLookupTable;
        this.remapCloneVisitor = new VariableRemapCloneVisitor(graphixRewritingContext);
        this.deepCopyVisitor = new GraphixDeepCopyVisitor();

        // Determine our BGP query semantics.
        String patternConfigKeyName = SemanticsPatternOption.OPTION_KEY_NAME;
        this.patternSemantics = (SemanticsPatternOption) graphixRewritingContext.getSetting(patternConfigKeyName);

        // Determine our navigation query semantics.
        String navConfigKeyName = SemanticsNavigationOption.OPTION_KEY_NAME;
        this.navigationSemantics = (SemanticsNavigationOption) graphixRewritingContext.getSetting(navConfigKeyName);
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
                    return variableName.equals(SqlppVariableUtil.toUserDefinedName(v.getVar().getValue()));
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
            public Expression visit(VertexPatternExpr vertexPatternExpr, ILangExpression arg)
                    throws CompilationException {
                VariableExpr vertexVariable = vertexPatternExpr.getVariableExpr();
                VariableExpr iterationVariable = aliasLookupTable.getIterationAlias(vertexVariable);
                ElementLabel elementLabel = vertexPatternExpr.getLabels().iterator().next();
                iterationVariable.setSourceLocation(vertexVariable.getSourceLocation());
                populateVariableMap(elementLabel, iterationVariable, vertexVariableMap);
                return null;
            }

            @Override
            public Expression visit(EdgePatternExpr edgePatternExpr, ILangExpression arg) throws CompilationException {
                EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
                VariableExpr edgeVariable = edgeDescriptor.getVariableExpr();
                VariableExpr iterationVariable = aliasLookupTable.getIterationAlias(edgeVariable);
                ElementLabel elementLabel = edgeDescriptor.getEdgeLabels().iterator().next();
                iterationVariable.setSourceLocation(edgeVariable.getSourceLocation());
                populateVariableMap(elementLabel, iterationVariable, edgeVariableMap);
                return null;
            }
        }, null);

        // Construct our isomorphism conjuncts.
        List<OperatorExpr> isomorphismConjuncts = new ArrayList<>();
        if (patternSemantics == SemanticsPatternOption.ISOMORPHISM
                || patternSemantics == SemanticsPatternOption.VERTEX_ISOMORPHISM) {
            vertexVariableMap.values().stream().map(MatchSemanticAction::generateIsomorphismConjuncts)
                    .forEach(isomorphismConjuncts::addAll);
        }
        if (patternSemantics == SemanticsPatternOption.ISOMORPHISM
                || patternSemantics == SemanticsPatternOption.EDGE_ISOMORPHISM) {
            edgeVariableMap.values().stream().map(MatchSemanticAction::generateIsomorphismConjuncts)
                    .forEach(isomorphismConjuncts::addAll);
        }

        // Iterate through our clause sequence.
        remapCloneVisitor.resetSubstitutions();
        loweringEnvironment.acceptTransformer(new ILowerListTransformer() {
            private final Set<VariableExpr> visitedVariables = new HashSet<>();

            @Override
            public void accept(ClauseCollection lowerList) throws CompilationException {
                List<AbstractClause> nonRepresentativeClauses = lowerList.getNonRepresentativeClauses();
                ListIterator<AbstractClause> clauseIterator = nonRepresentativeClauses.listIterator();
                while (clauseIterator.hasNext()) {
                    AbstractClause workingClause = clauseIterator.next();
                    if (workingClause.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                        LetClause letClause = (LetClause) workingClause;
                        visitedVariables.add(letClause.getVarExpr());

                    } else if (workingClause.getClauseType() == Clause.ClauseType.EXTENSION) {
                        // Navigation semantics will be introduced at the plan translator.
                        LowerSwitchClause lowerSwitchClause = (LowerSwitchClause) workingClause;
                        lowerSwitchClause.setNavigationSemantics(navigationSemantics);

                    } else if (workingClause.getClauseType() == Clause.ClauseType.WHERE_CLAUSE) {
                        continue;

                    } else {
                        AbstractBinaryCorrelateClause correlateClause = (AbstractBinaryCorrelateClause) workingClause;
                        visitedVariables.add(correlateClause.getRightVariable());

                        // If we encounter a LEFT-JOIN, then we have created a LEFT-MATCH branch.
                        if (workingClause.getClauseType() == Clause.ClauseType.JOIN_CLAUSE) {
                            JoinClause joinClause = (JoinClause) workingClause;
                            if (joinClause.getJoinType() == JoinType.LEFTOUTER) {
                                acceptLeftMatchJoin(clauseIterator, joinClause);
                            }
                        }
                    }

                    // Only introduce our conjunct if we have visited both variables (eagerly).
                    Set<OperatorExpr> appliedIsomorphismConjuncts = new HashSet<>();
                    for (OperatorExpr isomorphismConjunct : isomorphismConjuncts) {
                        List<Expression> operandList = isomorphismConjunct.getExprList();
                        VariableExpr termVariable1 = ((VariableExpr) operandList.get(0));
                        VariableExpr termVariable2 = ((VariableExpr) operandList.get(1));
                        if (visitedVariables.contains(termVariable1) && visitedVariables.contains(termVariable2)) {
                            clauseIterator.add(new WhereClause(isomorphismConjunct));
                            appliedIsomorphismConjuncts.add(isomorphismConjunct);
                        }
                    }
                    isomorphismConjuncts.removeAll(appliedIsomorphismConjuncts);
                }
            }

            private void acceptLeftMatchJoin(ListIterator<AbstractClause> clauseIterator, JoinClause joinClause)
                    throws CompilationException {
                // We can make the following assumptions about our JOIN here (i.e. the casts here are valid).
                Expression rightExpression = joinClause.getRightExpression();
                SelectExpression selectExpression = (SelectExpression) rightExpression;
                SelectSetOperation selectSetOperation = selectExpression.getSelectSetOperation();
                SelectBlock selectBlock = selectSetOperation.getLeftInput().getSelectBlock();
                FromGraphClause fromGraphClause = (FromGraphClause) selectBlock.getFromClause();
                LowerListClause lowerClause = (LowerListClause) fromGraphClause.getLowerClause();
                Set<VariableExpr> localLiveVariables = new HashSet<>();
                ListIterator<AbstractClause> leftClauseIterator =
                        lowerClause.getClauseCollection().getNonRepresentativeClauses().listIterator();
                while (leftClauseIterator.hasNext()) {
                    AbstractClause workingClause = leftClauseIterator.next();
                    VariableExpr rightVariable;
                    if (workingClause.getClauseType() == Clause.ClauseType.WHERE_CLAUSE) {
                        continue;

                    } else if (workingClause.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                        rightVariable = ((LetClause) workingClause).getVarExpr();

                    } else {
                        rightVariable = ((AbstractBinaryCorrelateClause) workingClause).getRightVariable();
                    }

                    // Add our isomorphism conjunct to our main iterator.
                    localLiveVariables.add(rightVariable);
                    Set<OperatorExpr> appliedIsomorphismConjuncts = new HashSet<>();
                    for (OperatorExpr isomorphismConjunct : isomorphismConjuncts) {
                        List<Expression> operandList = isomorphismConjunct.getExprList();
                        VariableExpr termVariable1 = ((VariableExpr) operandList.get(0));
                        VariableExpr termVariable2 = ((VariableExpr) operandList.get(1));
                        VariableExpr leftVariable;
                        if (termVariable1.equals(rightVariable)) {
                            leftVariable = termVariable2;

                        } else if (termVariable2.equals(rightVariable)) {
                            leftVariable = termVariable1;

                        } else {
                            continue;
                        }

                        // Is our left variable introduced in our LEFT-CLAUSE-COLLECTION? Add the conjunct here.
                        if (localLiveVariables.contains(leftVariable)) {
                            leftClauseIterator.add(new WhereClause(isomorphismConjunct));
                            appliedIsomorphismConjuncts.add(isomorphismConjunct);
                            visitedVariables.add(rightVariable);
                            continue;
                        }

                        // Have we seen our left variable somewhere else? Add our conjunct in our main collection.
                        if (visitedVariables.contains(leftVariable)) {
                            VariableExpr joinVariable1 = deepCopyVisitor.visit(joinClause.getRightVariable(), null);
                            VariableExpr joinVariable2 = deepCopyVisitor.visit(joinClause.getRightVariable(), null);
                            FieldAccessor fieldAccessor1 = new FieldAccessor(joinVariable1, rightVariable.getVar());
                            FieldAccessor fieldAccessor2 = new FieldAccessor(joinVariable2, rightVariable.getVar());
                            remapCloneVisitor.addSubstitution(rightVariable, fieldAccessor1);
                            ILangExpression qualifiedConjunct = remapCloneVisitor.substitute(isomorphismConjunct);

                            // Our right variable can also be optional.
                            FunctionSignature functionSignature = new FunctionSignature(BuiltinFunctions.IS_MISSING);
                            CallExpr isMissingCallExpr = new CallExpr(functionSignature, List.of(fieldAccessor2));
                            OperatorExpr disjunctionExpr = new OperatorExpr();
                            disjunctionExpr.addOperator(OperatorType.OR);
                            disjunctionExpr.addOperand(isMissingCallExpr);
                            disjunctionExpr.addOperand((Expression) qualifiedConjunct);
                            clauseIterator.add(new WhereClause(disjunctionExpr));
                            appliedIsomorphismConjuncts.add(isomorphismConjunct);
                            visitedVariables.add(rightVariable);
                        }
                    }
                    isomorphismConjuncts.removeAll(appliedIsomorphismConjuncts);
                }
            }
        });
    }
}
