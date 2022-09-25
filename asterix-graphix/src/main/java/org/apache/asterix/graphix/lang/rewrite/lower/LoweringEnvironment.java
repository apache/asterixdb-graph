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
package org.apache.asterix.graphix.lang.rewrite.lower;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.LowerListClause;
import org.apache.asterix.graphix.lang.clause.LowerSwitchClause;
import org.apache.asterix.graphix.lang.clause.LowerSwitchClause.ClauseInputEnvironment;
import org.apache.asterix.graphix.lang.clause.LowerSwitchClause.ClauseOutputEnvironment;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.action.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.ILowerListTransformer;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseCollection;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.CollectionTable;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.StateContainer;
import org.apache.asterix.graphix.lang.rewrite.util.LowerRewritingUtil;
import org.apache.asterix.graphix.lang.rewrite.visitor.GraphixDeepCopyVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.GraphixLoweringVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.VariableRemapCloneVisitor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IVisitorExtension;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateWithConditionClause;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.FreeVariableVisitor;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * @see GraphixLoweringVisitor
 */
public class LoweringEnvironment {
    private final GraphixRewritingContext graphixRewritingContext;
    private final GraphixDeepCopyVisitor graphixDeepCopyVisitor;
    private final ClauseCollection mainClauseCollection;
    private final GraphIdentifier graphIdentifier;
    private final SourceLocation sourceLocation;

    // The following are created through beginLeftMatch / beginTempLowerList / beginBranches.
    private ClauseCollection leftClauseCollection;
    private ClauseCollection tempClauseCollection;
    private ClauseCollection branchClauseCollection;
    private CollectionTable collectionTable;
    private boolean isInlineLegal;

    public LoweringEnvironment(GraphixRewritingContext graphixRewritingContext, GraphIdentifier graphIdentifier,
            SourceLocation sourceLocation) {
        this.mainClauseCollection = new ClauseCollection(sourceLocation);
        this.graphixDeepCopyVisitor = new GraphixDeepCopyVisitor();
        this.graphixRewritingContext = graphixRewritingContext;
        this.graphIdentifier = graphIdentifier;
        this.sourceLocation = sourceLocation;
        this.leftClauseCollection = null;
        this.tempClauseCollection = null;
        this.branchClauseCollection = null;
    }

    public GraphIdentifier getGraphIdentifier() {
        return graphIdentifier;
    }

    public void acceptAction(IEnvironmentAction environmentAction) throws CompilationException {
        environmentAction.apply(this);
    }

    public void acceptTransformer(ILowerListTransformer sequenceTransformer) throws CompilationException {
        // Fixed point lowering will always take precedence.
        sequenceTransformer.accept(Objects.requireNonNullElseGet(tempClauseCollection,
                () -> Objects.requireNonNullElseGet(branchClauseCollection,
                        () -> Objects.requireNonNullElse(leftClauseCollection, mainClauseCollection))));
    }

    public void setInlineLegal(boolean isInlineLegal) {
        this.isInlineLegal = isInlineLegal;
    }

    public boolean isInlineLegal() {
        return isInlineLegal;
    }

    public void beginLeftMatch() throws CompilationException {
        if (leftClauseCollection != null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                    "LEFT-MATCH lowering is currently in progress!");
        }
        leftClauseCollection = new ClauseCollection(sourceLocation);
    }

    public void endLeftMatch() throws CompilationException {
        if (leftClauseCollection.getNonRepresentativeClauses().isEmpty()) {
            // This is an extraneous LEFT-MATCH. Do not modify anything.
            leftClauseCollection = null;
            return;
        }

        // Build our substitution visitor and environment.
        VariableRemapCloneVisitor remapCloneVisitor = new VariableRemapCloneVisitor(graphixRewritingContext);
        VariableExpr nestingVariable = graphixRewritingContext.getGraphixVariableCopy("_LeftMatch");
        final Consumer<VariableExpr> substitutionAdder = v -> {
            VariableExpr nestingVariableCopy = new VariableExpr(nestingVariable.getVar());
            FieldAccessor fieldAccessor = new FieldAccessor(nestingVariableCopy, v.getVar());
            remapCloneVisitor.addSubstitution(v, fieldAccessor);
        };

        // Build up our projection list.
        List<Projection> projectionList = new ArrayList<>();
        List<AbstractClause> leftLowerClauses = leftClauseCollection.getNonRepresentativeClauses();
        for (AbstractClause workingClause : leftLowerClauses) {
            if (workingClause.getClauseType() == Clause.ClauseType.WHERE_CLAUSE) {
                continue;
            }

            // Identify our right variable.
            VariableExpr rightVariable;
            if (workingClause.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                rightVariable = ((LetClause) workingClause).getVarExpr();

            } else if (workingClause instanceof AbstractBinaryCorrelateClause) {
                rightVariable = ((AbstractBinaryCorrelateClause) workingClause).getRightVariable();

            } else {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Illegal clause found!");
            }
            projectionList.add(new Projection(Projection.Kind.NAMED_EXPR, rightVariable,
                    SqlppVariableUtil.toUserDefinedVariableName(rightVariable.getVar()).getValue()));
            substitutionAdder.accept(rightVariable);
        }

        // Nestle our clauses in a SELECT-BLOCK.
        LowerListClause leftLowerClause = new LowerListClause(leftClauseCollection);
        SelectClause selectClause = new SelectClause(null, new SelectRegular(projectionList), false);
        SelectBlock selectBlock = new SelectBlock(selectClause, null, null, null, null);
        selectBlock.setFromClause(new FromGraphClause(leftLowerClause));
        SetOperationInput setOperationInput = new SetOperationInput(selectBlock, null);
        SelectSetOperation selectSetOperation = new SelectSetOperation(setOperationInput, null);
        SelectExpression selectExpression = new SelectExpression(null, selectSetOperation, null, null, true);

        // Merge the collection we just built with our main sequence.
        IVisitorExtension visitorExtension = leftLowerClause.getVisitorExtension();
        Expression conditionExpression = generateJoinCondition(leftLowerClauses.listIterator(), visitorExtension);
        VariableExpr nestingVariableCopy = graphixDeepCopyVisitor.visit(nestingVariable, null);
        JoinClause leftJoinClause = new JoinClause(JoinType.LEFTOUTER, selectExpression, nestingVariableCopy, null,
                (Expression) remapCloneVisitor.substitute(conditionExpression), Literal.Type.MISSING);
        mainClauseCollection.addNonRepresentativeClause(leftJoinClause);

        // Introduce our representative variables back into our main sequence.
        for (LetClause representativeVertexBinding : leftClauseCollection.getRepresentativeVertexBindings()) {
            VariableExpr representativeVariable = representativeVertexBinding.getVarExpr();
            VariableExpr representativeVariableCopy = graphixDeepCopyVisitor.visit(representativeVariable, null);
            Expression rightExpression = representativeVertexBinding.getBindingExpr();
            Expression reboundExpression = (Expression) remapCloneVisitor.substitute(rightExpression);
            mainClauseCollection.addVertexBinding(representativeVariableCopy, reboundExpression);
        }
        for (LetClause representativeEdgeBinding : leftClauseCollection.getRepresentativeEdgeBindings()) {
            VariableExpr representativeVariable = representativeEdgeBinding.getVarExpr();
            VariableExpr representativeVariableCopy = graphixDeepCopyVisitor.visit(representativeVariable, null);
            Expression rightExpression = representativeEdgeBinding.getBindingExpr();
            Expression reboundExpression = (Expression) remapCloneVisitor.substitute(rightExpression);
            mainClauseCollection.addEdgeBinding(representativeVariableCopy, reboundExpression);
        }
        for (LetClause representativePathBinding : leftClauseCollection.getRepresentativePathBindings()) {
            VariableExpr representativeVariable = representativePathBinding.getVarExpr();
            VariableExpr representativeVariableCopy = graphixDeepCopyVisitor.visit(representativeVariable, null);
            Expression rightExpression = representativePathBinding.getBindingExpr();
            Expression reboundExpression = (Expression) remapCloneVisitor.substitute(rightExpression);
            mainClauseCollection.addPathBinding(representativeVariableCopy, reboundExpression);
        }

        // Do not reintroduce our vertex, edge, and path bindings.
        leftClauseCollection.getRepresentativeVertexBindings().clear();
        leftClauseCollection.getRepresentativeEdgeBindings().clear();
        leftClauseCollection.getRepresentativePathBindings().clear();
        leftClauseCollection = null;
    }

    public void beginTempLowerList() throws CompilationException {
        if (tempClauseCollection != null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                    "Temp branch lowering is currently in progress!");
        }
        tempClauseCollection = new ClauseCollection(sourceLocation);
    }

    public void endTempLowerList() {
        // We discard the collection we just built.
        tempClauseCollection = null;
    }

    public void beginBranches() throws CompilationException {
        if (collectionTable != null || branchClauseCollection != null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                    "Path branch lowering is currently in progress!");
        }
        collectionTable = new CollectionTable();
        branchClauseCollection = new ClauseCollection(sourceLocation);
    }

    public void flushBranch(EdgePatternExpr edgePatternExpr, boolean isJoiningLeftToRight) throws CompilationException {
        if (branchClauseCollection.getRepresentativeVertexBindings().size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                    "Only one vertex should exist in the clause collection!");
        }
        if (branchClauseCollection.getRepresentativeEdgeBindings().size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                    "Only one edge should exist in the clause collection!");
        }
        collectionTable.putCollection(edgePatternExpr, isJoiningLeftToRight, branchClauseCollection);
        branchClauseCollection = new ClauseCollection(sourceLocation);
    }

    public void endBranches(ClauseOutputEnvironment clauseOutputEnvironment,
            ClauseInputEnvironment clauseInputEnvironment, Map<ElementLabel, VariableExpr> inputMap,
            Map<ElementLabel, VariableExpr> outputMap, AliasLookupTable aliasLookupTable, SourceLocation sourceLocation)
            throws CompilationException {
        // Build the input map for our collection table.
        Map<ElementLabel, StateContainer> inputStateMap = new HashMap<>();
        for (Map.Entry<ElementLabel, VariableExpr> mapEntry : inputMap.entrySet()) {
            VariableExpr iterationAlias = aliasLookupTable.getIterationAlias(mapEntry.getValue());
            VariableExpr joinAlias = aliasLookupTable.getJoinAlias(mapEntry.getValue());
            inputStateMap.put(mapEntry.getKey(), new StateContainer(iterationAlias, joinAlias));
        }
        collectionTable.setInputMap(inputStateMap);

        // ...and the output map for out collection table.
        Map<ElementLabel, StateContainer> outputStateMap = new HashMap<>();
        for (Map.Entry<ElementLabel, VariableExpr> mapEntry : outputMap.entrySet()) {
            VariableExpr iterationAlias = aliasLookupTable.getIterationAlias(mapEntry.getValue());
            VariableExpr joinAlias = aliasLookupTable.getJoinAlias(mapEntry.getValue());
            outputStateMap.put(mapEntry.getKey(), new StateContainer(iterationAlias, joinAlias));
        }
        collectionTable.setOutputMap(outputStateMap);

        // Add our GRAPH-CLAUSE to our main sequence.
        LowerSwitchClause lowerSwitchClause =
                new LowerSwitchClause(collectionTable, clauseInputEnvironment, clauseOutputEnvironment);
        lowerSwitchClause.setSourceLocation(sourceLocation);
        mainClauseCollection.addNonRepresentativeClause(lowerSwitchClause);
        branchClauseCollection = null;
        collectionTable = null;
    }

    public void endLowering(FromGraphClause targetFromClause) {
        targetFromClause.setLowerClause(new LowerListClause(mainClauseCollection));
    }

    private static Expression generateJoinCondition(ListIterator<AbstractClause> lowerClauseIterator,
            IVisitorExtension visitorExtension) throws CompilationException {
        final List<Expression> joinConditionExpressions = new ArrayList<>();
        final Collection<VariableExpr> freeVariables = new HashSet<>();
        final FreeVariableVisitor freeVariableVisitor = new FreeVariableVisitor() {
            @Override
            public Void visit(IVisitorExtension visitorExtension, Collection<VariableExpr> freeVars)
                    throws CompilationException {
                Collection<VariableExpr> bindingVariables = new HashSet<>();
                Collection<VariableExpr> conditionFreeVars = new HashSet<>();
                Collection<VariableExpr> clauseFreeVars = new HashSet<>();
                while (lowerClauseIterator.hasNext()) {
                    AbstractClause lowerClause = lowerClauseIterator.next();
                    clauseFreeVars.clear();
                    if (lowerClause instanceof AbstractBinaryCorrelateClause) {
                        AbstractBinaryCorrelateClause correlateClause = (AbstractBinaryCorrelateClause) lowerClause;
                        correlateClause.getRightExpression().accept(this, clauseFreeVars);
                        if (lowerClause.getClauseType() == Clause.ClauseType.UNNEST_CLAUSE) {
                            clauseFreeVars.removeAll(bindingVariables);
                            if (!clauseFreeVars.isEmpty()) {
                                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                                        "Encountered UNNEST-CLAUSE with free variables.");
                            }

                        } else {
                            AbstractBinaryCorrelateWithConditionClause clauseWithCondition =
                                    (AbstractBinaryCorrelateWithConditionClause) correlateClause;
                            conditionFreeVars.clear();
                            clauseWithCondition.getConditionExpression().accept(this, conditionFreeVars);
                            conditionFreeVars.removeAll(bindingVariables);
                            conditionFreeVars.remove(correlateClause.getRightVariable());
                            if (!conditionFreeVars.isEmpty()) {
                                // We have found a JOIN with a free variable.
                                joinConditionExpressions.add(clauseWithCondition.getConditionExpression());
                                clauseWithCondition.setConditionExpression(new LiteralExpr(TrueLiteral.INSTANCE));
                            }
                            clauseFreeVars.addAll(conditionFreeVars);
                        }

                        // Adds binding variables.
                        bindingVariables.add(correlateClause.getRightVariable());
                        freeVars.addAll(clauseFreeVars);

                    } else if (lowerClause.getClauseType() == Clause.ClauseType.WHERE_CLAUSE) {
                        WhereClause whereClause = (WhereClause) lowerClause;
                        whereClause.getWhereExpr().accept(this, clauseFreeVars);
                        clauseFreeVars.removeAll(bindingVariables);
                        if (!clauseFreeVars.isEmpty()) {
                            joinConditionExpressions.add(whereClause.getWhereExpr());
                            lowerClauseIterator.remove();
                        }
                        freeVars.addAll(clauseFreeVars);

                    } else if (lowerClause.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                        LetClause letClause = (LetClause) lowerClause;
                        letClause.getBindingExpr().accept(this, clauseFreeVars);
                        clauseFreeVars.removeAll(bindingVariables);
                        bindingVariables.add(letClause.getVarExpr());
                        if (!clauseFreeVars.isEmpty()) {
                            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                                    "Encountered LET-CLAUSE with free variables.");
                        }
                    }
                }
                return null;
            }
        };
        freeVariableVisitor.visit(visitorExtension, freeVariables);
        return joinConditionExpressions.isEmpty() ? new LiteralExpr(TrueLiteral.INSTANCE)
                : LowerRewritingUtil.buildConnectedClauses(joinConditionExpressions, OperatorType.AND);
    }
}
