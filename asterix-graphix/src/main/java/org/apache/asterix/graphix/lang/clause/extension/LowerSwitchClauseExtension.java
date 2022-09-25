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
package org.apache.asterix.graphix.lang.clause.extension;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.LowerListClause;
import org.apache.asterix.graphix.lang.clause.LowerSwitchClause;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseCollection;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.CollectionTable;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.StateContainer;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.AbstractCallExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.parser.ScopeChecker;
import org.apache.asterix.lang.common.rewrites.VariableSubstitutionEnvironment;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * @see LowerSwitchClause
 */
public class LowerSwitchClauseExtension implements IGraphixVisitorExtension {
    private final CollectionTable collectionLookupTable;
    private final LowerSwitchClause lowerSwitchClause;

    public LowerSwitchClauseExtension(LowerSwitchClause lowerSwitchClause) {
        this.collectionLookupTable = lowerSwitchClause.getCollectionLookupTable();
        this.lowerSwitchClause = lowerSwitchClause;
    }

    public LowerSwitchClause getLowerSwitchClause() {
        return lowerSwitchClause;
    }

    @Override
    public Expression simpleExpressionDispatch(ILangVisitor<Expression, ILangExpression> simpleExpressionVisitor,
            ILangExpression argument) throws CompilationException {
        for (ClauseCollection clauseCollection : collectionLookupTable) {
            for (AbstractClause workingClause : clauseCollection) {
                workingClause.accept(simpleExpressionVisitor, argument);
            }
        }
        return null;
    }

    @Override
    public Void freeVariableDispatch(ILangVisitor<Void, Collection<VariableExpr>> freeVariableVisitor,
            Collection<VariableExpr> freeVariables) throws CompilationException {
        Iterator<Pair<ElementLabel, List<CollectionTable.Entry>>> entryIterator = collectionLookupTable.entryIterator();
        while (entryIterator.hasNext()) {
            Pair<ElementLabel, List<CollectionTable.Entry>> tableEntry = entryIterator.next();
            StateContainer inputState = collectionLookupTable.getInputMap().get(tableEntry.first);
            for (CollectionTable.Entry entry : tableEntry.second) {
                ClauseCollection clauseCollection = entry.getClauseCollection();
                LowerListClause lowerListClause = new LowerListClause(clauseCollection);
                LowerListClauseExtension lowerListClauseExtension = new LowerListClauseExtension(lowerListClause);

                // The input variables to each branch are **not** free (in the context of this visitor).
                Set<VariableExpr> clauseCollectionFreeVars = new HashSet<>();
                lowerListClauseExtension.freeVariableDispatch(freeVariableVisitor, clauseCollectionFreeVars);
                clauseCollectionFreeVars.removeIf(v -> {
                    final VariableExpr inputJoinVariable = inputState.getJoinVariable();
                    final VariableExpr inputIterationVariable = inputState.getIterationVariable();
                    return v.equals(inputJoinVariable) || v.equals(inputIterationVariable);
                });
            }
        }
        return null;
    }

    @Override
    public Void bindingVariableDispatch(ILangVisitor<Void, Collection<VariableExpr>> bindingVariableVisitor,
            Collection<VariableExpr> bindingVariables) throws CompilationException {
        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, lowerSwitchClause.getSourceLocation(),
                "Binding variable dispatch invoked for LOWER-SWITCH-CLAUSE!");
    }

    @Override
    public Expression variableScopeDispatch(ILangVisitor<Expression, ILangExpression> scopingVisitor,
            ILangExpression argument, ScopeChecker scopeChecker) throws CompilationException {
        // Traverse our branches.
        Iterator<Pair<ElementLabel, List<CollectionTable.Entry>>> entryIterator = collectionLookupTable.entryIterator();
        while (entryIterator.hasNext()) {
            Pair<ElementLabel, List<CollectionTable.Entry>> tableEntry = entryIterator.next();
            StateContainer inputState = collectionLookupTable.getInputMap().get(tableEntry.first);
            for (CollectionTable.Entry entry : tableEntry.second) {
                ClauseCollection clauseCollection = entry.getClauseCollection();
                LowerListClause lowerListClause = new LowerListClause(clauseCollection);
                LowerListClauseExtension lowerListClauseExtension = new LowerListClauseExtension(lowerListClause);

                // Our input variables should only be visible to each branch.
                Scope newScope = scopeChecker.createNewScope();
                addVariableToScope(newScope, inputState.getJoinVariable().getVar());
                addVariableToScope(newScope, inputState.getIterationVariable().getVar());
                lowerListClauseExtension.variableScopeDispatch(scopingVisitor, argument, scopeChecker);
                scopeChecker.removeCurrentScope();
            }
        }

        // Introduce our output variable into scope.
        LowerSwitchClause.ClauseOutputEnvironment outputEnv = lowerSwitchClause.getClauseOutputEnvironment();
        addVariableToScope(scopeChecker.getCurrentScope(), outputEnv.getOutputVariable().getVar());
        return null;
    }

    @Override
    public ILangExpression deepCopyDispatch(ILangVisitor<ILangExpression, Void> deepCopyVisitor)
            throws CompilationException {
        CollectionTable copyTable = new CollectionTable();
        Iterator<Pair<ElementLabel, List<CollectionTable.Entry>>> entryIterator = collectionLookupTable.entryIterator();
        while (entryIterator.hasNext()) {
            Pair<ElementLabel, List<CollectionTable.Entry>> tableEntry = entryIterator.next();
            for (CollectionTable.Entry entry : tableEntry.second) {
                ClauseCollection clauseCollection = entry.getClauseCollection();
                LowerListClauseExtension llce = new LowerListClauseExtension(new LowerListClause(clauseCollection));
                FromGraphClause fromGraphClause = (FromGraphClause) llce.deepCopyDispatch(deepCopyVisitor);
                LowerListClause lowerClauseCopy = (LowerListClause) fromGraphClause.getLowerClause();
                copyTable.putCollection(tableEntry.first, entry.getEdgeLabel(), entry.getDestinationLabel(),
                        lowerClauseCopy.getClauseCollection(), entry.getEdgeDirection());
            }
        }
        copyTable.setInputMap(collectionLookupTable.getInputMap());
        copyTable.setOutputMap(collectionLookupTable.getOutputMap());
        LowerSwitchClause.ClauseOutputEnvironment outputEnv = lowerSwitchClause.getClauseOutputEnvironment();
        VariableExpr outputVariableExpr = outputEnv.getOutputVariable();
        VariableExpr copyOutputVariableExpr = (VariableExpr) outputVariableExpr.accept(deepCopyVisitor, null);
        LowerSwitchClause.ClauseOutputEnvironment copyOutputEnv = new LowerSwitchClause.ClauseOutputEnvironment(
                copyOutputVariableExpr, outputEnv.getOutputVertexIterationVariable(),
                outputEnv.getOutputVertexJoinVariable(), outputEnv.getPathVariable(), outputEnv.getEndingLabel());
        LowerSwitchClause.ClauseInputEnvironment inputEnv = lowerSwitchClause.getClauseInputEnvironment();
        VariableExpr inputVariableExpr = inputEnv.getInputVariable();
        VariableExpr copyInputVariableExpr = (VariableExpr) inputVariableExpr.accept(deepCopyVisitor, null);
        LowerSwitchClause.ClauseInputEnvironment copyInputEnv =
                new LowerSwitchClause.ClauseInputEnvironment(copyInputVariableExpr, inputEnv.getStartingLabel());
        LowerSwitchClause copyLowerSwitchClause = new LowerSwitchClause(copyTable, copyInputEnv, copyOutputEnv);
        copyLowerSwitchClause.setNavigationSemantics(lowerSwitchClause.getNavigationSemantics());
        copyLowerSwitchClause.setSourceLocation(lowerSwitchClause.getSourceLocation());
        return copyLowerSwitchClause;
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> remapCloneDispatch(
            ILangVisitor<Pair<ILangExpression, VariableSubstitutionEnvironment>, VariableSubstitutionEnvironment> remapCloneVisitor,
            VariableSubstitutionEnvironment substitutionEnvironment) {
        // TODO (GLENN): Finish the remap-clone dispatch.
        return null;
    }

    @Override
    public Boolean inlineUDFsDispatch(ILangVisitor<Boolean, Void> inlineUDFsVisitor) throws CompilationException {
        boolean changed = false;
        for (ClauseCollection clauseCollection : collectionLookupTable) {
            for (AbstractClause workingClause : clauseCollection) {
                changed |= workingClause.accept(inlineUDFsVisitor, null);
            }
        }
        return changed;
    }

    @Override
    public Void gatherFunctionsDispatch(ILangVisitor<Void, Void> gatherFunctionsVisitor,
            Collection<? super AbstractCallExpression> functionCalls) throws CompilationException {
        for (ClauseCollection clauseCollection : collectionLookupTable) {
            for (AbstractClause workingClause : clauseCollection) {
                workingClause.accept(gatherFunctionsVisitor, null);
            }
        }
        return null;
    }

    @Override
    public Boolean checkSubqueryDispatch(ILangVisitor<Boolean, ILangExpression> checkSubqueryVisitor,
            ILangExpression argument) throws CompilationException {
        for (ClauseCollection clauseCollection : collectionLookupTable) {
            for (AbstractClause workingClause : clauseCollection) {
                if (workingClause.accept(checkSubqueryVisitor, null)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Boolean check92AggregateDispatch(ILangVisitor<Boolean, ILangExpression> check92AggregateVisitor,
            ILangExpression argument) {
        return false;
    }

    @Override
    public Boolean checkNonFunctionalDispatch(ILangVisitor<Boolean, Void> checkNonFunctionalVisitor)
            throws CompilationException {
        for (ClauseCollection clauseCollection : collectionLookupTable) {
            for (AbstractClause workingClause : clauseCollection) {
                if (workingClause.accept(checkNonFunctionalVisitor, null)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Boolean checkDatasetOnlyDispatch(ILangVisitor<Boolean, VariableExpr> checkDatasetOnlyVisitor,
            VariableExpr datasetCandidate) {
        return false;
    }

    @Override
    public Kind getKind() {
        return Kind.LOWER_SWITCH;
    }

    private void addVariableToScope(Scope scope, VarIdentifier varIdentifier) throws CompilationException {
        if (scope.findLocalSymbol(varIdentifier.getValue()) != null) {
            String varName = SqlppVariableUtil.toUserDefinedName(varIdentifier.getValue());
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, lowerSwitchClause.getSourceLocation(),
                    "Duplicate alias definitions: " + varName);
        }
        scope.addNewVarSymbolToScope(varIdentifier, Set.of());
    }
}
