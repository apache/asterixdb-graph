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
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.LowerListClause;
import org.apache.asterix.graphix.lang.clause.LowerSwitchClause;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseCollection;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.base.IVisitorExtension;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.AbstractCallExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.parser.ScopeChecker;
import org.apache.asterix.lang.common.rewrites.VariableSubstitutionEnvironment;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * @see LowerListClause
 */
public class LowerListClauseExtension implements IGraphixVisitorExtension {
    private final ClauseCollection clauseCollection;
    private final LowerListClause lowerListClause;

    public LowerListClauseExtension(LowerListClause lowerListClause) {
        this.clauseCollection = lowerListClause.getClauseCollection();
        this.lowerListClause = lowerListClause;
    }

    public LowerListClause getLowerListClause() {
        return lowerListClause;
    }

    @Override
    public Expression simpleExpressionDispatch(ILangVisitor<Expression, ILangExpression> simpleExpressionVisitor,
            ILangExpression argument) throws CompilationException {
        for (AbstractClause workingClause : clauseCollection) {
            if (workingClause instanceof LowerSwitchClause) {
                LowerSwitchClause lowerSwitchClause = (LowerSwitchClause) workingClause;
                simpleExpressionVisitor.visit(lowerSwitchClause.getVisitorExtension(), argument);

            } else {
                workingClause.accept(simpleExpressionVisitor, argument);
            }
        }
        return null;
    }

    @Override
    public Void freeVariableDispatch(ILangVisitor<Void, Collection<VariableExpr>> freeVariableVisitor,
            Collection<VariableExpr> freeVariables) throws CompilationException {
        Collection<VariableExpr> bindingVariables = new HashSet<>();
        Collection<VariableExpr> clauseFreeVariables = new HashSet<>();
        for (AbstractClause workingClause : clauseCollection) {
            clauseFreeVariables.clear();
            switch (workingClause.getClauseType()) {
                case LET_CLAUSE:
                    LetClause letClause = (LetClause) workingClause;
                    letClause.getBindingExpr().accept(freeVariableVisitor, clauseFreeVariables);
                    clauseFreeVariables.removeAll(bindingVariables);
                    freeVariables.addAll(clauseFreeVariables);
                    bindingVariables.add(letClause.getVarExpr());
                    break;

                case UNNEST_CLAUSE:
                    UnnestClause unnestClause = (UnnestClause) workingClause;
                    unnestClause.getRightExpression().accept(freeVariableVisitor, clauseFreeVariables);
                    clauseFreeVariables.removeAll(bindingVariables);
                    freeVariables.addAll(clauseFreeVariables);
                    bindingVariables.add(unnestClause.getRightVariable());
                    if (unnestClause.hasPositionalVariable()) {
                        bindingVariables.add(unnestClause.getPositionalVariable());
                    }
                    break;

                case JOIN_CLAUSE:
                    JoinClause joinClause = (JoinClause) workingClause;
                    joinClause.getRightExpression().accept(freeVariableVisitor, clauseFreeVariables);
                    clauseFreeVariables.removeAll(bindingVariables);
                    freeVariables.addAll(clauseFreeVariables);

                    // Handle our condition expression, which can reference its right variable.
                    Collection<VariableExpr> conditionFreeVariables = new HashSet<>();
                    joinClause.getConditionExpression().accept(freeVariableVisitor, conditionFreeVariables);
                    conditionFreeVariables.removeAll(bindingVariables);
                    conditionFreeVariables.remove(joinClause.getRightVariable());
                    bindingVariables.add(joinClause.getRightVariable());
                    if (joinClause.hasPositionalVariable()) {
                        conditionFreeVariables.remove(joinClause.getPositionalVariable());
                        bindingVariables.add(joinClause.getPositionalVariable());
                    }
                    freeVariables.addAll(conditionFreeVariables);
                    break;

                case WHERE_CLAUSE:
                    WhereClause whereClause = (WhereClause) workingClause;
                    whereClause.getWhereExpr().accept(freeVariableVisitor, clauseFreeVariables);
                    clauseFreeVariables.removeAll(bindingVariables);
                    freeVariables.addAll(clauseFreeVariables);
                    break;

                case EXTENSION:
                    if (workingClause instanceof LowerSwitchClause) {
                        LowerSwitchClause lowerSwitchClause = (LowerSwitchClause) workingClause;
                        IVisitorExtension visitorExtension = lowerSwitchClause.getVisitorExtension();
                        visitorExtension.freeVariableDispatch(freeVariableVisitor, freeVariables);
                        break;
                    }

                default:
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                            "Illegal clause found in the lower clause list!");
            }
        }
        return null;
    }

    @Override
    public Void bindingVariableDispatch(ILangVisitor<Void, Collection<VariableExpr>> bindingVariableVisitor,
            Collection<VariableExpr> bindingVariables) {
        for (AbstractClause workingClause : clauseCollection) {
            switch (workingClause.getClauseType()) {
                case LET_CLAUSE:
                    LetClause letClause = (LetClause) workingClause;
                    bindingVariables.add(letClause.getVarExpr());
                    break;

                case UNNEST_CLAUSE:
                case JOIN_CLAUSE:
                    AbstractBinaryCorrelateClause correlateClause = (AbstractBinaryCorrelateClause) workingClause;
                    bindingVariables.add(correlateClause.getRightVariable());
                    break;

                case EXTENSION:
                    if (workingClause instanceof LowerSwitchClause) {
                        LowerSwitchClause lowerSwitchClause = (LowerSwitchClause) workingClause;
                        bindingVariables.add(lowerSwitchClause.getClauseOutputEnvironment().getOutputVariable());
                        break;
                    }
            }
        }
        return null;
    }

    @Override
    public Expression variableScopeDispatch(ILangVisitor<Expression, ILangExpression> scopingVisitor,
            ILangExpression argument, ScopeChecker scopeChecker) throws CompilationException {
        for (AbstractClause workingClause : clauseCollection) {
            if (workingClause.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                // We do not extend the scope for our LET-CLAUSE nodes.
                LetClause letClause = (LetClause) workingClause;
                letClause.setBindingExpr(letClause.getBindingExpr().accept(scopingVisitor, letClause));
                VariableExpr letClauseVariable = letClause.getVarExpr();
                if (scopeChecker.getCurrentScope().findLocalSymbol(letClauseVariable.getVar().getValue()) != null) {
                    String varName = SqlppVariableUtil.toUserDefinedName(letClauseVariable.getVar().getValue());
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, letClauseVariable.getSourceLocation(),
                            "Duplicate alias definitions: " + varName);
                }
                scopeChecker.getCurrentScope().addNewVarSymbolToScope(letClauseVariable.getVar(),
                        Set.of(AbstractSqlppExpressionScopingVisitor.SqlppVariableAnnotation.CONTEXT_VARIABLE));

            } else if (workingClause instanceof LowerSwitchClause) {
                LowerSwitchClause lowerSwitchClause = (LowerSwitchClause) workingClause;
                IVisitorExtension visitorExtension = lowerSwitchClause.getVisitorExtension();
                visitorExtension.variableScopeDispatch(scopingVisitor, argument, scopeChecker);

            } else {
                workingClause.accept(scopingVisitor, argument);
            }
        }
        return null;
    }

    @Override
    public ILangExpression deepCopyDispatch(ILangVisitor<ILangExpression, Void> deepCopyVisitor)
            throws CompilationException {
        ClauseCollection copyCollection = new ClauseCollection(clauseCollection.getSourceLocation());
        for (AbstractClause clause : clauseCollection.getNonRepresentativeClauses()) {
            if (clause instanceof LowerSwitchClause) {
                LowerSwitchClause lowerSwitchClause = (LowerSwitchClause) clause;
                IVisitorExtension visitorExtension = lowerSwitchClause.getVisitorExtension();
                AbstractClause copiedClause = (AbstractClause) visitorExtension.deepCopyDispatch(deepCopyVisitor);
                copyCollection.addNonRepresentativeClause(copiedClause);

            } else {
                copyCollection.addNonRepresentativeClause((AbstractClause) clause.accept(deepCopyVisitor, null));
            }
        }
        for (LetClause clause : clauseCollection.getRepresentativeVertexBindings()) {
            LetClause copiedClause = (LetClause) clause.accept(deepCopyVisitor, null);
            copyCollection.addVertexBinding(copiedClause.getVarExpr(), copiedClause.getBindingExpr());
        }
        for (LetClause clause : clauseCollection.getRepresentativeEdgeBindings()) {
            LetClause copiedClause = (LetClause) clause.accept(deepCopyVisitor, null);
            copyCollection.addEdgeBinding(copiedClause.getVarExpr(), copiedClause.getBindingExpr());
        }
        for (LetClause clause : clauseCollection.getRepresentativePathBindings()) {
            LetClause copiedClause = (LetClause) clause.accept(deepCopyVisitor, null);
            copyCollection.addPathBinding(copiedClause.getVarExpr(), copiedClause.getBindingExpr());
        }
        for (AbstractBinaryCorrelateClause clause : clauseCollection.getUserDefinedCorrelateClauses()) {
            AbstractBinaryCorrelateClause copiedClause =
                    (AbstractBinaryCorrelateClause) clause.accept(deepCopyVisitor, null);
            copyCollection.addUserDefinedCorrelateClause(copiedClause);
        }

        // Note: a LOWER-LIST-CLAUSE is also the entry-point for a FROM-GRAPH-CLAUSE, so we return the latter.
        LowerListClause copyListClause = new LowerListClause(copyCollection);
        copyListClause.setSourceLocation(lowerListClause.getSourceLocation());
        FromGraphClause fromGraphClause = new FromGraphClause(copyListClause);
        fromGraphClause.setSourceLocation(lowerListClause.getSourceLocation());
        return fromGraphClause;
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
        for (AbstractClause workingClause : clauseCollection) {
            if (workingClause instanceof LowerSwitchClause) {
                LowerSwitchClause lowerSwitchClause = (LowerSwitchClause) workingClause;
                IVisitorExtension visitorExtension = lowerSwitchClause.getVisitorExtension();
                changed |= visitorExtension.inlineUDFsDispatch(inlineUDFsVisitor);

            } else {
                changed |= workingClause.accept(inlineUDFsVisitor, null);
            }
        }
        return changed;
    }

    @Override
    public Void gatherFunctionsDispatch(ILangVisitor<Void, Void> gatherFunctionsVisitor,
            Collection<? super AbstractCallExpression> functionCalls) throws CompilationException {
        for (AbstractClause workingClause : clauseCollection) {
            if (workingClause instanceof LowerSwitchClause) {
                LowerSwitchClause lowerSwitchClause = (LowerSwitchClause) workingClause;
                IVisitorExtension visitorExtension = lowerSwitchClause.getVisitorExtension();
                visitorExtension.gatherFunctionsDispatch(gatherFunctionsVisitor, functionCalls);

            } else {
                workingClause.accept(gatherFunctionsVisitor, null);
            }
        }
        return null;
    }

    @Override
    public Boolean checkSubqueryDispatch(ILangVisitor<Boolean, ILangExpression> checkSubqueryVisitor,
            ILangExpression argument) throws CompilationException {
        for (AbstractClause workingClause : clauseCollection) {
            if (workingClause instanceof LowerSwitchClause) {
                LowerSwitchClause lowerSwitchClause = (LowerSwitchClause) workingClause;
                IVisitorExtension visitorExtension = lowerSwitchClause.getVisitorExtension();
                if (visitorExtension.checkSubqueryDispatch(checkSubqueryVisitor, argument)) {
                    return true;
                }

            } else if (workingClause.accept(checkSubqueryVisitor, null)) {
                return true;
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
        for (AbstractClause workingClause : clauseCollection) {
            if (workingClause instanceof LowerSwitchClause) {
                LowerSwitchClause lowerSwitchClause = (LowerSwitchClause) workingClause;
                IVisitorExtension visitorExtension = lowerSwitchClause.getVisitorExtension();
                if (visitorExtension.checkNonFunctionalDispatch(checkNonFunctionalVisitor)) {
                    return true;
                }

            } else if (workingClause.accept(checkNonFunctionalVisitor, null)) {
                return true;
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
        return Kind.LOWER_LIST;
    }
}
