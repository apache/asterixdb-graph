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
package org.apache.asterix.graphix.lang.rewrite.visitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * Rewrite a SELECT-EXPR and its SET-OP inputs to perform the following:
 * <ol>
 *  <li>Expose all user-defined variables from each SET-OP by modifying their SELECT-CLAUSE.</li>
 *  <li>Qualify the source SELECT-CLAUSE (before expansion) with the nesting variable.</li>
 *  <li>Qualify our output modifiers (ORDER-BY, LIMIT) with the nesting variable.</li>
 *  <li>Qualify our GROUP-BY / GROUP-AS (the grouping list) / HAVING / LET (after GROUP-BY) clauses with the nesting
 *  variable.</li>
 * </ol>
 */
public class PostCanonicalExpansionVisitor extends AbstractGraphixQueryVisitor {
    private final GraphixDeepCopyVisitor deepCopyVisitor;
    private final GraphixRewritingContext graphixRewritingContext;
    private final QualifyingVisitor qualifyingVisitor;

    // We require the following from our canonicalization pass.
    private final Collection<SelectBlock> generatedSelectBlocks;
    private final Collection<VariableExpr> sourceSelectLiveVariables;
    private final SelectBlock selectBlockExpansionSource;

    public PostCanonicalExpansionVisitor(GraphixRewritingContext graphixRewritingContext,
            SelectBlock selectBlockExpansionSource, Collection<SelectBlock> generatedSelectBlocks,
            Collection<VariableExpr> sourceSelectLiveVariables) {
        this.deepCopyVisitor = new GraphixDeepCopyVisitor();
        this.graphixRewritingContext = graphixRewritingContext;
        this.selectBlockExpansionSource = selectBlockExpansionSource;
        this.generatedSelectBlocks = generatedSelectBlocks;
        this.sourceSelectLiveVariables = sourceSelectLiveVariables;
        this.qualifyingVisitor = new QualifyingVisitor();
    }

    @Override
    public Expression visit(SelectExpression selectExpression, ILangExpression arg) throws CompilationException {
        VariableExpr iterationVariable = graphixRewritingContext.getGraphixVariableCopy("_Containing");

        // Modify the involved SELECT-CLAUSEs to output our user-live variables and remove any GROUP-BY clauses.
        selectExpression.getSelectSetOperation().accept(this, arg);
        FromTerm fromTerm = new FromTerm(selectExpression, iterationVariable, null, null);
        FromClause fromClause = new FromClause(List.of(fromTerm));
        fromTerm.setSourceLocation(selectExpression.getSourceLocation());
        fromClause.setSourceLocation(selectExpression.getSourceLocation());

        // Qualify the SELECT-CLAUSE given to us by our caller.
        qualifyingVisitor.qualifyingVar = deepCopyVisitor.visit(iterationVariable, null);
        SelectClause selectClause = selectBlockExpansionSource.getSelectClause();
        selectClause.accept(qualifyingVisitor, null);

        // Modify our output modifiers (if any) to qualify them with our output variable.
        OrderbyClause orderByClause = selectExpression.getOrderbyClause();
        LimitClause limitClause = selectExpression.getLimitClause();
        if (selectExpression.hasOrderby()) {
            orderByClause.accept(qualifyingVisitor, null);
        }
        if (selectExpression.hasLimit()) {
            limitClause.accept(qualifyingVisitor, null);
        }

        // Remove the output modifiers from our current SELECT-EXPR.
        boolean isSubquery = selectExpression.isSubquery();
        selectExpression.setLimitClause(null);
        selectExpression.setOrderbyClause(null);
        selectExpression.setSubquery(true);

        // Modify our GROUP-BY (if any) to qualify them with our output variable.
        GroupbyClause groupbyClause = selectBlockExpansionSource.getGroupbyClause();
        List<AbstractClause> letHavingClausesAfterGby = selectBlockExpansionSource.getLetHavingListAfterGroupby();
        if (selectBlockExpansionSource.hasGroupbyClause()) {
            groupbyClause.accept(qualifyingVisitor, null);

            // Ensure that any variables that may be used after the GROUP-BY don't see this qualifying variable.
            List<Pair<Expression, Identifier>> newGroupFieldList = new ArrayList<>();
            for (Pair<Expression, Identifier> expressionIdentifierPair : groupbyClause.getGroupFieldList()) {
                Expression newExpression = expressionIdentifierPair.first.accept(qualifyingVisitor, null);
                newGroupFieldList.add(new Pair<>(newExpression, expressionIdentifierPair.second));
            }
            VariableExpr iterationVariableCopy = deepCopyVisitor.visit(iterationVariable, null);
            newGroupFieldList.add(new Pair<>(iterationVariableCopy, iterationVariable.getVar()));
            groupbyClause.setGroupFieldList(newGroupFieldList);
        }
        if (selectBlockExpansionSource.hasLetHavingClausesAfterGroupby()) {
            for (AbstractClause abstractClause : letHavingClausesAfterGby) {
                abstractClause.accept(qualifyingVisitor, null);
            }
        }

        // Finalize our post-canonicalization: attach our SELECT-CLAUSE, GROUP-BY, output modifiers...
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, null, groupbyClause, null);
        selectBlock.getLetHavingListAfterGroupby().addAll(letHavingClausesAfterGby);
        selectBlock.setSourceLocation(selectBlockExpansionSource.getSourceLocation());
        SetOperationInput setOperationInput = new SetOperationInput(selectBlock, null);
        SelectSetOperation selectSetOperation = new SelectSetOperation(setOperationInput, null);
        selectSetOperation.setSourceLocation(selectBlockExpansionSource.getSourceLocation());
        SelectExpression newSelectExpression =
                new SelectExpression(null, selectSetOperation, orderByClause, limitClause, isSubquery);
        newSelectExpression.setSourceLocation(selectExpression.getSourceLocation());
        return newSelectExpression;
    }

    @Override
    public Expression visit(SelectSetOperation selectSetOperation, ILangExpression arg) throws CompilationException {
        // Only visit SET-OP-INPUTs if they were involved in our canonicalization.
        SetOperationInput leftInput = selectSetOperation.getLeftInput();
        if (leftInput.selectBlock() && generatedSelectBlocks.contains(leftInput.getSelectBlock())) {
            leftInput.getSelectBlock().accept(this, arg);
        }
        for (SetOperationRight setOperationRight : selectSetOperation.getRightInputs()) {
            SetOperationInput rightInput = setOperationRight.getSetOperationRightInput();
            if (rightInput.selectBlock() && generatedSelectBlocks.contains(rightInput.getSelectBlock())) {
                rightInput.getSelectBlock().accept(this, arg);
            }
        }
        return null;
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        if (selectBlock.hasGroupbyClause()) {
            selectBlock.setGroupbyClause(null);
        }
        if (selectBlock.hasLetHavingClausesAfterGroupby()) {
            selectBlock.getLetHavingListAfterGroupby().clear();
        }
        selectBlock.getSelectClause().accept(this, arg);
        return null;
    }

    @Override
    public Expression visit(SelectClause selectClause, ILangExpression arg) throws CompilationException {
        // We are going to throw away this SELECT-CLAUSE and return all user-live variables instead.
        List<Projection> newProjectionList = new ArrayList<>();
        for (VariableExpr userLiveVariable : sourceSelectLiveVariables) {
            String name = SqlppVariableUtil.toUserDefinedName(userLiveVariable.getVar().getValue());
            VariableExpr userLiveVariableCopy = deepCopyVisitor.visit(userLiveVariable, null);
            newProjectionList.add(new Projection(Projection.Kind.NAMED_EXPR, userLiveVariableCopy, name));
        }
        selectClause.setSelectElement(null);
        selectClause.setSelectRegular(new SelectRegular(newProjectionList));
        return null;
    }

    private class QualifyingVisitor extends AbstractGraphixQueryVisitor {
        private VariableExpr qualifyingVar;

        @Override
        public Expression visit(VariableExpr variableExpr, ILangExpression arg) throws CompilationException {
            if (sourceSelectLiveVariables.contains(variableExpr)) {
                VarIdentifier fieldAccessVar = SqlppVariableUtil.toUserDefinedVariableName(variableExpr.getVar());
                VariableExpr qualifyingVariableCopy = deepCopyVisitor.visit(qualifyingVar, null);
                FieldAccessor fieldAccessor = new FieldAccessor(qualifyingVariableCopy, fieldAccessVar);
                fieldAccessor.setSourceLocation(variableExpr.getSourceLocation());
                return fieldAccessor;
            }
            return super.visit(variableExpr, arg);
        }

        @Override
        public Expression visit(FieldAccessor fieldAccessor, ILangExpression arg) throws CompilationException {
            Expression fieldAccessorExpr = fieldAccessor.getExpr();
            Identifier fieldAccessIdent = fieldAccessor.getIdent();
            if (fieldAccessorExpr.getKind() == Expression.Kind.FIELD_ACCESSOR_EXPRESSION) {
                FieldAccessor innerFieldAccessExpr = (FieldAccessor) fieldAccessorExpr.accept(this, arg);
                FieldAccessor outerFieldAccessExpr = new FieldAccessor(innerFieldAccessExpr, fieldAccessIdent);
                outerFieldAccessExpr.setSourceLocation(fieldAccessor.getSourceLocation());
                return outerFieldAccessExpr;

            } else if (fieldAccessorExpr.getKind() == Expression.Kind.VARIABLE_EXPRESSION) {
                VariableExpr fieldAccessVarExpr = (VariableExpr) fieldAccessorExpr;
                VarIdentifier fieldAccessVar = SqlppVariableUtil.toUserDefinedVariableName(fieldAccessVarExpr.getVar());
                if (sourceSelectLiveVariables.contains(fieldAccessVarExpr)) {
                    VariableExpr qualifyingVariableCopy = deepCopyVisitor.visit(qualifyingVar, null);
                    FieldAccessor innerFieldAccessExpr = new FieldAccessor(qualifyingVariableCopy, fieldAccessVar);
                    FieldAccessor outerFieldAccessExpr = new FieldAccessor(innerFieldAccessExpr, fieldAccessIdent);
                    outerFieldAccessExpr.setSourceLocation(fieldAccessor.getSourceLocation());
                    return outerFieldAccessExpr;
                }
            }
            return super.visit(fieldAccessor, arg);
        }
    }
}
