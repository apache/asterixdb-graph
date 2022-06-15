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
package org.apache.asterix.graphix.lang.rewrites.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * Perform an analysis of a normalized graph element body (i.e. no Graphix AST nodes) to determine if we can inline
 * this graph element body with the greater SELECT-BLOCK node during lowering.
 * 1. Is this a dataset CALL-EXPR? If so, we can inline this directly.
 * 2. Is this expression a SELECT-EXPR containing a single FROM-TERM w/ possibly only UNNEST clauses? If so, we can
 * inline this expression.
 * 3. Are there are LET-WHERE expressions? We can inline these if the two questions are true.
 * 4. Are there any aggregate functions (or any aggregation)? If so, we cannot inline this expression.
 * 5. Are there any UNION-ALLs? If so, we cannot inline this expression.
 * 6. Are there any ORDER-BY or LIMIT clauses? If so, we cannot inline this expression.
 */
public class ElementBodyAnalysisVisitor extends AbstractSqlppSimpleExpressionVisitor {
    private final ElementBodyAnalysisContext elementBodyAnalysisContext = new ElementBodyAnalysisContext();

    public static class ElementBodyAnalysisContext {
        private List<AbstractBinaryCorrelateClause> unnestClauses = null;
        private List<LetClause> letClauses = null;
        private List<WhereClause> whereClauses = null;
        private List<Projection> selectClauseProjections = null;
        private VariableExpr fromTermVariable = null;
        private Expression selectElement = null;

        // At a minimum, this field must be defined.
        private CallExpr datasetCallExpression = null;
        private DataverseName dataverseName = null;
        private String datasetName = null;

        // We take an optimistic approach, and look for cases where we _cannot_ inline our body.
        private boolean isExpressionInline = true;

        public DataverseName getDataverseName() {
            return dataverseName;
        }

        public String getDatasetName() {
            return datasetName;
        }

        public boolean isExpressionInline() {
            return isExpressionInline;
        }

        public List<LetClause> getLetClauses() {
            return letClauses;
        }

        public List<WhereClause> getWhereClauses() {
            return whereClauses;
        }

        public List<Projection> getSelectClauseProjections() {
            return selectClauseProjections;
        }

        public List<AbstractBinaryCorrelateClause> getUnnestClauses() {
            return unnestClauses;
        }

        public VariableExpr getFromTermVariable() {
            return fromTermVariable;
        }

        public Expression getSelectElement() {
            return selectElement;
        }

        public CallExpr getDatasetCallExpression() {
            return datasetCallExpression;
        }

        public boolean isSelectClauseInline() {
            if (selectElement == null && selectClauseProjections == null) {
                return true;

            } else if (selectElement != null && selectElement.getKind() == Expression.Kind.VARIABLE_EXPRESSION) {
                VariableExpr selectElementVariableExpr = (VariableExpr) selectElement;
                return selectElementVariableExpr.getVar() == fromTermVariable.getVar();
            }
            return false;
        }
    }

    @Override
    public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
        FunctionSignature functionSignature = callExpr.getFunctionSignature();
        FunctionIdentifier functionIdentifier = functionSignature.createFunctionIdentifier();
        if (functionIdentifier.equals(BuiltinFunctions.DATASET)) {
            LiteralExpr dataverseNameExpr = (LiteralExpr) callExpr.getExprList().get(0);
            LiteralExpr datasetNameExpr = (LiteralExpr) callExpr.getExprList().get(1);
            String dataverseName = ((StringLiteral) dataverseNameExpr.getValue()).getValue();
            String datasetName = ((StringLiteral) datasetNameExpr.getValue()).getValue();
            elementBodyAnalysisContext.datasetCallExpression = callExpr;
            elementBodyAnalysisContext.dataverseName = DataverseName.createBuiltinDataverseName(dataverseName);
            elementBodyAnalysisContext.datasetName = datasetName;
            return callExpr;

        } else if (FunctionMapUtil.isSql92AggregateFunction(functionSignature)
                || FunctionMapUtil.isCoreAggregateFunction(functionSignature)
                || BuiltinFunctions.getWindowFunction(functionIdentifier) != null) {
            elementBodyAnalysisContext.isExpressionInline = false;
            return callExpr;
        }
        return callExpr;
    }

    @Override
    public Expression visit(SelectExpression selectExpression, ILangExpression arg) throws CompilationException {
        if (arg != null) {
            // We are not in a top-level SELECT-EXPR. Do not proceed.
            return selectExpression;
        }
        if (selectExpression.hasOrderby() || selectExpression.hasLimit() || selectExpression.hasLetClauses()) {
            elementBodyAnalysisContext.isExpressionInline = false;
            return selectExpression;
        }
        selectExpression.getSelectSetOperation().accept(this, selectExpression);
        return selectExpression;
    }

    @Override
    public Expression visit(SelectSetOperation selectSetOperation, ILangExpression arg) throws CompilationException {
        if (selectSetOperation.hasRightInputs()) {
            elementBodyAnalysisContext.isExpressionInline = false;
            return null;
        }
        SetOperationInput leftInput = selectSetOperation.getLeftInput();
        if (leftInput.subquery()) {
            elementBodyAnalysisContext.isExpressionInline = false;
            return null;
        }
        leftInput.getSelectBlock().accept(this, arg);
        return null;
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        if (selectBlock.hasGroupbyClause() || selectBlock.hasLetHavingClausesAfterGroupby()) {
            elementBodyAnalysisContext.isExpressionInline = false;
            return null;
        }
        if (selectBlock.hasFromClause()) {
            selectBlock.getFromClause().accept(this, arg);
        }
        if (selectBlock.hasLetWhereClauses()) {
            selectBlock.getLetWhereList().forEach(c -> {
                if (c.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                    if (elementBodyAnalysisContext.letClauses == null) {
                        elementBodyAnalysisContext.letClauses = new ArrayList<>();
                    }
                    elementBodyAnalysisContext.letClauses.add((LetClause) c);

                } else { //c.getClauseType() == Clause.ClauseType.WHERE_CLAUSE
                    if (elementBodyAnalysisContext.whereClauses == null) {
                        elementBodyAnalysisContext.whereClauses = new ArrayList<>();
                    }
                    elementBodyAnalysisContext.whereClauses.add((WhereClause) c);
                }
            });
        }
        selectBlock.getSelectClause().accept(this, arg);
        return null;
    }

    @Override
    public Expression visit(FromClause fromClause, ILangExpression arg) throws CompilationException {
        if (fromClause.getFromTerms().size() > 1) {
            elementBodyAnalysisContext.isExpressionInline = false;
            return null;
        }
        fromClause.getFromTerms().get(0).accept(this, arg);
        return null;
    }

    @Override
    public Expression visit(FromTerm fromTerm, ILangExpression arg) throws CompilationException {
        List<AbstractBinaryCorrelateClause> correlateClauses = fromTerm.getCorrelateClauses();
        if (correlateClauses.stream().anyMatch(c -> !c.getClauseType().equals(Clause.ClauseType.UNNEST_CLAUSE))) {
            elementBodyAnalysisContext.isExpressionInline = false;
            return null;

        } else if (fromTerm.hasPositionalVariable()) {
            // TODO (GLENN): Add support for positional variables.
            elementBodyAnalysisContext.isExpressionInline = false;
            return null;

        }
        if (!correlateClauses.isEmpty()) {
            elementBodyAnalysisContext.unnestClauses = correlateClauses;
        }
        fromTerm.getLeftExpression().accept(this, arg);
        elementBodyAnalysisContext.fromTermVariable = fromTerm.getLeftVariable();
        return null;
    }

    @Override
    public Expression visit(SelectClause selectClause, ILangExpression arg) throws CompilationException {
        if (selectClause.selectElement()) {
            elementBodyAnalysisContext.selectElement = selectClause.getSelectElement().getExpression();

        } else if (selectClause.selectRegular()) {
            SelectRegular selectRegular = selectClause.getSelectRegular();
            List<Projection> projectionList = selectRegular.getProjections();
            if (projectionList.stream().anyMatch(p -> p.getKind() != Projection.Kind.NAMED_EXPR)) {
                elementBodyAnalysisContext.isExpressionInline = false;

            } else {
                elementBodyAnalysisContext.selectClauseProjections = projectionList;
                for (Projection projection : projectionList) {
                    projection.getExpression().accept(this, arg);
                }
            }
        }
        return null;
    }

    public ElementBodyAnalysisContext getElementBodyAnalysisContext() throws CompilationException {
        if (elementBodyAnalysisContext.isExpressionInline
                && (elementBodyAnalysisContext.datasetName == null || elementBodyAnalysisContext.dataverseName == null
                        || elementBodyAnalysisContext.datasetCallExpression == null)) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                    "Analysis of inline element body yielded no dataset!");
        }
        return elementBodyAnalysisContext;
    }
}
