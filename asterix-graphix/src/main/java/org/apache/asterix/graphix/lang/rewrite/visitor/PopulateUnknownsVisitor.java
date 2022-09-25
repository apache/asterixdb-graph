/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.graphix.lang.rewrite.visitor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.GenerateColumnNameVisitor;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * A pre-Graphix transformation pass to populate a number of unknowns in our Graphix AST.
 * <ol>
 *  <li>Populate all unknown graph elements (vertices and edges).</li>
 *  <li>Populate all unknown column names in SELECT-CLAUSEs.</li>
 *  <li>Populate all unknown GROUP-BY keys.</li>
 *  <li>Fill in all GROUP-BY fields.</li>
 * </ol>
 */
public class PopulateUnknownsVisitor extends AbstractGraphixQueryVisitor {
    private final GenerateColumnNameVisitor generateColumnNameVisitor;
    private final Supplier<VariableExpr> newVariableSupplier;

    public PopulateUnknownsVisitor(GraphixRewritingContext graphixRewritingContext) {
        generateColumnNameVisitor = new GenerateColumnNameVisitor(graphixRewritingContext);
        newVariableSupplier = () -> new VariableExpr(graphixRewritingContext.newVariable());
    }

    @Override
    public Expression visit(SelectExpression selectExpression, ILangExpression arg) throws CompilationException {
        selectExpression.accept(generateColumnNameVisitor, arg);
        return super.visit(selectExpression, arg);
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        super.visit(selectBlock, arg);

        if (selectBlock.hasGroupbyClause()) {
            // Collect all variables that should belong in the GROUP-BY field list.
            Set<VariableExpr> userLiveVariables = new HashSet<>();
            if (selectBlock.hasFromClause() && selectBlock.getFromClause() instanceof FromGraphClause) {
                FromGraphClause fromGraphClause = (FromGraphClause) selectBlock.getFromClause();
                for (MatchClause matchClause : fromGraphClause.getMatchClauses()) {
                    for (PathPatternExpr pathExpression : matchClause.getPathExpressions()) {
                        if (pathExpression.getVariableExpr() != null) {
                            userLiveVariables.add(pathExpression.getVariableExpr());
                        }
                        for (VertexPatternExpr vertexExpression : pathExpression.getVertexExpressions()) {
                            userLiveVariables.add(vertexExpression.getVariableExpr());
                        }
                        for (EdgePatternExpr edgeExpression : pathExpression.getEdgeExpressions()) {
                            userLiveVariables.add(edgeExpression.getEdgeDescriptor().getVariableExpr());
                        }
                    }
                }
                if (!fromGraphClause.getCorrelateClauses().isEmpty()) {
                    List<AbstractBinaryCorrelateClause> correlateClauses = fromGraphClause.getCorrelateClauses();
                    for (AbstractBinaryCorrelateClause correlateClause : correlateClauses) {
                        userLiveVariables.add(correlateClause.getRightVariable());
                    }
                }

            } else if (selectBlock.hasFromClause()) {
                FromClause fromClause = selectBlock.getFromClause();
                for (FromTerm fromTerm : fromClause.getFromTerms()) {
                    userLiveVariables.add(fromTerm.getLeftVariable());
                    for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
                        userLiveVariables.add(correlateClause.getRightVariable());
                    }
                }
            }
            if (selectBlock.hasLetWhereClauses()) {
                for (AbstractClause abstractClause : selectBlock.getLetWhereList()) {
                    if (abstractClause.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                        LetClause letClause = (LetClause) abstractClause;
                        userLiveVariables.add(letClause.getVarExpr());
                    }
                }
            }

            // Add the live variables to our GROUP-BY field list.
            List<Pair<Expression, Identifier>> newGroupFieldList = new ArrayList<>();
            for (VariableExpr userLiveVariable : userLiveVariables) {
                String variableName = SqlppVariableUtil.toUserDefinedName(userLiveVariable.getVar().getValue());
                newGroupFieldList.add(new Pair<>(userLiveVariable, new Identifier(variableName)));
            }
            selectBlock.getGroupbyClause().setGroupFieldList(newGroupFieldList);
        }
        return null;
    }

    @Override
    public Expression visit(VertexPatternExpr vertexExpression, ILangExpression arg) throws CompilationException {
        if (vertexExpression.getVariableExpr() == null) {
            vertexExpression.setVariableExpr(newVariableSupplier.get());
        }
        return super.visit(vertexExpression, arg);
    }

    @Override
    public Expression visit(EdgePatternExpr edgeExpression, ILangExpression arg) throws CompilationException {
        EdgeDescriptor edgeDescriptor = edgeExpression.getEdgeDescriptor();
        if (edgeDescriptor.getVariableExpr() == null) {
            edgeDescriptor.setVariableExpr(newVariableSupplier.get());
        }
        return super.visit(edgeExpression, arg);
    }
}
