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
package org.apache.asterix.graphix.lang.rewrites.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.GraphSelectBlock;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.GenerateColumnNameVisitor;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * A pre-Graphix transformation pass to populate a number of unknowns in our Graphix AST.
 * a) Populate all unknown graph elements (vertices and edges).
 * b) Populate all unknown column names in SELECT-CLAUSEs.
 * c) Populate all unknown GROUP-BY keys.
 * d) Fill in all GROUP-BY fields.
 */
public class PopulateUnknownsVisitor extends AbstractGraphixQueryVisitor {
    private final GenerateColumnNameVisitor generateColumnNameVisitor;
    private final Supplier<VarIdentifier> newVariableSupplier;

    public PopulateUnknownsVisitor(GraphixRewritingContext graphixRewritingContext) {
        generateColumnNameVisitor = new GenerateColumnNameVisitor(graphixRewritingContext);
        newVariableSupplier = graphixRewritingContext::getNewGraphixVariable;
    }

    @Override
    public Expression visit(SelectExpression selectExpression, ILangExpression arg) throws CompilationException {
        selectExpression.accept(generateColumnNameVisitor, arg);
        return super.visit(selectExpression, arg);
    }

    @Override
    public Expression visit(GraphSelectBlock graphSelectBlock, ILangExpression arg) throws CompilationException {
        super.visit(graphSelectBlock, arg);

        if (graphSelectBlock.hasGroupbyClause()) {
            // Collect all variables that should belong in the GROUP-BY field list.
            List<VarIdentifier> userLiveVariables = new ArrayList<>();
            for (MatchClause matchClause : graphSelectBlock.getFromGraphClause().getMatchClauses()) {
                for (PathPatternExpr pathExpression : matchClause.getPathExpressions()) {
                    if (pathExpression.getVariableExpr() != null) {
                        userLiveVariables.add(pathExpression.getVariableExpr().getVar());
                    }
                    for (VertexPatternExpr vertexExpression : pathExpression.getVertexExpressions()) {
                        VarIdentifier vertexVariable = vertexExpression.getVariableExpr().getVar();
                        if (!GraphixRewritingContext.isGraphixVariable(vertexVariable)) {
                            userLiveVariables.add(vertexVariable);
                        }
                    }
                    for (EdgePatternExpr edgeExpression : pathExpression.getEdgeExpressions()) {
                        VarIdentifier edgeVariable = edgeExpression.getEdgeDescriptor().getVariableExpr().getVar();
                        if (!GraphixRewritingContext.isGraphixVariable(edgeVariable)) {
                            userLiveVariables.add(edgeVariable);
                        }
                    }
                }
            }
            if (!graphSelectBlock.getFromGraphClause().getCorrelateClauses().isEmpty()) {
                FromGraphClause fromGraphClause = graphSelectBlock.getFromGraphClause();
                List<AbstractBinaryCorrelateClause> correlateClauses = fromGraphClause.getCorrelateClauses();
                for (AbstractBinaryCorrelateClause correlateClause : correlateClauses) {
                    VarIdentifier bindingVariable = correlateClause.getRightVariable().getVar();
                    if (!GraphixRewritingContext.isGraphixVariable(bindingVariable)) {
                        userLiveVariables.add(bindingVariable);
                    }
                }
            }
            if (graphSelectBlock.hasLetWhereClauses()) {
                for (AbstractClause abstractClause : graphSelectBlock.getLetWhereList()) {
                    if (abstractClause.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                        LetClause letClause = (LetClause) abstractClause;
                        VarIdentifier bindingVariable = letClause.getVarExpr().getVar();
                        if (!GraphixRewritingContext.isGraphixVariable(bindingVariable)) {
                            userLiveVariables.add(bindingVariable);
                        }
                    }
                }
            }

            // Add the live variables to our GROUP-BY field list.
            List<Pair<Expression, Identifier>> newGroupFieldList = new ArrayList<>();
            for (VarIdentifier userLiveVariable : userLiveVariables) {
                String variableName = SqlppVariableUtil.toUserDefinedName(userLiveVariable.getValue());
                VariableExpr variableExpr = new VariableExpr(userLiveVariable);
                newGroupFieldList.add(new Pair<>(variableExpr, new Identifier(variableName)));
            }
            graphSelectBlock.getGroupbyClause().setGroupFieldList(newGroupFieldList);
        }
        return null;
    }

    @Override
    public Expression visit(VertexPatternExpr vertexExpression, ILangExpression arg) throws CompilationException {
        if (vertexExpression.getVariableExpr() == null) {
            vertexExpression.setVariableExpr(new VariableExpr(newVariableSupplier.get()));
        }
        return super.visit(vertexExpression, arg);
    }

    @Override
    public Expression visit(EdgePatternExpr edgeExpression, ILangExpression arg) throws CompilationException {
        EdgeDescriptor edgeDescriptor = edgeExpression.getEdgeDescriptor();
        if (edgeDescriptor.getVariableExpr() == null) {
            edgeDescriptor.setVariableExpr(new VariableExpr(newVariableSupplier.get()));
        }
        return super.visit(edgeExpression, arg);
    }
}
