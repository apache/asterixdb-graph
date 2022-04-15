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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.GraphSelectBlock;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.GraphConstructor;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.statement.CreateGraphStatement;
import org.apache.asterix.graphix.lang.statement.GraphDropStatement;
import org.apache.asterix.graphix.lang.statement.GraphElementDecl;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

/**
 * Ensure that any subtree whose immediate parent node includes a {@link FromGraphClause} follow Graphix-specific
 * variable resolution rules (i.e. do not rely on context variables).
 * - {@link PathPatternExpr} may introduce a variable. Uniqueness is enforced here.
 * - {@link EdgePatternExpr} may introduce a variable. Uniqueness is enforced w/ {@link PreRewriteCheckVisitor}.
 * - {@link VertexPatternExpr} may introduce a variable. Uniqueness is not required.
 * - {@link org.apache.asterix.lang.sqlpp.clause.JoinClause} may introduce a variable (handled in parent).
 * - {@link org.apache.asterix.lang.sqlpp.clause.NestClause} may introduce a variable (handled in parent).
 * - {@link org.apache.asterix.lang.sqlpp.clause.UnnestClause} may introduce a variable (handled in parent).
 * - {@link org.apache.asterix.lang.common.clause.GroupbyClause} may introduce a variable (handled in parent).
 * - {@link org.apache.asterix.lang.common.clause.LetClause} may introduce a variable (handled in parent).
 */
public class ScopingCheckVisitor extends AbstractSqlppExpressionScopingVisitor
        implements IGraphixLangVisitor<Expression, ILangExpression> {
    private final Deque<Mutable<Boolean>> graphixVisitStack = new ArrayDeque<>();

    public ScopingCheckVisitor(LangRewritingContext context) {
        super(context);

        // We start with an element of false in our stack.
        graphixVisitStack.addLast(new MutableObject<>(false));
    }

    @Override
    public Expression visit(SelectExpression selectExpression, ILangExpression arg) throws CompilationException {
        graphixVisitStack.addLast(new MutableObject<>(false));
        super.visit(selectExpression, arg);
        graphixVisitStack.removeLast();
        return selectExpression;
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        return (selectBlock instanceof GraphSelectBlock) ? this.visit((GraphSelectBlock) selectBlock, arg)
                : super.visit(selectBlock, arg);
    }

    @Override
    public Expression visit(GraphSelectBlock graphSelectBlock, ILangExpression arg) throws CompilationException {
        graphixVisitStack.getLast().setValue(true);
        if (graphSelectBlock.hasFromGraphClause()) {
            graphSelectBlock.getFromGraphClause().accept(this, arg);

        } else if (graphSelectBlock.hasFromClause()) {
            graphSelectBlock.getFromClause().accept(this, arg);
        }
        if (graphSelectBlock.hasLetWhereClauses()) {
            for (AbstractClause clause : graphSelectBlock.getLetWhereList()) {
                clause.accept(this, arg);
            }
        }
        if (graphSelectBlock.hasGroupbyClause()) {
            graphSelectBlock.getGroupbyClause().accept(this, arg);
        }
        if (graphSelectBlock.hasLetHavingClausesAfterGroupby()) {
            for (AbstractClause clause : graphSelectBlock.getLetHavingListAfterGroupby()) {
                clause.accept(this, arg);
            }
        }
        graphSelectBlock.getSelectClause().accept(this, arg);
        return null;
    }

    @Override
    public Expression visit(FromGraphClause fromGraphClause, ILangExpression arg) throws CompilationException {
        // We are now working with a new scope.
        scopeChecker.createNewScope();
        for (MatchClause matchClause : fromGraphClause.getMatchClauses()) {
            matchClause.accept(this, arg);
        }
        for (AbstractBinaryCorrelateClause correlateClause : fromGraphClause.getCorrelateClauses()) {
            correlateClause.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(MatchClause matchClause, ILangExpression arg) throws CompilationException {
        for (PathPatternExpr pathPatternExpr : matchClause.getPathExpressions()) {
            pathPatternExpr.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(PathPatternExpr pathPatternExpr, ILangExpression arg) throws CompilationException {
        // Visit our vertices first, then our edges.
        for (VertexPatternExpr vertexExpression : pathPatternExpr.getVertexExpressions()) {
            vertexExpression.accept(this, arg);
        }
        for (EdgePatternExpr edgeExpression : pathPatternExpr.getEdgeExpressions()) {
            edgeExpression.accept(this, arg);
        }

        // Ensure that we don't have a duplicate alias here.
        VariableExpr pathVariable = pathPatternExpr.getVariableExpr();
        if (pathVariable != null) {
            String pathVariableValue = pathVariable.getVar().getValue();
            if (scopeChecker.getCurrentScope().findLocalSymbol(pathVariableValue) != null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, pathVariable.getSourceLocation(),
                        "Duplicate alias definitions: " + SqlppVariableUtil.toUserDefinedName(pathVariableValue));
            }
            scopeChecker.getCurrentScope().addNewVarSymbolToScope(pathVariable.getVar());
        }
        return pathPatternExpr;
    }

    @Override
    public Expression visit(EdgePatternExpr edgePatternExpr, ILangExpression arg) throws CompilationException {
        // We do not visit any **terminal** vertices here.
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        if (edgeDescriptor.getVariableExpr() != null) {
            scopeChecker.getCurrentScope().addNewVarSymbolToScope(edgeDescriptor.getVariableExpr().getVar());
        }
        return edgePatternExpr;
    }

    @Override
    public Expression visit(VertexPatternExpr vertexPatternExpr, ILangExpression arg) throws CompilationException {
        if (vertexPatternExpr.getVariableExpr() != null) {
            scopeChecker.getCurrentScope().addNewVarSymbolToScope(vertexPatternExpr.getVariableExpr().getVar());
        }
        return vertexPatternExpr;
    }

    // We aren't going to inline our column aliases (yet), so add our select variables to our scope.
    @Override
    public Expression visit(SelectClause selectClause, ILangExpression arg) throws CompilationException {
        super.visit(selectClause, arg);
        if (selectClause.selectRegular()) {
            SelectRegular selectRegular = selectClause.getSelectRegular();
            for (Projection projection : selectRegular.getProjections()) {
                String variableName = SqlppVariableUtil.toInternalVariableName(projection.getName());
                scopeChecker.getCurrentScope().addSymbolToScope(new Identifier(variableName), Set.of());
            }
        }
        return null;
    }

    @Override
    public Expression visit(VariableExpr varExpr, ILangExpression arg) throws CompilationException {
        boolean hasVisitedGraphixNode = !graphixVisitStack.isEmpty() && graphixVisitStack.getLast().getValue();
        String varSymbol = varExpr.getVar().getValue();

        // We will only throw an unresolved error if we first encounter a Graphix AST node.
        if (hasVisitedGraphixNode && scopeChecker.getCurrentScope().findSymbol(varSymbol) == null) {
            throw new CompilationException(ErrorCode.UNDEFINED_IDENTIFIER, varExpr.getSourceLocation(),
                    SqlppVariableUtil.toUserDefinedVariableName(varSymbol).getValue());
        }
        return varExpr;
    }

    // We leave the scoping of our GRAPH-CONSTRUCTOR bodies to our body rewriter.
    @Override
    public Expression visit(GraphConstructor graphConstructor, ILangExpression arg) throws CompilationException {
        return null;
    }

    @Override
    public Expression visit(GraphConstructor.VertexConstructor vertexConstructor, ILangExpression arg)
            throws CompilationException {
        return null;
    }

    @Override
    public Expression visit(GraphConstructor.EdgeConstructor edgeConstructor, ILangExpression arg)
            throws CompilationException {
        return null;
    }

    // The following should not appear in queries.
    @Override
    public Expression visit(CreateGraphStatement createGraphStatement, ILangExpression arg)
            throws CompilationException {
        return null;
    }

    @Override
    public Expression visit(GraphElementDecl graphElementDecl, ILangExpression arg) throws CompilationException {
        return null;
    }

    @Override
    public Expression visit(GraphDropStatement graphDropStatement, ILangExpression arg) throws CompilationException {
        return null;
    }
}
