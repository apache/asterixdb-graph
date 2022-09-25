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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.GraphConstructor;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.statement.CreateGraphStatement;
import org.apache.asterix.graphix.lang.statement.DeclareGraphStatement;
import org.apache.asterix.graphix.lang.statement.GraphDropStatement;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.visitor.DeepCopyVisitor;

/**
 * Extend {@link DeepCopyVisitor} to include Graphix AST nodes.
 */
public class GraphixDeepCopyVisitor extends DeepCopyVisitor implements IGraphixLangVisitor<ILangExpression, Void> {
    @Override
    public SelectBlock visit(SelectBlock selectBlock, Void arg) throws CompilationException {
        SelectClause clonedSelectClause = this.visit(selectBlock.getSelectClause(), arg);
        FromClause clonedFromClause = null;
        if (selectBlock.hasFromClause() && selectBlock.getFromClause() instanceof FromGraphClause) {
            clonedFromClause = this.visit((FromGraphClause) selectBlock.getFromClause(), arg);

        } else if (selectBlock.hasFromClause()) {
            clonedFromClause = super.visit(selectBlock.getFromClause(), arg);
        }
        GroupbyClause clonedGroupByClause = null;
        if (selectBlock.hasGroupbyClause()) {
            clonedGroupByClause = this.visit(selectBlock.getGroupbyClause(), arg);
        }
        List<AbstractClause> clonedLetWhereClauses = new ArrayList<>();
        List<AbstractClause> clonedLetHavingClauses = new ArrayList<>();
        for (AbstractClause letWhereClause : selectBlock.getLetWhereList()) {
            clonedLetWhereClauses.add((AbstractClause) letWhereClause.accept(this, arg));
        }
        for (AbstractClause letHavingClause : selectBlock.getLetHavingListAfterGroupby()) {
            clonedLetHavingClauses.add((AbstractClause) letHavingClause.accept(this, arg));
        }
        SelectBlock clonedSelectBlock = new SelectBlock(clonedSelectClause, clonedFromClause, clonedLetWhereClauses,
                clonedGroupByClause, clonedLetHavingClauses);
        clonedSelectBlock.setSourceLocation(selectBlock.getSourceLocation());
        return clonedSelectBlock;
    }

    @Override
    public FromGraphClause visit(FromGraphClause fromGraphClause, Void arg) throws CompilationException {
        List<AbstractBinaryCorrelateClause> clonedCorrelateClauses = new ArrayList<>();
        List<MatchClause> clonedMatchClauses = new ArrayList<>();
        for (AbstractBinaryCorrelateClause correlateClause : fromGraphClause.getCorrelateClauses()) {
            clonedCorrelateClauses.add((AbstractBinaryCorrelateClause) correlateClause.accept(this, arg));
        }
        for (MatchClause matchClause : fromGraphClause.getMatchClauses()) {
            clonedMatchClauses.add(this.visit(matchClause, arg));
        }
        FromGraphClause clonedFromGraphClause;
        if (fromGraphClause.getGraphConstructor() != null) {
            GraphConstructor graphConstructor = fromGraphClause.getGraphConstructor();
            clonedFromGraphClause = new FromGraphClause(graphConstructor, clonedMatchClauses, clonedCorrelateClauses);

        } else {
            clonedFromGraphClause = new FromGraphClause(fromGraphClause.getDataverseName(),
                    fromGraphClause.getGraphName(), clonedMatchClauses, clonedCorrelateClauses);
        }
        clonedFromGraphClause.setSourceLocation(fromGraphClause.getSourceLocation());
        return clonedFromGraphClause;
    }

    @Override
    public MatchClause visit(MatchClause matchClause, Void arg) throws CompilationException {
        List<PathPatternExpr> clonedPathExpression = new ArrayList<>();
        for (PathPatternExpr pathExpression : matchClause.getPathExpressions()) {
            clonedPathExpression.add(this.visit(pathExpression, arg));
        }
        MatchClause clonedMatchClause = new MatchClause(clonedPathExpression, matchClause.getMatchType());
        clonedMatchClause.setSourceLocation(matchClause.getSourceLocation());
        return clonedMatchClause;
    }

    @Override
    public PathPatternExpr visit(PathPatternExpr pathPatternExpr, Void arg) throws CompilationException {
        List<EdgePatternExpr> clonedEdgeExpressions = new ArrayList<>();
        List<VertexPatternExpr> clonedVertexExpressions = new ArrayList<>();
        VariableExpr clonedVariableExpr = null;
        if (pathPatternExpr.getVariableExpr() != null) {
            clonedVariableExpr = this.visit(pathPatternExpr.getVariableExpr(), arg);
        }

        // Only visit dangling vertices in our edge.
        Set<VariableExpr> visitedVertices = new HashSet<>();
        for (EdgePatternExpr edgeExpression : pathPatternExpr.getEdgeExpressions()) {
            EdgePatternExpr clonedEdgeExpression = this.visit(edgeExpression, arg);
            clonedEdgeExpressions.add(clonedEdgeExpression);
            clonedEdgeExpression.setSourceLocation(edgeExpression.getSourceLocation());
            VertexPatternExpr clonedLeftVertex = clonedEdgeExpression.getLeftVertex();
            VertexPatternExpr clonedRightVertex = clonedEdgeExpression.getRightVertex();
            clonedVertexExpressions.add(clonedLeftVertex);
            clonedVertexExpressions.add(clonedRightVertex);
            visitedVertices.add(clonedLeftVertex.getVariableExpr());
            visitedVertices.add(clonedRightVertex.getVariableExpr());
        }
        for (VertexPatternExpr vertexExpression : pathPatternExpr.getVertexExpressions()) {
            if (!visitedVertices.contains(vertexExpression.getVariableExpr())) {
                VertexPatternExpr clonedVertexExpression = this.visit(vertexExpression, arg);
                clonedVertexExpression.setSourceLocation(vertexExpression.getSourceLocation());
                clonedVertexExpressions.add(clonedVertexExpression);
                visitedVertices.add(clonedVertexExpression.getVariableExpr());
            }
        }

        // Clone our sub-path expressions.
        PathPatternExpr clonedPathPatternExpr =
                new PathPatternExpr(clonedVertexExpressions, clonedEdgeExpressions, clonedVariableExpr);
        clonedPathPatternExpr.setSourceLocation(pathPatternExpr.getSourceLocation());
        for (LetClause letClause : pathPatternExpr.getReboundSubPathList()) {
            clonedPathPatternExpr.getReboundSubPathList().add(this.visit(letClause, arg));
        }
        return clonedPathPatternExpr;
    }

    @Override
    public EdgePatternExpr visit(EdgePatternExpr edgePatternExpr, Void arg) throws CompilationException {
        // Clone our expressions.
        VertexPatternExpr clonedLeftVertex = this.visit(edgePatternExpr.getLeftVertex(), arg);
        VertexPatternExpr clonedRightVertex = this.visit(edgePatternExpr.getRightVertex(), arg);
        VertexPatternExpr clonedInternalVertex = null;
        if (edgePatternExpr.getInternalVertex() != null) {
            clonedInternalVertex = this.visit(edgePatternExpr.getInternalVertex(), arg);
        }
        VariableExpr clonedVariableExpr = null;
        if (edgePatternExpr.getEdgeDescriptor().getVariableExpr() != null) {
            clonedVariableExpr = this.visit(edgePatternExpr.getEdgeDescriptor().getVariableExpr(), arg);
        }
        clonedLeftVertex.setSourceLocation(edgePatternExpr.getLeftVertex().getSourceLocation());
        clonedRightVertex.setSourceLocation(edgePatternExpr.getRightVertex().getSourceLocation());

        // Generate a cloned edge.
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        Set<ElementLabel> clonedEdgeDescriptorLabels = new HashSet<>(edgeDescriptor.getEdgeLabels());
        Expression clonedFilterExpr = null;
        if (edgeDescriptor.getFilterExpr() != null) {
            clonedFilterExpr = (Expression) edgeDescriptor.getFilterExpr().accept(this, arg);
        }
        EdgeDescriptor clonedDescriptor = new EdgeDescriptor(edgeDescriptor.getEdgeDirection(),
                edgeDescriptor.getPatternType(), clonedEdgeDescriptorLabels, clonedFilterExpr, clonedVariableExpr,
                edgeDescriptor.getMinimumHops(), edgeDescriptor.getMaximumHops());
        EdgePatternExpr clonedEdge = new EdgePatternExpr(clonedLeftVertex, clonedRightVertex, clonedDescriptor);
        clonedEdge.setInternalVertex(clonedInternalVertex);
        clonedEdge.setSourceLocation(edgePatternExpr.getSourceLocation());
        return clonedEdge;
    }

    @Override
    public VertexPatternExpr visit(VertexPatternExpr vertexPatternExpr, Void arg) throws CompilationException {
        VariableExpr clonedVariableExpr = null;
        if (vertexPatternExpr.getVariableExpr() != null) {
            clonedVariableExpr = this.visit(vertexPatternExpr.getVariableExpr(), arg);
        }
        Expression clonedFilterExpr = null;
        if (vertexPatternExpr.getFilterExpr() != null) {
            clonedFilterExpr = (Expression) vertexPatternExpr.getFilterExpr().accept(this, arg);
        }
        Set<ElementLabel> clonedElementLabels = new HashSet<>(vertexPatternExpr.getLabels());
        VertexPatternExpr clonedVertexExpr =
                new VertexPatternExpr(clonedVariableExpr, clonedFilterExpr, clonedElementLabels);
        clonedVertexExpr.setSourceLocation(vertexPatternExpr.getSourceLocation());
        return clonedVertexExpr;
    }

    // We do not touch our GRAPH-CONSTRUCTOR here.
    @Override
    public ILangExpression visit(GraphConstructor gc, Void arg) throws CompilationException {
        return null;
    }

    @Override
    public ILangExpression visit(GraphConstructor.VertexConstructor vc, Void arg) throws CompilationException {
        return null;
    }

    @Override
    public ILangExpression visit(GraphConstructor.EdgeConstructor ec, Void arg) throws CompilationException {
        return null;
    }

    // We can safely ignore the statements below, we will not encounter them in queries.
    @Override
    public ILangExpression visit(DeclareGraphStatement dgs, Void arg) throws CompilationException {
        return null;
    }

    @Override
    public ILangExpression visit(CreateGraphStatement cgs, Void arg) throws CompilationException {
        return null;
    }

    @Override
    public ILangExpression visit(GraphElementDeclaration gel, Void arg) throws CompilationException {
        return null;
    }

    @Override
    public ILangExpression visit(GraphDropStatement gds, Void arg) throws CompilationException {
        return null;
    }

}
