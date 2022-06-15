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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.GraphSelectBlock;
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
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.visitor.DeepCopyVisitor;

/**
 * Extend {@link DeepCopyVisitor} to include Graphix AST nodes.
 */
public class GraphixDeepCopyVisitor extends DeepCopyVisitor implements IGraphixLangVisitor<ILangExpression, Void> {
    @Override
    public GraphSelectBlock visit(GraphSelectBlock graphSelectBlock, Void arg) throws CompilationException {
        SelectClause clonedSelectClause = this.visit(graphSelectBlock.getSelectClause(), arg);
        FromGraphClause clonedFromGraphClause = this.visit(graphSelectBlock.getFromGraphClause(), arg);
        GroupbyClause clonedGroupByClause = null;
        if (graphSelectBlock.hasGroupbyClause()) {
            clonedGroupByClause = this.visit(graphSelectBlock.getGroupbyClause(), arg);
        }
        List<AbstractClause> clonedLetWhereClauses = new ArrayList<>();
        List<AbstractClause> clonedLetHavingClauses = new ArrayList<>();
        for (AbstractClause letWhereClause : graphSelectBlock.getLetWhereList()) {
            clonedLetWhereClauses.add((AbstractClause) letWhereClause.accept(this, arg));
        }
        for (AbstractClause letHavingClause : graphSelectBlock.getLetHavingListAfterGroupby()) {
            clonedLetHavingClauses.add((AbstractClause) letHavingClause.accept(this, arg));
        }
        return new GraphSelectBlock(clonedSelectClause, clonedFromGraphClause, clonedLetWhereClauses,
                clonedGroupByClause, clonedLetHavingClauses);
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
        if (fromGraphClause.getGraphConstructor() != null) {
            GraphConstructor graphConstructor = fromGraphClause.getGraphConstructor();
            return new FromGraphClause(graphConstructor, clonedMatchClauses, clonedCorrelateClauses);

        } else {
            return new FromGraphClause(fromGraphClause.getDataverseName(), fromGraphClause.getGraphName(),
                    clonedMatchClauses, clonedCorrelateClauses);
        }
    }

    @Override
    public MatchClause visit(MatchClause matchClause, Void arg) throws CompilationException {
        List<PathPatternExpr> clonedPathExpression = new ArrayList<>();
        for (PathPatternExpr pathExpression : matchClause.getPathExpressions()) {
            clonedPathExpression.add(this.visit(pathExpression, arg));
        }
        return new MatchClause(clonedPathExpression, matchClause.getMatchType());
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
        Set<VarIdentifier> visitedVertices = new HashSet<>();
        for (EdgePatternExpr edgeExpression : pathPatternExpr.getEdgeExpressions()) {
            EdgePatternExpr clonedEdgeExpression = this.visit(edgeExpression, arg);
            clonedEdgeExpressions.add(clonedEdgeExpression);
            clonedVertexExpressions.add(clonedEdgeExpression.getLeftVertex());
            clonedVertexExpressions.add(clonedEdgeExpression.getRightVertex());
            visitedVertices.add(clonedEdgeExpression.getRightVertex().getVariableExpr().getVar());
            visitedVertices.add(clonedEdgeExpression.getLeftVertex().getVariableExpr().getVar());
        }
        for (VertexPatternExpr vertexExpression : pathPatternExpr.getVertexExpressions()) {
            if (!visitedVertices.contains(vertexExpression.getVariableExpr().getVar())) {
                VertexPatternExpr clonedVertexExpression = this.visit(vertexExpression, arg);
                clonedVertexExpressions.add(clonedVertexExpression);
                visitedVertices.add(clonedVertexExpression.getVariableExpr().getVar());
            }
        }

        // Clone our sub-path expressions.
        PathPatternExpr clonedPathPatternExpr =
                new PathPatternExpr(clonedVertexExpressions, clonedEdgeExpressions, clonedVariableExpr);
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
        List<VertexPatternExpr> clonedInternalVertices = new ArrayList<>();
        for (VertexPatternExpr internalVertex : edgePatternExpr.getInternalVertices()) {
            clonedInternalVertices.add(this.visit(internalVertex, arg));
        }
        VariableExpr clonedVariableExpr = null;
        if (edgePatternExpr.getEdgeDescriptor().getVariableExpr() != null) {
            clonedVariableExpr = this.visit(edgePatternExpr.getEdgeDescriptor().getVariableExpr(), arg);
        }

        // Generate a cloned edge.
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        Set<ElementLabel> clonedEdgeDescriptorLabels = new HashSet<>(edgeDescriptor.getEdgeLabels());
        EdgeDescriptor clonedDescriptor = new EdgeDescriptor(edgeDescriptor.getEdgeDirection(),
                edgeDescriptor.getPatternType(), clonedEdgeDescriptorLabels, clonedVariableExpr,
                edgeDescriptor.getMinimumHops(), edgeDescriptor.getMaximumHops());
        EdgePatternExpr clonedEdge = new EdgePatternExpr(clonedLeftVertex, clonedRightVertex, clonedDescriptor);
        clonedEdge.replaceInternalVertices(clonedInternalVertices);
        return clonedEdge;
    }

    @Override
    public VertexPatternExpr visit(VertexPatternExpr vertexPatternExpr, Void arg) throws CompilationException {
        VariableExpr clonedVariableExpr = null;
        if (vertexPatternExpr.getVariableExpr() != null) {
            clonedVariableExpr = this.visit(vertexPatternExpr.getVariableExpr(), arg);
        }
        Set<ElementLabel> clonedVertexLabels = new HashSet<>(vertexPatternExpr.getLabels());
        return new VertexPatternExpr(clonedVariableExpr, clonedVertexLabels);
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
