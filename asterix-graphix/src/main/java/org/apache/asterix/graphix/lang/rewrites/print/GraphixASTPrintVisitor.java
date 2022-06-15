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
package org.apache.asterix.graphix.lang.rewrites.print;

import java.io.PrintWriter;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.GraphSelectBlock;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.GraphConstructor;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.visitor.IGraphixLangVisitor;
import org.apache.asterix.graphix.lang.statement.CreateGraphStatement;
import org.apache.asterix.graphix.lang.statement.DeclareGraphStatement;
import org.apache.asterix.graphix.lang.statement.GraphDropStatement;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.visitor.SqlppAstPrintVisitor;

public class GraphixASTPrintVisitor extends SqlppAstPrintVisitor implements IGraphixLangVisitor<Void, Integer> {
    public GraphixASTPrintVisitor(PrintWriter out) {
        super(out);
    }

    @Override
    public Void visit(GraphConstructor graphConstructor, Integer step) throws CompilationException {
        out.println(skip(step) + "GRAPH [");
        for (GraphConstructor.VertexConstructor vertexConstructor : graphConstructor.getVertexElements()) {
            vertexConstructor.accept(this, step + 1);
        }
        for (GraphConstructor.EdgeConstructor edgeConstructor : graphConstructor.getEdgeElements()) {
            edgeConstructor.accept(this, step + 1);
        }
        out.println(skip(step) + "]");
        return null;
    }

    @Override
    public Void visit(GraphConstructor.VertexConstructor vertexConstructor, Integer step) throws CompilationException {
        out.print(skip(step) + "VERTEX ");
        out.println("(:" + vertexConstructor.getLabel() + ")");
        out.println(skip(step) + "AS ");
        vertexConstructor.getExpression().accept(this, step + 1);
        return null;
    }

    @Override
    public Void visit(GraphConstructor.EdgeConstructor edgeConstructor, Integer step) throws CompilationException {
        out.print(skip(step) + "EDGE ");
        out.print("(:" + edgeConstructor.getSourceLabel() + ")");
        out.print("-[:" + edgeConstructor.getEdgeLabel() + "]->");
        out.println("(:" + edgeConstructor.getDestinationLabel() + ")");
        out.println(skip(step) + "AS ");
        edgeConstructor.getExpression().accept(this, step + 1);
        return null;
    }

    @Override
    public Void visit(SelectBlock selectBlock, Integer step) throws CompilationException {
        return (selectBlock instanceof GraphSelectBlock) ? this.visit((GraphSelectBlock) selectBlock, step)
                : super.visit(selectBlock, step);
    }

    @Override
    public Void visit(GraphSelectBlock graphSelectBlock, Integer step) throws CompilationException {
        graphSelectBlock.getSelectClause().accept(this, step);
        if (graphSelectBlock.hasFromClause()) {
            graphSelectBlock.getFromClause().accept(this, step);

        } else if (graphSelectBlock.hasFromGraphClause()) {
            graphSelectBlock.getFromGraphClause().accept(this, step);
        }
        if (graphSelectBlock.hasLetWhereClauses()) {
            for (AbstractClause letWhereClause : graphSelectBlock.getLetWhereList()) {
                letWhereClause.accept(this, step);
            }
        }
        if (graphSelectBlock.hasGroupbyClause()) {
            graphSelectBlock.getGroupbyClause().accept(this, step);
            if (graphSelectBlock.hasLetHavingClausesAfterGroupby()) {
                for (AbstractClause letHavingClause : graphSelectBlock.getLetHavingListAfterGroupby()) {
                    letHavingClause.accept(this, step);
                }
            }
        }
        return null;
    }

    @Override
    public Void visit(FromGraphClause fromGraphClause, Integer step) throws CompilationException {
        out.println(skip(step) + "FROM [");
        if (fromGraphClause.getGraphConstructor() != null) {
            fromGraphClause.getGraphConstructor().accept(this, step + 1);

        } else {
            out.print(skip(step + 1) + "GRAPH ");
            if (fromGraphClause.getDataverseName() != null) {
                out.print(fromGraphClause.getDataverseName().toString());
                out.print(".");
            }
            out.print(fromGraphClause.getGraphName());
        }
        out.println(skip(step) + "]");
        for (MatchClause matchClause : fromGraphClause.getMatchClauses()) {
            matchClause.accept(this, step);
        }
        for (AbstractBinaryCorrelateClause correlateClause : fromGraphClause.getCorrelateClauses()) {
            correlateClause.accept(this, step);
        }
        return null;
    }

    @Override
    public Void visit(MatchClause matchClause, Integer step) throws CompilationException {
        out.print(skip(step));
        switch (matchClause.getMatchType()) {
            case LEADING:
            case INNER:
                out.println("MATCH [");
                break;
            case LEFTOUTER:
                out.println("LEFT MATCH [");
                break;
        }
        for (PathPatternExpr pathExpression : matchClause.getPathExpressions()) {
            pathExpression.accept(this, step + 1);
        }
        out.println(skip(step) + "]");
        return null;
    }

    @Override
    public Void visit(PathPatternExpr pathPatternExpr, Integer step) throws CompilationException {
        List<VertexPatternExpr> danglingVertices = pathPatternExpr.getVertexExpressions().stream().filter(v -> {
            // Collect all vertices that are not attached to an edge.
            for (EdgePatternExpr edgeExpression : pathPatternExpr.getEdgeExpressions()) {
                VertexPatternExpr leftVertex = edgeExpression.getLeftVertex();
                VertexPatternExpr rightVertex = edgeExpression.getRightVertex();
                if (leftVertex == v || rightVertex == v) {
                    return false;
                }
            }
            return true;
        }).collect(Collectors.toList());
        int index = 0;
        for (VertexPatternExpr vertexPatternExpr : danglingVertices) {
            if (index > 0) {
                out.print(skip(step) + ",");
            }
            vertexPatternExpr.accept(this, step);
            out.println();
            index++;
        }
        for (EdgePatternExpr edgeExpression : pathPatternExpr.getEdgeExpressions()) {
            if (index > 0) {
                out.print(skip(step) + ",");
            }
            edgeExpression.accept(this, step);
            out.println();
            index++;
        }
        if (pathPatternExpr.getVariableExpr() != null) {
            out.print(skip(step) + "AS ");
            pathPatternExpr.getVariableExpr().accept(this, 0);
        }
        return null;
    }

    @Override
    public Void visit(EdgePatternExpr edgePatternExpr, Integer step) throws CompilationException {
        out.print(skip(step));
        edgePatternExpr.getLeftVertex().accept(this, 0);
        switch (edgePatternExpr.getEdgeDescriptor().getEdgeDirection()) {
            case LEFT_TO_RIGHT:
            case UNDIRECTED:
                out.print("-[");
                break;
            case RIGHT_TO_LEFT:
                out.print("<-[");
                break;
        }
        if (edgePatternExpr.getEdgeDescriptor().getVariableExpr() != null) {
            out.print(edgePatternExpr.getEdgeDescriptor().getVariableExpr().getVar().getValue());
        }
        out.print(":(");
        int index = 0;
        for (ElementLabel label : edgePatternExpr.getEdgeDescriptor().getEdgeLabels()) {
            if (index > 0) {
                out.print("|");
            }
            out.print(label);
            index++;
        }
        out.print("){");
        out.print(edgePatternExpr.getEdgeDescriptor().getMinimumHops().toString());
        out.print(",");
        out.print(edgePatternExpr.getEdgeDescriptor().getMaximumHops().toString());
        out.print("}");
        switch (edgePatternExpr.getEdgeDescriptor().getEdgeDirection()) {
            case LEFT_TO_RIGHT:
                out.print("]->");
                break;
            case RIGHT_TO_LEFT:
            case UNDIRECTED:
                out.print("]-");
                break;
        }
        edgePatternExpr.getRightVertex().accept(this, 0);
        return null;
    }

    @Override
    public Void visit(VertexPatternExpr vertexPatternExpr, Integer step) throws CompilationException {
        out.print(skip(step) + "(");
        if (vertexPatternExpr.getVariableExpr() != null) {
            out.print(vertexPatternExpr.getVariableExpr().getVar().getValue());
        }
        out.print(":");
        int index = 0;
        for (ElementLabel label : vertexPatternExpr.getLabels()) {
            if (index > 0) {
                out.print("|");
            }
            out.print(label);
            index++;
        }
        out.print(")");
        return null;
    }

    // The following should not appear in queries (the former, pre-rewrite).
    @Override
    public Void visit(DeclareGraphStatement declareGraphStatement, Integer arg) throws CompilationException {
        return null;
    }

    @Override
    public Void visit(CreateGraphStatement createGraphStatement, Integer step) throws CompilationException {
        return null;
    }

    @Override
    public Void visit(GraphElementDeclaration graphElementDeclaration, Integer step) throws CompilationException {
        return null;
    }

    @Override
    public Void visit(GraphDropStatement graphDropStatement, Integer step) throws CompilationException {
        return null;
    }
}
