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
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;

public abstract class AbstractGraphixQueryVisitor extends AbstractSqlppSimpleExpressionVisitor
        implements IGraphixLangVisitor<Expression, ILangExpression> {
    @Override
    public Expression visit(GraphConstructor gc, ILangExpression arg) throws CompilationException {
        // Visit all vertices before visiting any edges.
        for (GraphConstructor.VertexConstructor vertexConstructor : gc.getVertexElements()) {
            vertexConstructor.accept(this, arg);
        }
        for (GraphConstructor.EdgeConstructor edgeConstructor : gc.getEdgeElements()) {
            edgeConstructor.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(GraphConstructor.VertexConstructor ve, ILangExpression arg) throws CompilationException {
        return this.visit(ve.getExpression(), arg);
    }

    @Override
    public Expression visit(GraphConstructor.EdgeConstructor ee, ILangExpression arg) throws CompilationException {
        return this.visit(ee.getExpression(), arg);
    }

    @Override
    public Expression visit(SelectBlock sb, ILangExpression arg) throws CompilationException {
        return (sb instanceof GraphSelectBlock) ? this.visit((GraphSelectBlock) sb, arg) : super.visit(sb, arg);
    }

    @Override
    public Expression visit(GraphSelectBlock gsb, ILangExpression arg) throws CompilationException {
        // Traverse in the same order as a regular SELECT-BLOCK: FROM, LET/WHERE, GROUP-BY, LET/HAVING, SELECT.
        if (gsb.hasFromGraphClause()) {
            gsb.getFromGraphClause().accept(this, arg);

        } else if (gsb.hasFromClause()) {
            gsb.getFromClause().accept(this, arg);
        }
        if (gsb.hasLetWhereClauses()) {
            for (AbstractClause clause : gsb.getLetWhereList()) {
                clause.accept(this, arg);
            }
        }
        if (gsb.hasGroupbyClause()) {
            gsb.getGroupbyClause().accept(this, arg);
        }
        if (gsb.hasLetHavingClausesAfterGroupby()) {
            for (AbstractClause clause : gsb.getLetHavingListAfterGroupby()) {
                clause.accept(this, arg);
            }
        }
        gsb.getSelectClause().accept(this, arg);
        return null;
    }

    @Override
    public Expression visit(FromGraphClause fgc, ILangExpression arg) throws CompilationException {
        // Visit our graph constructor (if it exists), then all of our MATCH clauses, and finally our correlate clauses.
        if (fgc.getGraphConstructor() != null) {
            fgc.getGraphConstructor().accept(this, arg);
        }
        for (MatchClause matchClause : fgc.getMatchClauses()) {
            matchClause.accept(this, arg);
        }
        for (AbstractBinaryCorrelateClause correlateClause : fgc.getCorrelateClauses()) {
            correlateClause.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(MatchClause mc, ILangExpression arg) throws CompilationException {
        for (PathPatternExpr pathPatternExpr : mc.getPathExpressions()) {
            pathPatternExpr.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
        // Visit our vertices first, then our edges.
        for (VertexPatternExpr vertexExpression : ppe.getVertexExpressions()) {
            vertexExpression.accept(this, arg);
        }
        for (EdgePatternExpr edgeExpression : ppe.getEdgeExpressions()) {
            edgeExpression.accept(this, arg);
        }
        if (ppe.getVariableExpr() != null) {
            ppe.getVariableExpr().accept(this, arg);
        }
        return ppe;
    }

    @Override
    public Expression visit(EdgePatternExpr epe, ILangExpression arg) throws CompilationException {
        // We do not visit any **terminal** vertices here. These should be handled by the containing PathPatternExpr.
        EdgeDescriptor edgeDescriptor = epe.getEdgeDescriptor();
        if (edgeDescriptor.getVariableExpr() != null) {
            edgeDescriptor.getVariableExpr().accept(this, arg);
        }
        for (VertexPatternExpr internalVertex : epe.getInternalVertices()) {
            internalVertex.accept(this, arg);
        }
        return epe;
    }

    @Override
    public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
        if (vpe.getVariableExpr() != null) {
            vpe.getVariableExpr().accept(this, arg);
        }
        return vpe;
    }

    @Override
    public Expression visit(GraphElementDeclaration gel, ILangExpression arg) throws CompilationException {
        return null;
    }

    @Override
    public Expression visit(DeclareGraphStatement dgs, ILangExpression arg) throws CompilationException {
        return null;
    }

    @Override
    public Expression visit(CreateGraphStatement cgs, ILangExpression arg) throws CompilationException {
        return null;
    }

    @Override
    public Expression visit(GraphDropStatement gds, ILangExpression arg) throws CompilationException {
        return null;
    }
}
