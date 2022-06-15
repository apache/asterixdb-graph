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
import org.apache.asterix.common.exceptions.ErrorCode;
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
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.NestClause;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;

/**
 * Throw an error if a graph AST node (that isn't a special instance of {@link FromGraphClause}) is ever encountered.
 */
public class PostRewriteCheckVisitor extends AbstractSqlppSimpleExpressionVisitor
        implements IGraphixLangVisitor<Expression, ILangExpression> {
    @Override
    public Expression visit(GraphConstructor gc, ILangExpression arg) throws CompilationException {
        return throwException(gc);
    }

    @Override
    public Expression visit(GraphConstructor.VertexConstructor ve, ILangExpression arg) throws CompilationException {
        return throwException(ve);
    }

    @Override
    public Expression visit(GraphConstructor.EdgeConstructor ee, ILangExpression arg) throws CompilationException {
        return throwException(ee);
    }

    @Override
    public Expression visit(DeclareGraphStatement dgs, ILangExpression arg) throws CompilationException {
        return throwException(dgs);
    }

    @Override
    public Expression visit(CreateGraphStatement cgs, ILangExpression arg) throws CompilationException {
        return throwException(cgs);
    }

    @Override
    public Expression visit(GraphElementDeclaration gel, ILangExpression arg) throws CompilationException {
        return throwException(gel);
    }

    @Override
    public Expression visit(GraphDropStatement gds, ILangExpression arg) throws CompilationException {
        return throwException(gds);
    }

    @Override
    public Expression visit(GraphSelectBlock gsb, ILangExpression arg) throws CompilationException {
        // The only Graph AST node that should survive is the GRAPH-SELECT-BLOCK, which should functionally act the same
        // as its parent class SELECT-BLOCK.
        if (!gsb.hasFromClause() || gsb.hasFromGraphClause()) {
            return throwException(gsb);

        } else {
            return null;
        }
    }

    @Override
    public Expression visit(FromGraphClause fgc, ILangExpression arg) throws CompilationException {
        return throwException(fgc);
    }

    @Override
    public Expression visit(MatchClause mc, ILangExpression arg) throws CompilationException {
        return throwException(mc);
    }

    @Override
    public Expression visit(EdgePatternExpr epe, ILangExpression arg) throws CompilationException {
        return throwException(epe);
    }

    @Override
    public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
        return throwException(ppe);
    }

    @Override
    public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
        return throwException(vpe);
    }

    // There are a few expressions that skip some sub-expressions that we need to visit...
    @Override
    public Expression visit(FromTerm ft, ILangExpression arg) throws CompilationException {
        ft.getLeftVariable().accept(this, arg);
        return super.visit(ft, arg);
    }

    @Override
    public Expression visit(JoinClause jc, ILangExpression arg) throws CompilationException {
        jc.getRightVariable().accept(this, arg);
        return super.visit(jc, arg);
    }

    @Override
    public Expression visit(NestClause nc, ILangExpression arg) throws CompilationException {
        nc.getRightVariable().accept(this, arg);
        return super.visit(nc, arg);
    }

    @Override
    public Expression visit(UnnestClause uc, ILangExpression arg) throws CompilationException {
        uc.getRightVariable().accept(this, arg);
        return super.visit(uc, arg);
    }

    @Override
    public Expression visit(LetClause lc, ILangExpression arg) throws CompilationException {
        lc.getVarExpr().accept(this, arg);
        return super.visit(lc, arg);
    }

    @Override
    public Expression visit(WindowExpression we, ILangExpression arg) throws CompilationException {
        we.getWindowVar().accept(this, arg);
        return super.visit(we, arg);
    }

    private Expression throwException(ILangExpression e) throws CompilationException {
        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, e.getSourceLocation(),
                e.getClass().getName() + " was encountered. Check has failed.");
    }
}
