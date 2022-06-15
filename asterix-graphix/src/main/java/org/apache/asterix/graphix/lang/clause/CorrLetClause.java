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
package org.apache.asterix.graphix.lang.clause;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.rewrites.visitor.ILetCorrelateClauseVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;

/**
 * Clause for introducing a {@link LetClause} into the scope of any correlated clauses. This clause allows us to avoid
 * nesting the Graphix lowering result to handle any correlated clauses on graph elements.
 */
public class CorrLetClause extends AbstractBinaryCorrelateClause {
    private final LetClause letClause;

    public CorrLetClause(Expression rightExpr, VariableExpr rightVar, VariableExpr rightPosVar) {
        super(rightExpr, rightVar, rightPosVar);
        letClause = new LetClause((rightVar == null) ? rightPosVar : rightVar, rightExpr);
    }

    @Override
    public void setRightExpression(Expression rightExpr) {
        VariableExpr variableExpr = (getRightVariable() == null) ? getPositionalVariable() : getRightVariable();
        letClause.setVarExpr(variableExpr);
        letClause.setBindingExpr(rightExpr);
        super.setRightExpression(rightExpr);
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.LET_CLAUSE;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        if (visitor instanceof ILetCorrelateClauseVisitor) {
            return ((ILetCorrelateClauseVisitor<R, T>) visitor).visit(this, arg);

        } else {
            // This node will survive our Graphix lowering, so by default we call the dispatch on our LET-CLAUSE node.
            return letClause.accept(visitor, arg);
        }
    }
}
