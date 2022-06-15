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
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;

/**
 * Clause for introducing a {@link WhereClause} in an intermediate list of correlated clauses. This clause allows us to
 * perform predicate push-down at the AST level (rather than at the Algebricks level).
 */
public class CorrWhereClause extends AbstractBinaryCorrelateClause {
    private final WhereClause whereClause;

    public CorrWhereClause(Expression conditionExpr) {
        super(conditionExpr, null, null);
        whereClause = new WhereClause(conditionExpr);
    }

    public Expression getExpression() {
        return whereClause.getWhereExpr();
    }

    public void setExpression(Expression expression) {
        whereClause.setWhereExpr(expression);
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.WHERE_CLAUSE;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        // This node will survive our Graphix lowering, so by default we call the dispatch on our WHERE-CLAUSE node.
        return whereClause.accept(visitor, arg);
    }
}
