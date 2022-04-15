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
package org.apache.asterix.graphix.lang.rewrites.util;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.struct.OperatorType;

public final class ClauseRewritingUtil {
    public static Expression buildConnectedClauses(List<Expression> clauses, OperatorType connector) {
        List<Expression> nonTrueClauses = clauses.stream().filter(e -> {
            if (e.getKind() == Expression.Kind.LITERAL_EXPRESSION) {
                LiteralExpr literalExpr = (LiteralExpr) e;
                return literalExpr.getValue().getLiteralType() != Literal.Type.TRUE;
            }
            return true;
        }).collect(Collectors.toList());
        switch (nonTrueClauses.size()) {
            case 0:
                // We only have a TRUE clause. Return this.
                return clauses.get(0);
            case 1:
                // We do not need to connect a single clause.
                return nonTrueClauses.get(0);
            default:
                // Otherwise, connect all non-true clauses.
                return new OperatorExpr(nonTrueClauses, Collections.nCopies(clauses.size() - 1, connector), false);
        }
    }

    public static Expression appendConnectedClauses(Expression existingClause, List<Expression> clauses,
            OperatorType connector) {
        OperatorExpr resultantExpr = new OperatorExpr();
        for (Expression isomorphismConjunct : clauses) {
            resultantExpr.addOperand(isomorphismConjunct);
            resultantExpr.addOperator(connector);
        }
        // We use the last connector from before to connect our existing clause.
        resultantExpr.addOperand(existingClause);
        return resultantExpr;
    }
}
