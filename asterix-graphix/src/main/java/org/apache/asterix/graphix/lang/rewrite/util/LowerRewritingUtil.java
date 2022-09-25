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
package org.apache.asterix.graphix.lang.rewrite.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.rewrite.visitor.GraphixDeepCopyVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.OperatorType;

public final class LowerRewritingUtil {
    public static Expression buildConnectedClauses(List<Expression> clauses, OperatorType connector) {
        switch (clauses.size()) {
            case 0:
                // We must be given a non-empty list.
                throw new IllegalArgumentException("Given non-empty set of clauses.");

            case 1:
                // We do not need to connect a single clause.
                return clauses.get(0);

            default:
                // Otherwise, connect all non-true clauses.
                return new OperatorExpr(clauses, Collections.nCopies(clauses.size() - 1, connector), false);
        }
    }

    public static Expression buildVertexEdgeJoin(List<FieldAccessor> vertexKey, List<FieldAccessor> edgeKey) {
        List<Expression> joinClauses = new ArrayList<>();
        for (int i = 0; i < edgeKey.size(); i++) {
            OperatorExpr equalityExpr = new OperatorExpr(List.of(edgeKey.get(i), vertexKey.get(i)),
                    Collections.singletonList(OperatorType.EQ), false);
            joinClauses.add(equalityExpr);
        }
        return buildConnectedClauses(joinClauses, OperatorType.AND);
    }

    public static List<FieldAccessor> buildAccessorList(Expression startingExpr, List<List<String>> fieldNames)
            throws CompilationException {
        GraphixDeepCopyVisitor deepCopyVisitor = new GraphixDeepCopyVisitor();
        List<FieldAccessor> fieldAccessors = new ArrayList<>();
        for (List<String> nestedField : fieldNames) {
            Expression copiedStartingExpr = (Expression) startingExpr.accept(deepCopyVisitor, null);
            FieldAccessor workingAccessor = new FieldAccessor(copiedStartingExpr, new Identifier(nestedField.get(0)));
            for (String field : nestedField.subList(1, nestedField.size())) {
                workingAccessor = new FieldAccessor(workingAccessor, new Identifier(field));
            }
            fieldAccessors.add(workingAccessor);
        }
        return fieldAccessors;
    }
}
