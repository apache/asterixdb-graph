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

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;

/**
 * Substitute qualifying {@link VariableExpr} nodes (via their {@link VarIdentifier}) with a deep-copy of an expression.
 */
public class VariableSubstitutionVisitor extends AbstractSqlppExpressionScopingVisitor {
    private final Map<VarIdentifier, Expression> substitutionMap = new HashMap<>();

    public VariableSubstitutionVisitor(LangRewritingContext context) {
        super(context);
    }

    public void addSubstitution(VarIdentifier varIdentifier, Expression substitution) {
        substitutionMap.put(varIdentifier, substitution);
    }

    @Override
    public Expression visit(VariableExpr variableExpr, ILangExpression arg) throws CompilationException {
        Expression substitution = substitutionMap.getOrDefault(variableExpr.getVar(), variableExpr);
        if (substitution != null) {
            substitution = (Expression) SqlppRewriteUtil.deepCopy(substitution);
        }
        return substitution;
    }
}
