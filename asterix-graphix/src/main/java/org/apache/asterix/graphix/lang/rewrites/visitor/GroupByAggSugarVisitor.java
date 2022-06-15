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

import java.util.Collection;
import java.util.Collections;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.clause.CorrLetClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppGroupByAggregationSugarVisitor;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;

/**
 * An extension of {@link SqlppGroupByAggregationSugarVisitor} to properly handle {@link CorrLetClause} nodes.
 */
public class GroupByAggSugarVisitor extends SqlppGroupByAggregationSugarVisitor
        implements ILetCorrelateClauseVisitor<Expression, ILangExpression> {
    public GroupByAggSugarVisitor(LangRewritingContext context, Collection<VarIdentifier> externalVars) {
        super(context, externalVars);
    }

    @Override
    public Expression visit(CorrLetClause corrLetClause, ILangExpression arg) throws CompilationException {
        // Do NOT extend the current scope.
        corrLetClause.setRightExpression(visit(corrLetClause.getRightExpression(), corrLetClause));
        VariableExpr varExpr = corrLetClause.getRightVariable();
        Scope currentScope = scopeChecker.getCurrentScope();
        if (currentScope.findLocalSymbol(varExpr.getVar().getValue()) != null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, varExpr.getSourceLocation(),
                    "Duplicate alias definitions: " + SqlppVariableUtil.toUserDefinedName(varExpr.getVar().getValue()));
        }
        currentScope.addNewVarSymbolToScope(varExpr.getVar(), Collections.emptySet());
        return null;
    }
}
