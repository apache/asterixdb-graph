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
package org.apache.asterix.graphix.lang.rewrite.visitor;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.VariableSubstitutionEnvironment;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.visitor.SqlppCloneAndSubstituteVariablesVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * An extension of {@link SqlppCloneAndSubstituteVariablesVisitor} that also remaps binding variables.
 */
public class VariableRemapCloneVisitor extends SqlppCloneAndSubstituteVariablesVisitor {
    private final Map<VariableExpr, Expression> variableToExpressionMap = new HashMap<>();

    public VariableRemapCloneVisitor(GraphixRewritingContext graphixRewritingContext) {
        super(graphixRewritingContext);
    }

    public void addSubstitution(VariableExpr variable, Expression newExpr) {
        variableToExpressionMap.put(variable, newExpr);
    }

    public void resetSubstitutions() {
        variableToExpressionMap.clear();
    }

    public ILangExpression substitute(ILangExpression langExpression) throws CompilationException {
        return langExpression.accept(this, createKeepSubstitutionEnvironment()).first;
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(FromTerm fromTerm,
            VariableSubstitutionEnvironment env) throws CompilationException {
        Pair<ILangExpression, VariableSubstitutionEnvironment> pair = super.visit(fromTerm, env);
        if (env.constainsOldVar(fromTerm.getLeftVariable())
                || (fromTerm.hasPositionalVariable() && env.constainsOldVar(fromTerm.getPositionalVariable()))) {
            FromTerm fromTermAfterVisit = (FromTerm) pair.first;
            VariableExpr remapLeftVariable = (VariableExpr) rewriteVariableExpr(fromTerm.getLeftVariable(), env);
            VariableExpr remapPositionalVariable = fromTerm.hasPositionalVariable()
                    ? (VariableExpr) rewriteVariableExpr(fromTerm.getPositionalVariable(), env) : null;
            FromTerm fromTermWithRemapVariable = new FromTerm(fromTermAfterVisit.getLeftExpression(), remapLeftVariable,
                    remapPositionalVariable, fromTermAfterVisit.getCorrelateClauses());
            fromTermWithRemapVariable.setSourceLocation(fromTermAfterVisit.getSourceLocation());
            return new Pair<>(fromTermWithRemapVariable, pair.second);

        } else {
            return pair;
        }
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(JoinClause joinClause,
            VariableSubstitutionEnvironment env) throws CompilationException {
        Pair<ILangExpression, VariableSubstitutionEnvironment> pair = super.visit(joinClause, env);
        if (env.constainsOldVar(joinClause.getRightVariable())
                || (joinClause.hasPositionalVariable() && env.constainsOldVar(joinClause.getPositionalVariable()))) {
            JoinClause joinClauseAfterVisit = (JoinClause) pair.first;
            VariableExpr remapRightVariable = (VariableExpr) rewriteVariableExpr(joinClause.getRightVariable(), env);
            VariableExpr remapPositionalVariable = joinClause.hasPositionalVariable()
                    ? (VariableExpr) rewriteVariableExpr(joinClause.getPositionalVariable(), env) : null;

            // A new environment gets created for our condition expression in our parent, so we need to revisit here.
            ILangExpression remapConditionExpression = joinClause.getConditionExpression().accept(this, env).first;
            JoinClause joinClauseWithRemapVariable = new JoinClause(joinClauseAfterVisit.getJoinType(),
                    joinClauseAfterVisit.getRightExpression(), remapRightVariable, remapPositionalVariable,
                    (Expression) remapConditionExpression, joinClauseAfterVisit.getOuterJoinMissingValueType());
            joinClauseWithRemapVariable.setSourceLocation(joinClauseAfterVisit.getSourceLocation());
            return new Pair<>(joinClauseWithRemapVariable, pair.second);

        } else {
            return pair;
        }
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(UnnestClause unnestClause,
            VariableSubstitutionEnvironment env) throws CompilationException {
        Pair<ILangExpression, VariableSubstitutionEnvironment> pair = super.visit(unnestClause, env);
        if (env.constainsOldVar(unnestClause.getRightVariable()) || (unnestClause.hasPositionalVariable()
                && env.constainsOldVar(unnestClause.getPositionalVariable()))) {
            UnnestClause unnestClauseAfterVisit = (UnnestClause) pair.first;
            VariableExpr remapRightVariable = (VariableExpr) rewriteVariableExpr(unnestClause.getRightVariable(), env);
            VariableExpr remapPositionalVariable = unnestClause.hasPositionalVariable()
                    ? (VariableExpr) rewriteVariableExpr(unnestClause.getPositionalVariable(), env) : null;
            UnnestClause unnestClauseWithRemapVariable = new UnnestClause(unnestClauseAfterVisit.getUnnestType(),
                    unnestClauseAfterVisit.getRightExpression(), remapRightVariable, remapPositionalVariable,
                    unnestClauseAfterVisit.getOuterUnnestMissingValueType());
            unnestClauseWithRemapVariable.setSourceLocation(unnestClauseAfterVisit.getSourceLocation());
            return new Pair<>(unnestClauseWithRemapVariable, pair.second);

        } else {
            return pair;
        }
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(LetClause letClause,
            VariableSubstitutionEnvironment env) throws CompilationException {
        Pair<ILangExpression, VariableSubstitutionEnvironment> pair = super.visit(letClause, env);
        if (env.constainsOldVar(letClause.getVarExpr())) {
            LetClause letClauseAfterVisit = (LetClause) pair.first;
            VariableExpr remapVariable = (VariableExpr) rewriteVariableExpr(letClause.getVarExpr(), env);
            LetClause letClauseWithRemapVariable = new LetClause(remapVariable, letClauseAfterVisit.getBindingExpr());
            letClauseWithRemapVariable.setSourceLocation(letClauseAfterVisit.getSourceLocation());
            return new Pair<>(letClauseWithRemapVariable, pair.second);

        } else {
            return pair;
        }
    }

    private VariableSubstitutionEnvironment createKeepSubstitutionEnvironment() {
        return new VariableSubstitutionEnvironment(variableToExpressionMap) {
            @Override
            public void removeSubstitution(VariableExpr oldVar) {
                // We don't want to remove our substitution here.
            }
        };
    }
}
