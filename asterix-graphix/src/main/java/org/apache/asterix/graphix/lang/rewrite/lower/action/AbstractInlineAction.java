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
package org.apache.asterix.graphix.lang.rewrite.lower.action;

import static org.apache.asterix.graphix.lang.rewrite.visitor.ElementBodyAnalysisVisitor.ElementBodyAnalysisContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.visitor.GraphixDeepCopyVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.VariableRemapCloneVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;

/**
 * Inline an element body into a {@link LoweringEnvironment}. This includes:
 * <ol>
 *  <li>Copying {@link UnnestClause}, {@link LetClause}, and {@link WhereClause} AST nodes from our body analysis</li>
 *  <li>Creating {@link RecordConstructor} AST nodes to inline
 *  {@link org.apache.asterix.lang.sqlpp.clause.SelectRegular} nodes.</li>
 * </ol>
 */
public abstract class AbstractInlineAction implements IEnvironmentAction {
    protected final GraphixRewritingContext graphixRewritingContext;
    protected final ElementBodyAnalysisContext bodyAnalysisContext;
    protected final VariableRemapCloneVisitor remapCloneVisitor;
    protected final GraphixDeepCopyVisitor deepCopyVisitor;

    // This may be mutated by our child.
    protected VariableExpr elementVariable;

    protected AbstractInlineAction(GraphixRewritingContext graphixRewritingContext,
            ElementBodyAnalysisContext bodyAnalysisContext, VariableExpr elementVariable) {
        this.graphixRewritingContext = graphixRewritingContext;
        this.bodyAnalysisContext = bodyAnalysisContext;
        this.elementVariable = elementVariable;
        this.remapCloneVisitor = new VariableRemapCloneVisitor(graphixRewritingContext);
        this.deepCopyVisitor = new GraphixDeepCopyVisitor();
    }

    @Override
    public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
        final Function<VariableExpr, VariableExpr> substitutionAdder = v -> {
            VariableExpr reboundVariableExpr = graphixRewritingContext.getGraphixVariableCopy(v);
            remapCloneVisitor.addSubstitution(v, reboundVariableExpr);
            return reboundVariableExpr;
        };

        // To inline, we need to ensure that we substitute variables accordingly.
        remapCloneVisitor.resetSubstitutions();
        if (bodyAnalysisContext.getFromTermVariable() != null) {
            VariableExpr fromTermVariableExpr = bodyAnalysisContext.getFromTermVariable();
            VariableExpr elementVariableExpr = new VariableExpr(elementVariable.getVar());
            remapCloneVisitor.addSubstitution(fromTermVariableExpr, elementVariableExpr);
        }

        // If we have any UNNEST clauses, we need to add these.
        if (bodyAnalysisContext.getUnnestClauses() != null) {
            for (AbstractBinaryCorrelateClause unnestClause : bodyAnalysisContext.getUnnestClauses()) {
                loweringEnvironment.acceptTransformer(lowerList -> {
                    UnnestClause copiedClause = (UnnestClause) remapCloneVisitor.substitute(unnestClause);
                    if (copiedClause.hasPositionalVariable()) {
                        substitutionAdder.apply(copiedClause.getPositionalVariable());
                    }
                    VariableExpr reboundUnnestVariable = substitutionAdder.apply(copiedClause.getRightVariable());
                    UnnestClause newUnnestClause =
                            new UnnestClause(copiedClause.getUnnestType(), copiedClause.getRightExpression(),
                                    reboundUnnestVariable, null, copiedClause.getOuterUnnestMissingValueType());
                    newUnnestClause.setSourceLocation(unnestClause.getSourceLocation());
                    lowerList.addNonRepresentativeClause(newUnnestClause);
                });
            }
        }

        // If we have any LET clauses, we need to substitute them in our WHERE and SELECT clauses.
        if (bodyAnalysisContext.getLetClauses() != null) {
            for (LetClause letClause : bodyAnalysisContext.getLetClauses()) {
                // Remap this LET-CLAUSE to include our new variables. Move this to our correlated clauses.
                LetClause copiedClause = (LetClause) remapCloneVisitor.substitute(letClause);
                VariableExpr reboundLetVariable = substitutionAdder.apply(copiedClause.getVarExpr());
                VariableExpr reboundLetVariableCopy = deepCopyVisitor.visit(reboundLetVariable, null);
                Expression copiedBindingExpr = copiedClause.getBindingExpr();
                loweringEnvironment.acceptTransformer(lowerList -> {
                    LetClause reboundLetClause = new LetClause(reboundLetVariableCopy, copiedBindingExpr);
                    reboundLetClause.setSourceLocation(letClause.getSourceLocation());
                    lowerList.addNonRepresentativeClause(reboundLetClause);
                });
            }
        }

        // If we have any WHERE clauses, we need to add these.
        if (bodyAnalysisContext.getWhereClauses() != null) {
            for (WhereClause whereClause : bodyAnalysisContext.getWhereClauses()) {
                WhereClause copiedClause = (WhereClause) remapCloneVisitor.substitute(whereClause);
                loweringEnvironment.acceptTransformer(lowerList -> {
                    WhereClause newWhereClause = new WhereClause(copiedClause.getWhereExpr());
                    newWhereClause.setSourceLocation(whereClause.getSourceLocation());
                    lowerList.addNonRepresentativeClause(newWhereClause);
                });
            }
        }
    }

    protected RecordConstructor buildRecordConstructor() throws CompilationException {
        if (bodyAnalysisContext.getSelectClauseProjections() != null) {
            // Map our original variable to our element variable.
            List<FieldBinding> fieldBindings = new ArrayList<>();
            for (Projection projection : bodyAnalysisContext.getSelectClauseProjections()) {
                LiteralExpr fieldNameExpr = new LiteralExpr(new StringLiteral(projection.getName()));
                ILangExpression fieldValueExpr = remapCloneVisitor.substitute(projection.getExpression());
                fieldBindings.add(new FieldBinding(fieldNameExpr, (Expression) fieldValueExpr));
            }
            return new RecordConstructor(fieldBindings);

        } else {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                    "Non-inlineable SELECT clause encountered, but was body was marked as inline!");
        }
    }
}
