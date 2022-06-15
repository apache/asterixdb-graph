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
package org.apache.asterix.graphix.lang.rewrites.lower.action;

import static org.apache.asterix.graphix.lang.rewrites.visitor.ElementBodyAnalysisVisitor.ElementBodyAnalysisContext;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.clause.CorrLetClause;
import org.apache.asterix.graphix.lang.clause.CorrWhereClause;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrites.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrites.visitor.VariableSubstitutionVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;

/**
 * Inline an element body into a {@link LoweringEnvironment}. This includes a) copying {@link UnnestClause},
 * {@link LetClause}, and {@link WhereClause} AST nodes from our body analysis, and b) creating
 * {@link RecordConstructor} AST nodes to inline {@link org.apache.asterix.lang.sqlpp.clause.SelectRegular} nodes.
 */
public abstract class AbstractInlineAction implements IEnvironmentAction {
    protected final GraphixRewritingContext graphixRewritingContext;
    protected final ElementBodyAnalysisContext bodyAnalysisContext;

    // This may be mutated by our child.
    protected VarIdentifier elementVariable;

    // The following is reset on each application.
    private VariableSubstitutionVisitor substitutionVisitor;

    protected AbstractInlineAction(GraphixRewritingContext graphixRewritingContext,
            ElementBodyAnalysisContext bodyAnalysisContext, VarIdentifier elementVariable) {
        this.graphixRewritingContext = graphixRewritingContext;
        this.bodyAnalysisContext = bodyAnalysisContext;
        this.elementVariable = elementVariable;
    }

    @Override
    public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
        // To inline, we need to ensure that we substitute variables accordingly.
        substitutionVisitor = new VariableSubstitutionVisitor(graphixRewritingContext);
        if (bodyAnalysisContext.getFromTermVariable() != null) {
            VariableExpr fromTermVariableExpr = bodyAnalysisContext.getFromTermVariable();
            VariableExpr elementVariableExpr = new VariableExpr(elementVariable);
            substitutionVisitor.addSubstitution(fromTermVariableExpr.getVar(), elementVariableExpr);
        }

        // If we have any UNNEST clauses, we need to add these.
        if (bodyAnalysisContext.getUnnestClauses() != null) {
            for (AbstractBinaryCorrelateClause unnestClause : bodyAnalysisContext.getUnnestClauses()) {
                VarIdentifier reboundUnnestVariableID = graphixRewritingContext.getNewGraphixVariable();
                VariableExpr reboundVariableExpr = new VariableExpr(reboundUnnestVariableID);

                // Remap this UNNEST-CLAUSE to include our new variables.
                loweringEnvironment.acceptTransformer(clauseSequence -> {
                    UnnestClause copiedClause = (UnnestClause) SqlppRewriteUtil.deepCopy(unnestClause);
                    copiedClause.accept(substitutionVisitor, null);
                    VariableExpr substitutionVariableExpr = (copiedClause.hasPositionalVariable())
                            ? copiedClause.getPositionalVariable() : copiedClause.getRightVariable();
                    substitutionVisitor.addSubstitution(substitutionVariableExpr.getVar(), reboundVariableExpr);
                    UnnestClause newUnnestClause = new UnnestClause(copiedClause.getUnnestType(),
                            copiedClause.getRightExpression(), new VariableExpr(reboundUnnestVariableID), null,
                            copiedClause.getOuterUnnestMissingValueType());
                    clauseSequence.addMainClause(newUnnestClause);
                });
            }
        }

        // If we have any LET clauses, we need to substitute them in our WHERE and SELECT clauses.
        if (bodyAnalysisContext.getLetClauses() != null) {
            for (LetClause letClause : bodyAnalysisContext.getLetClauses()) {
                VarIdentifier reboundLetVariableID = graphixRewritingContext.getNewGraphixVariable();
                VariableExpr reboundVariableExpr = new VariableExpr(reboundLetVariableID);

                // Remap this LET-CLAUSE to include our new variables. Move this to our correlated clauses.
                LetClause copiedClause = (LetClause) SqlppRewriteUtil.deepCopy(letClause);
                copiedClause.accept(substitutionVisitor, null);
                Expression copiedBindingExpr = copiedClause.getBindingExpr();
                substitutionVisitor.addSubstitution(copiedClause.getVarExpr().getVar(), reboundVariableExpr);
                loweringEnvironment.acceptTransformer(corrSequence -> {
                    VariableExpr reboundLetVariableExpr = new VariableExpr(reboundLetVariableID);
                    corrSequence.addMainClause(new CorrLetClause(copiedBindingExpr, reboundLetVariableExpr, null));
                });
            }
        }

        // If we have any WHERE clauses, we need to add these.
        if (bodyAnalysisContext.getWhereClauses() != null) {
            for (WhereClause whereClause : bodyAnalysisContext.getWhereClauses()) {
                WhereClause copiedClause = (WhereClause) SqlppRewriteUtil.deepCopy(whereClause);
                copiedClause.accept(substitutionVisitor, null);
                loweringEnvironment.acceptTransformer(corrSequence -> {
                    CorrWhereClause corrWhereClause = new CorrWhereClause(copiedClause.getWhereExpr());
                    corrSequence.addMainClause(corrWhereClause);
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
                ILangExpression copiedExpr = SqlppRewriteUtil.deepCopy(projection.getExpression());
                Expression fieldValueExpr = copiedExpr.accept(substitutionVisitor, null);
                fieldBindings.add(new FieldBinding(fieldNameExpr, fieldValueExpr));
            }
            return new RecordConstructor(fieldBindings);

        } else {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                    "Non-inlineable SELECT clause encountered, but was body was marked as inline!");
        }
    }
}
