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
package org.apache.asterix.graphix.lang.rewrites.lower;

import static org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil.toUserDefinedVariableName;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Consumer;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.clause.CorrLetClause;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.GraphSelectBlock;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrites.lower.action.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrites.lower.transform.CorrelatedClauseSequence;
import org.apache.asterix.graphix.lang.rewrites.lower.transform.ISequenceTransformer;
import org.apache.asterix.graphix.lang.rewrites.visitor.VariableSubstitutionVisitor;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;

/**
 * @see org.apache.asterix.graphix.lang.rewrites.visitor.GraphixLoweringVisitor
 */
public class LoweringEnvironment {
    private final GraphSelectBlock graphSelectBlock;
    private final CorrelatedClauseSequence mainClauseSequence;
    private final GraphixRewritingContext graphixRewritingContext;
    private CorrelatedClauseSequence leftMatchClauseSequence;

    public LoweringEnvironment(GraphSelectBlock graphSelectBlock, GraphixRewritingContext graphixRewritingContext) {
        this.graphixRewritingContext = graphixRewritingContext;
        this.mainClauseSequence = new CorrelatedClauseSequence();
        this.graphSelectBlock = graphSelectBlock;
        this.leftMatchClauseSequence = null;
    }

    public void acceptAction(IEnvironmentAction environmentAction) throws CompilationException {
        environmentAction.apply(this);
    }

    public void acceptTransformer(ISequenceTransformer sequenceTransformer) throws CompilationException {
        boolean isLeftMatch = leftMatchClauseSequence == null;
        sequenceTransformer.accept(isLeftMatch ? mainClauseSequence : leftMatchClauseSequence);
    }

    public void beginLeftMatch() throws CompilationException {
        if (leftMatchClauseSequence != null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                    "LEFT-MATCH lowering is currently in progress!");
        }
        leftMatchClauseSequence = new CorrelatedClauseSequence();
    }

    public void endLeftMatch(IWarningCollector warningCollector) throws CompilationException {
        VariableSubstitutionVisitor substitutionVisitor = new VariableSubstitutionVisitor(graphixRewritingContext);
        VariableExpr nestingVariableExpr = new VariableExpr(graphixRewritingContext.getNewGraphixVariable());
        final Consumer<VarIdentifier> substitutionAdder = v -> {
            VariableExpr sourceNestingVariableExpr = new VariableExpr(nestingVariableExpr.getVar());
            FieldAccessor fieldAccessor = new FieldAccessor(sourceNestingVariableExpr, v);
            substitutionVisitor.addSubstitution(v, fieldAccessor);
        };

        // Build up our projection list.
        List<Projection> projectionList = new ArrayList<>();
        ListIterator<AbstractBinaryCorrelateClause> forProjectIterator = leftMatchClauseSequence.getMainIterator();
        while (forProjectIterator.hasNext()) {
            AbstractBinaryCorrelateClause workingClause = forProjectIterator.next();
            if (workingClause.getClauseType() == Clause.ClauseType.WHERE_CLAUSE) {
                continue;
            }
            VarIdentifier rightVariable = workingClause.getRightVariable().getVar();
            projectionList.add(new Projection(Projection.Kind.NAMED_EXPR, new VariableExpr(rightVariable),
                    toUserDefinedVariableName(rightVariable).getValue()));
            substitutionAdder.accept(rightVariable);
        }

        // Assemble a FROM-TERM from our LEFT-MATCH sequence (do not add our representatives).
        JoinClause headCorrelateClause = (JoinClause) leftMatchClauseSequence.popMainClauseSequence();
        ListIterator<AbstractBinaryCorrelateClause> mainIterator = leftMatchClauseSequence.getMainIterator();
        List<AbstractBinaryCorrelateClause> correlateClauses = IteratorUtils.toList(mainIterator);
        raiseCrossJoinWarning(correlateClauses, warningCollector, null);
        FromTerm fromTerm = new FromTerm(headCorrelateClause.getRightExpression(),
                headCorrelateClause.getRightVariable(), headCorrelateClause.getPositionalVariable(), correlateClauses);

        // Nest our FROM-TERM.
        SelectClause selectClause = new SelectClause(null, new SelectRegular(projectionList), false);
        SelectBlock selectBlock = new SelectBlock(selectClause, new FromClause(List.of(fromTerm)), null, null, null);
        SetOperationInput setOperationInput = new SetOperationInput(selectBlock, null);
        SelectSetOperation selectSetOperation = new SelectSetOperation(setOperationInput, null);
        SelectExpression selectExpression = new SelectExpression(null, selectSetOperation, null, null, true);

        // Attach our assembled sequence back to the main sequence.
        Expression conditionExpression = headCorrelateClause.getConditionExpression();
        Expression newConditionExpression = conditionExpression.accept(substitutionVisitor, null);
        JoinClause leftJoinClause = new JoinClause(JoinType.LEFTOUTER, selectExpression, nestingVariableExpr, null,
                newConditionExpression, Literal.Type.MISSING);
        mainClauseSequence.addMainClause(leftJoinClause);

        // Introduce our representative variables back into  our main sequence.
        for (CorrLetClause representativeVertexBinding : leftMatchClauseSequence.getRepresentativeVertexBindings()) {
            VarIdentifier representativeVariable = representativeVertexBinding.getRightVariable().getVar();
            Expression rightExpression = representativeVertexBinding.getRightExpression();
            Expression reboundExpression = rightExpression.accept(substitutionVisitor, null);
            mainClauseSequence.addRepresentativeVertexBinding(representativeVariable, reboundExpression);
        }
        for (CorrLetClause representativeEdgeBinding : leftMatchClauseSequence.getRepresentativeEdgeBindings()) {
            VarIdentifier representativeVariable = representativeEdgeBinding.getRightVariable().getVar();
            Expression rightExpression = representativeEdgeBinding.getRightExpression();
            Expression reboundExpression = rightExpression.accept(substitutionVisitor, null);
            mainClauseSequence.addRepresentativeEdgeBinding(representativeVariable, reboundExpression);
        }
        leftMatchClauseSequence = null;
    }

    public void finalizeLowering(FromGraphClause fromGraphClause, IWarningCollector warningCollector) {
        AbstractBinaryCorrelateClause headCorrelateClause = mainClauseSequence.popMainClauseSequence();
        List<AbstractBinaryCorrelateClause> correlateClauses = IterableUtils.toList(mainClauseSequence);
        raiseCrossJoinWarning(correlateClauses, warningCollector, fromGraphClause.getSourceLocation());
        FromTerm outputFromTerm = new FromTerm(headCorrelateClause.getRightExpression(),
                headCorrelateClause.getRightVariable(), headCorrelateClause.getPositionalVariable(), correlateClauses);
        graphSelectBlock.setFromClause(new FromClause(List.of(outputFromTerm)));
        graphSelectBlock.getFromClause().setSourceLocation(fromGraphClause.getSourceLocation());
    }

    private static void raiseCrossJoinWarning(List<AbstractBinaryCorrelateClause> correlateClauses,
            IWarningCollector warningCollector, SourceLocation sourceLocation) {
        for (AbstractBinaryCorrelateClause correlateClause : correlateClauses) {
            if (correlateClause.getClauseType() == Clause.ClauseType.JOIN_CLAUSE) {
                JoinClause joinClause = (JoinClause) correlateClause;
                if (joinClause.getConditionExpression().getKind() == Expression.Kind.LITERAL_EXPRESSION) {
                    LiteralExpr literalExpr = (LiteralExpr) joinClause.getConditionExpression();
                    if (literalExpr.getValue().equals(TrueLiteral.INSTANCE) && warningCollector.shouldWarn()) {
                        warningCollector.warn(Warning.of(sourceLocation, ErrorCode.COMPILATION_ERROR,
                                "Potential disconnected pattern encountered! A CROSS-JOIN has been introduced."));
                    }
                }
            }
        }
    }
}
