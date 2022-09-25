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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.algebra.compiler.option.ElementEvaluationOption;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.canonical.CanonicalElementBranchConsumer;
import org.apache.asterix.graphix.lang.rewrite.canonical.CanonicalElementExpansionConsumer;
import org.apache.asterix.graphix.lang.rewrite.canonical.CanonicalElementGeneratorFactory;
import org.apache.asterix.graphix.lang.rewrite.common.BranchLookupTable;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.visitor.CheckSql92AggregateVisitor;

/**
 * Perform a canonicalization pass, which aims to list out unambiguous <i>navigational complex graph patterns</i>.
 * <p>
 * For each SELECT-EXPR, we perform the following:
 * <ol>
 *  <li>Collect all ambiguous graph elements in each SELECT-SET-OP.</li>
 *  <li>Enumerate each canonical form of each ambiguous graph element.</li>
 *  <li>Invoke one of two canonical element consumers for all ambiguous elements:
 *   <ol>
 *    <li>An expansion consumer, which will generate N UNION-ALL branches for N canonical forms of some element.</li>
 *    <li>A branch consumer, which will populate a separate {@link BranchLookupTable} that holds all canonical forms of
 *    some element.</li>
 *   </ol></li>
 *  <li>If our expansion consumer has fired, then we must ensure that the generated UNION-ALL does not affect any
 *  grouping or sorts. Call {@link PostCanonicalExpansionVisitor}.</li>
 * </ol>
 */
public class QueryCanonicalizationVisitor extends AbstractGraphixQueryVisitor {
    private final CheckSql92AggregateVisitor checkSql92AggregateVisitor;
    private final GraphixDeepCopyVisitor graphixDeepCopyVisitor;
    private final GraphixRewritingContext graphixRewritingContext;
    private final BranchLookupTable branchLookupTable;

    public QueryCanonicalizationVisitor(BranchLookupTable branchLookupTable,
            GraphixRewritingContext graphixRewritingContext) {
        this.checkSql92AggregateVisitor = new CheckSql92AggregateVisitor();
        this.graphixDeepCopyVisitor = new GraphixDeepCopyVisitor();
        this.graphixRewritingContext = graphixRewritingContext;
        this.branchLookupTable = branchLookupTable;
    }

    @Override
    public Expression visit(SelectExpression selectExpression, ILangExpression arg) throws CompilationException {
        // Visit our SELECT-EXPR-level LET-CLAUSEs.
        for (LetClause letClause : selectExpression.getLetList()) {
            letClause.accept(this, selectExpression);
        }

        // First pass: collect all ambiguous graph elements.
        AmbiguousElementVisitor ambiguousElementVisitor = new AmbiguousElementVisitor(graphixRewritingContext);
        SelectSetOperation selectSetOperation = selectExpression.getSelectSetOperation();
        selectSetOperation.accept(ambiguousElementVisitor, selectExpression);
        if (ambiguousElementVisitor.getAmbiguousElements().isEmpty()) {
            // We have no ambiguous elements. Our [sub]query is in canonical form, and we can exit here.
            return selectExpression;
        }

        // Second pass: enumerate all canonical forms of each ambiguous element.
        Map<AbstractExpression, List<? extends AbstractExpression>> canonicalElementMap = new HashMap<>();
        for (AbstractExpression element : ambiguousElementVisitor.getAmbiguousElements()) {
            CanonicalElementGeneratorFactory factory = ambiguousElementVisitor.getGeneratorFactory(element);
            if (element instanceof VertexPatternExpr) {
                VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) element;
                List<VertexPatternExpr> canonicalVertices = factory.generateCanonicalVertices(vertexPatternExpr);
                canonicalElementMap.put(element, canonicalVertices);

            } else { // ambiguousElement instanceof EdgePatternExpr
                EdgePatternExpr edgePatternExpr = (EdgePatternExpr) element;
                EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
                if (edgeDescriptor.getPatternType() == EdgeDescriptor.PatternType.EDGE) {
                    List<EdgePatternExpr> canonicalEdges = factory.generateCanonicalEdges(edgePatternExpr);
                    canonicalElementMap.put(element, canonicalEdges);

                } else { // edgeDescriptor.getPatternType() == EdgeDescriptor.PatternType.PATH
                    ElementEvaluationOption option = ambiguousElementVisitor.getElementEvaluationOption(element);
                    List<PathPatternExpr> canonicalPaths = factory.generateCanonicalPaths(edgePatternExpr,
                            option == ElementEvaluationOption.SWITCH_AND_CYCLE);
                    canonicalElementMap.put(element, canonicalPaths);
                }
            }
        }

        // Third pass: invoke the appropriate consumer for each canonical element.
        SelectExpression selectExpressionCopy = graphixDeepCopyVisitor.visit(selectExpression, null);
        CanonicalElementExpansionConsumer expansionConsumer =
                new CanonicalElementExpansionConsumer(selectExpressionCopy, graphixRewritingContext);
        CanonicalElementBranchConsumer branchConsumer = new CanonicalElementBranchConsumer(branchLookupTable);
        Map<SelectBlock, List<AbstractExpression>> groupedElements = new HashMap<>();
        ambiguousElementVisitor.getAmbiguousElements().forEach(e -> {
            SelectBlock sourceSelectBlock = ambiguousElementVisitor.getSourceSelectBlock(e);
            groupedElements.putIfAbsent(sourceSelectBlock, new ArrayList<>());
            groupedElements.get(sourceSelectBlock).add(e);
        });
        for (Map.Entry<SelectBlock, List<AbstractExpression>> entry : groupedElements.entrySet()) {
            expansionConsumer.resetSelectBlock(entry.getKey());
            for (AbstractExpression ambiguousElement : entry.getValue()) {
                ElementEvaluationOption option = ambiguousElementVisitor.getElementEvaluationOption(ambiguousElement);
                if (option == ElementEvaluationOption.EXPAND_AND_UNION) {
                    expansionConsumer.accept(ambiguousElement, canonicalElementMap.get(ambiguousElement));

                } else { // option == ElementEvaluationOption.SWITCH_AND_CYCLE
                    branchConsumer.accept(ambiguousElement, canonicalElementMap.get(ambiguousElement));
                }
            }
        }

        // Check if we have any output-modifiers / grouping, that we have no SET-OPs, and if expansion has occurred.
        boolean hasRightInputs = selectSetOperation.hasRightInputs();
        boolean hasOutputModifiers = selectExpression.hasLimit() || selectExpression.hasOrderby();
        boolean hasGroupBy = false, hasAggregation = false;
        SelectBlock originalLeftSelectBlock = selectSetOperation.getLeftInput().getSelectBlock();
        if (selectSetOperation.getLeftInput().selectBlock()) {
            hasGroupBy = originalLeftSelectBlock.hasGroupbyClause();
            hasAggregation = checkSql92AggregateVisitor.visit(originalLeftSelectBlock, null);
            hasAggregation |= originalLeftSelectBlock.getSelectClause().distinct();
        }

        // Finalize our expansion consumer.
        Set<SelectBlock> generatedSelectBlocks = new HashSet<>();
        expansionConsumer.finalize(selectExpression, generatedSelectBlocks::add);

        // Perform a post-canonical expansion pass if necessary.
        boolean hasExpansionOccurred = !generatedSelectBlocks.isEmpty();
        if (hasExpansionOccurred && !hasRightInputs && (hasOutputModifiers | hasGroupBy | hasAggregation)) {
            Set<VariableExpr> liveVariables = new HashSet<>();
            FromGraphClause fromGraphClause = (FromGraphClause) originalLeftSelectBlock.getFromClause();
            for (MatchClause matchClause : fromGraphClause.getMatchClauses()) {
                for (PathPatternExpr pathExpression : matchClause.getPathExpressions()) {
                    if (pathExpression.getVariableExpr() != null) {
                        liveVariables.add(pathExpression.getVariableExpr());
                    }
                    for (VertexPatternExpr vertexExpression : pathExpression.getVertexExpressions()) {
                        VariableExpr vertexVariable = vertexExpression.getVariableExpr();
                        liveVariables.add(vertexVariable);
                    }
                    for (EdgePatternExpr edgeExpression : pathExpression.getEdgeExpressions()) {
                        VariableExpr edgeVariable = edgeExpression.getEdgeDescriptor().getVariableExpr();
                        liveVariables.add(edgeVariable);
                    }
                }
            }
            if (!fromGraphClause.getCorrelateClauses().isEmpty()) {
                List<AbstractBinaryCorrelateClause> correlateClauses = fromGraphClause.getCorrelateClauses();
                for (AbstractBinaryCorrelateClause correlateClause : correlateClauses) {
                    VariableExpr bindingVariable = correlateClause.getRightVariable();
                    liveVariables.add(bindingVariable);
                }
            }
            if (originalLeftSelectBlock.hasLetWhereClauses()) {
                for (AbstractClause abstractClause : originalLeftSelectBlock.getLetWhereList()) {
                    if (abstractClause.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                        LetClause letClause = (LetClause) abstractClause;
                        VariableExpr bindingVariable = letClause.getVarExpr();
                        liveVariables.add(bindingVariable);
                    }
                }
            }
            return new PostCanonicalExpansionVisitor(graphixRewritingContext, originalLeftSelectBlock,
                    generatedSelectBlocks, liveVariables).visit(selectExpression, arg);
        }

        // Return our current SELECT-EXPR.
        return selectExpression;
    }
}
