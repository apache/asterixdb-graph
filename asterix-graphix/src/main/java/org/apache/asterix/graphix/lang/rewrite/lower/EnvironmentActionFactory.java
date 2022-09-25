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
package org.apache.asterix.graphix.lang.rewrite.lower;

import static org.apache.asterix.graphix.lang.rewrite.util.LowerRewritingUtil.buildAccessorList;
import static org.apache.asterix.graphix.lang.rewrite.util.LowerRewritingUtil.buildVertexEdgeJoin;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.common.metadata.EdgeIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.common.metadata.IElementIdentifier;
import org.apache.asterix.graphix.common.metadata.VertexIdentifier;
import org.apache.asterix.graphix.lang.annotation.LoweringExemptAnnotation;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.common.ElementLookupTable;
import org.apache.asterix.graphix.lang.rewrite.lower.action.AbstractInlineAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.MatchSemanticAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.PathPatternAction;
import org.apache.asterix.graphix.lang.rewrite.visitor.ElementBodyAnalysisVisitor.ElementBodyAnalysisContext;
import org.apache.asterix.graphix.lang.rewrite.visitor.GraphixDeepCopyVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.VariableRemapCloneVisitor;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;

/**
 * Build {@link IEnvironmentAction} instances to manipulate a {@link LoweringEnvironment}.
 */
public class EnvironmentActionFactory {
    private final Map<IElementIdentifier, ElementBodyAnalysisContext> analysisContextMap;
    private final ElementLookupTable elementLookupTable;
    private final AliasLookupTable aliasLookupTable;
    private final GraphixRewritingContext graphixRewritingContext;
    private final VariableRemapCloneVisitor remapCloneVisitor;
    private final GraphixDeepCopyVisitor graphixDeepCopyVisitor;

    // The following must be provided before any creation methods are used.
    private GraphIdentifier graphIdentifier;

    public EnvironmentActionFactory(Map<IElementIdentifier, ElementBodyAnalysisContext> analysisContextMap,
            ElementLookupTable elementLookupTable, AliasLookupTable aliasLookupTable,
            GraphixRewritingContext graphixRewritingContext) {
        this.analysisContextMap = analysisContextMap;
        this.elementLookupTable = elementLookupTable;
        this.aliasLookupTable = aliasLookupTable;
        this.graphixRewritingContext = graphixRewritingContext;
        this.graphixDeepCopyVisitor = new GraphixDeepCopyVisitor();
        this.remapCloneVisitor = new VariableRemapCloneVisitor(graphixRewritingContext);
    }

    public void reset(GraphIdentifier graphIdentifier) {
        this.aliasLookupTable.reset();
        this.graphIdentifier = graphIdentifier;
    }

    /**
     * @see PathPatternAction
     */
    public IEnvironmentAction buildPathPatternAction(PathPatternExpr pathPatternExpr) {
        return new PathPatternAction(pathPatternExpr);
    }

    /**
     * @see MatchSemanticAction
     */
    public IEnvironmentAction buildMatchSemanticAction(FromGraphClause fromGraphClause) throws CompilationException {
        return new MatchSemanticAction(graphixRewritingContext, fromGraphClause, aliasLookupTable);
    }

    /**
     * Build an {@link IEnvironmentAction} to introduce a WHERE clause into an environment with the given expression.
     */
    public IEnvironmentAction buildFilterExprAction(Expression filterExpr, VariableExpr elementVariable,
            VariableExpr iterationVariable) throws CompilationException {
        VariableExpr iterationVarCopy = graphixDeepCopyVisitor.visit(iterationVariable, null);
        VariableExpr elementVarCopy = graphixDeepCopyVisitor.visit(elementVariable, null);
        iterationVarCopy.setSourceLocation(filterExpr.getSourceLocation());
        remapCloneVisitor.addSubstitution(elementVarCopy, iterationVarCopy);
        return loweringEnvironment -> loweringEnvironment.acceptTransformer(lowerList -> {
            ILangExpression remapCopyFilterExpr = remapCloneVisitor.substitute(filterExpr);
            WhereClause filterWhereClause = new WhereClause((Expression) remapCopyFilterExpr);
            filterWhereClause.setSourceLocation(filterExpr.getSourceLocation());
            lowerList.addNonRepresentativeClause(filterWhereClause);
            remapCloneVisitor.resetSubstitutions();
        });
    }

    /**
     * Build an {@link IEnvironmentAction} to handle a dangling vertex / vertex that is (currently) disconnected.
     * Even though we introduce CROSS-JOINs here, we will not actually perform this CROSS-JOIN if this is the first
     * vertex we are lowering. There are three possible {@link IEnvironmentAction}s generated here:
     * <ul>
     *  <li>An action for inlined vertices that have no projections.</li>
     *  <li>An action for inlined vertices with projections.</li>
     *  <li>An action for non-inlined vertices.</li>
     * </ul>
     */
    public IEnvironmentAction buildDanglingVertexAction(VertexPatternExpr vertexPatternExpr)
            throws CompilationException {
        if (vertexPatternExpr.findHint(LoweringExemptAnnotation.class) == LoweringExemptAnnotation.INSTANCE) {
            return loweringEnvironment -> {
            };
        }
        VariableExpr vertexVar = vertexPatternExpr.getVariableExpr();
        VariableExpr iterationVar = graphixRewritingContext.getGraphixVariableCopy(vertexVar);
        VariableExpr intermediateVar = graphixRewritingContext.getGraphixVariableCopy(vertexVar);

        // We should only be working with one identifier (given that we only have one label).
        List<VertexIdentifier> vertexElementIDs = vertexPatternExpr.generateIdentifiers(graphIdentifier);
        if (vertexElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Found non-canonical vertex pattern!");
        }
        VertexIdentifier vertexIdentifier = vertexElementIDs.get(0);
        ElementBodyAnalysisContext vertexAnalysisContext = analysisContextMap.get(vertexIdentifier);
        if (vertexAnalysisContext.isExpressionInline() && vertexAnalysisContext.isSelectClauseInline()) {
            return new AbstractInlineAction(graphixRewritingContext, vertexAnalysisContext, iterationVar) {
                @Override
                public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
                    // Introduce our iteration expression.
                    loweringEnvironment.acceptTransformer(lowerList -> {
                        CallExpr datasetCallExpression = vertexAnalysisContext.getDatasetCallExpression();
                        VariableExpr iterationVarCopy = graphixDeepCopyVisitor.visit(iterationVar, null);
                        JoinClause joinClause = new JoinClause(JoinType.INNER, datasetCallExpression, iterationVarCopy,
                                null, new LiteralExpr(TrueLiteral.INSTANCE), null);
                        joinClause.setSourceLocation(vertexPatternExpr.getSourceLocation());
                        lowerList.addNonRepresentativeClause(joinClause);
                    });

                    // Inline our vertex body.
                    super.apply(loweringEnvironment);

                    // If we have a filter expression, add it as a WHERE clause here.
                    final Expression filterExpr = vertexPatternExpr.getFilterExpr();
                    if (filterExpr != null) {
                        loweringEnvironment.acceptAction(buildFilterExprAction(filterExpr, vertexVar, iterationVar));
                    }

                    // Bind our intermediate (join) variable and vertex variable.
                    loweringEnvironment.acceptTransformer(lowerList -> {
                        VariableExpr iterationVarCopy1 = graphixDeepCopyVisitor.visit(iterationVar, null);
                        VariableExpr iterationVarCopy2 = graphixDeepCopyVisitor.visit(iterationVar, null);
                        VariableExpr intermediateVarCopy = graphixDeepCopyVisitor.visit(intermediateVar, null);
                        VariableExpr vertexVarCopy = graphixDeepCopyVisitor.visit(vertexVar, null);
                        LetClause nonRepresentativeBinding = new LetClause(intermediateVarCopy, iterationVarCopy1);
                        lowerList.addNonRepresentativeClause(nonRepresentativeBinding);
                        lowerList.addVertexBinding(vertexVarCopy, iterationVarCopy2);
                    });
                    aliasLookupTable.addIterationAlias(vertexVar, iterationVar);
                    aliasLookupTable.addJoinAlias(vertexVar, intermediateVar);
                }
            };

        } else if (vertexAnalysisContext.isExpressionInline()) {
            return new AbstractInlineAction(graphixRewritingContext, vertexAnalysisContext, iterationVar) {
                @Override
                public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
                    // Introduce our iteration expression.
                    loweringEnvironment.acceptTransformer(lowerList -> {
                        CallExpr datasetCallExpression = vertexAnalysisContext.getDatasetCallExpression();
                        VariableExpr iterationVarCopy = graphixDeepCopyVisitor.visit(iterationVar, null);
                        JoinClause joinClause = new JoinClause(JoinType.INNER, datasetCallExpression, iterationVarCopy,
                                null, new LiteralExpr(TrueLiteral.INSTANCE), null);
                        joinClause.setSourceLocation(vertexPatternExpr.getSourceLocation());
                        lowerList.addNonRepresentativeClause(joinClause);
                    });

                    // Inline our vertex body.
                    super.apply(loweringEnvironment);

                    // If we have a filter expression, add it as a WHERE clause here.
                    final Expression filterExpr = vertexPatternExpr.getFilterExpr();
                    if (filterExpr != null) {
                        loweringEnvironment.acceptAction(buildFilterExprAction(filterExpr, vertexVar, iterationVar));
                    }

                    // Build a record constructor from our context to bind to our vertex variable.
                    loweringEnvironment.acceptTransformer(lowerList -> {
                        VariableExpr intermediateVarCopy = graphixDeepCopyVisitor.visit(intermediateVar, null);
                        RecordConstructor recordConstructor1 = buildRecordConstructor();
                        RecordConstructor recordConstructor2 = buildRecordConstructor();
                        LetClause nonRepresentativeBinding = new LetClause(intermediateVarCopy, recordConstructor1);
                        lowerList.addNonRepresentativeClause(nonRepresentativeBinding);
                        lowerList.addVertexBinding(vertexVar, recordConstructor2);
                    });
                    aliasLookupTable.addIterationAlias(vertexVar, iterationVar);
                    aliasLookupTable.addJoinAlias(vertexVar, intermediateVar);
                }
            };

        } else {
            GraphElementDeclaration elementDeclaration = elementLookupTable.getElementDecl(vertexIdentifier);
            return loweringEnvironment -> {
                // Introduce our iteration expression.
                loweringEnvironment.acceptTransformer(lowerList -> {
                    ILangExpression declBodyCopy = SqlppRewriteUtil.deepCopy(elementDeclaration.getNormalizedBody());
                    VariableExpr iterationVarCopy = graphixDeepCopyVisitor.visit(iterationVar, null);
                    JoinClause joinClause = new JoinClause(JoinType.INNER, (Expression) declBodyCopy, iterationVarCopy,
                            null, new LiteralExpr(TrueLiteral.INSTANCE), null);
                    joinClause.setSourceLocation(vertexPatternExpr.getSourceLocation());
                    lowerList.addNonRepresentativeClause(joinClause);
                });

                // If we have a filter expression, add it as a WHERE clause here.
                final Expression filterExpr = vertexPatternExpr.getFilterExpr();
                if (filterExpr != null) {
                    loweringEnvironment.acceptAction(buildFilterExprAction(filterExpr, vertexVar, iterationVar));
                }

                // Bind our intermediate (join) variable and vertex variable.
                loweringEnvironment.acceptTransformer(lowerList -> {
                    VariableExpr iterationVarCopy1 = graphixDeepCopyVisitor.visit(iterationVar, null);
                    VariableExpr iterationVarCopy2 = graphixDeepCopyVisitor.visit(iterationVar, null);
                    VariableExpr intermediateVarCopy = graphixDeepCopyVisitor.visit(intermediateVar, null);
                    LetClause nonRepresentativeBinding = new LetClause(intermediateVarCopy, iterationVarCopy1);
                    lowerList.addNonRepresentativeClause(nonRepresentativeBinding);
                    lowerList.addVertexBinding(vertexVar, iterationVarCopy2);
                });
                aliasLookupTable.addIterationAlias(vertexVar, iterationVar);
                aliasLookupTable.addJoinAlias(vertexVar, intermediateVar);
            };
        }
    }

    /**
     * Build an {@link IEnvironmentAction} to handle an edge that we can fold into (attach from) an already introduced
     * vertex. A folded edge is implicitly inlined. There are two possible {@link IEnvironmentAction}s generated here:
     * <ul>
     *  <li>An action for inlined, folded edges that have no projections.</li>
     *  <li>An action for inlined, folded edges that have projections.</li>
     * </ul>
     */
    public IEnvironmentAction buildFoldedEdgeAction(VertexPatternExpr vertexPatternExpr,
            EdgePatternExpr edgePatternExpr) throws CompilationException {
        VariableExpr vertexVar = vertexPatternExpr.getVariableExpr();
        VariableExpr edgeVar = edgePatternExpr.getEdgeDescriptor().getVariableExpr();
        VariableExpr intermediateVar = graphixRewritingContext.getGraphixVariableCopy(edgeVar);

        // We should only be working with one identifier (given that we only have one label).
        List<EdgeIdentifier> edgeElementIDs = edgePatternExpr.generateIdentifiers(graphIdentifier);
        if (edgeElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Found non-canonical edge pattern!");
        }
        EdgeIdentifier edgeIdentifier = edgeElementIDs.get(0);
        ElementBodyAnalysisContext edgeAnalysisContext = analysisContextMap.get(edgeIdentifier);
        if (edgeAnalysisContext.isSelectClauseInline()) {
            return new AbstractInlineAction(graphixRewritingContext, edgeAnalysisContext, null) {
                @Override
                public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
                    // We want to bind directly to the iteration variable of our vertex, not the join variable.
                    elementVariable = aliasLookupTable.getIterationAlias(vertexVar);

                    // Inline our edge body.
                    super.apply(loweringEnvironment);

                    // If we have a filter expression, add it as a WHERE clause here.
                    final Expression filterExpr = edgePatternExpr.getEdgeDescriptor().getFilterExpr();
                    if (filterExpr != null) {
                        loweringEnvironment.acceptAction(buildFilterExprAction(filterExpr, edgeVar, elementVariable));
                    }

                    // Build a binding for our edge variable.
                    loweringEnvironment.acceptTransformer(lowerList -> {
                        VariableExpr elementVarCopy1 = graphixDeepCopyVisitor.visit(elementVariable, null);
                        VariableExpr elementVarCopy2 = graphixDeepCopyVisitor.visit(elementVariable, null);
                        VariableExpr intermediateVarCopy = graphixDeepCopyVisitor.visit(intermediateVar, null);
                        LetClause nonRepresentativeBinding = new LetClause(intermediateVarCopy, elementVarCopy1);
                        lowerList.addNonRepresentativeClause(nonRepresentativeBinding);
                        lowerList.addEdgeBinding(edgeVar, elementVarCopy2);
                    });
                    aliasLookupTable.addIterationAlias(edgeVar, elementVariable);
                    aliasLookupTable.addJoinAlias(edgeVar, intermediateVar);
                }
            };

        } else {
            return new AbstractInlineAction(graphixRewritingContext, edgeAnalysisContext, null) {
                @Override
                public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
                    // We want to bind directly to the iteration variable of our vertex, not the join variable.
                    elementVariable = aliasLookupTable.getIterationAlias(vertexVar);

                    // Inline our edge body.
                    super.apply(loweringEnvironment);

                    // If we have a filter expression, add it as a WHERE clause here.
                    final Expression filterExpr = edgePatternExpr.getEdgeDescriptor().getFilterExpr();
                    if (filterExpr != null) {
                        loweringEnvironment.acceptAction(buildFilterExprAction(filterExpr, edgeVar, elementVariable));
                    }

                    // Build a record constructor from our context to bind to our edge and intermediate (join) var.
                    loweringEnvironment.acceptTransformer(lowerList -> {
                        VariableExpr intermediateVarCopy = graphixDeepCopyVisitor.visit(intermediateVar, null);
                        RecordConstructor recordConstructor1 = buildRecordConstructor();
                        RecordConstructor recordConstructor2 = buildRecordConstructor();
                        LetClause nonRepresentativeBinding = new LetClause(intermediateVarCopy, recordConstructor1);
                        lowerList.addNonRepresentativeClause(nonRepresentativeBinding);
                        lowerList.addEdgeBinding(edgeVar, recordConstructor2);
                    });
                    aliasLookupTable.addIterationAlias(edgeVar, elementVariable);
                    aliasLookupTable.addJoinAlias(edgeVar, intermediateVar);
                }
            };
        }
    }

    /**
     * Build an {@link IEnvironmentAction} to handle an edge that we cannot fold into an already introduced vertex.
     * There are three possible {@link IEnvironmentAction}s generated here:
     * <ul>
     *  <li>An action for inlined edges that have no projections.</li>
     *  <li>An action for inlined edges that have projections.</li>
     *  <li>An action for non-inlined edges.</li>
     * </ul>
     */
    public IEnvironmentAction buildNonFoldedEdgeAction(VertexPatternExpr vertexPatternExpr,
            EdgePatternExpr edgePatternExpr, Function<EdgeIdentifier, List<List<String>>> edgeKeyAccess)
            throws CompilationException {
        VariableExpr edgeVar = edgePatternExpr.getEdgeDescriptor().getVariableExpr();
        VariableExpr vertexVar = vertexPatternExpr.getVariableExpr();
        VariableExpr iterationVar = graphixRewritingContext.getGraphixVariableCopy(edgeVar);
        VariableExpr intermediateVar = graphixRewritingContext.getGraphixVariableCopy(edgeVar);

        // We should only be working with one edge identifier...
        List<EdgeIdentifier> edgeElementIDs = edgePatternExpr.generateIdentifiers(graphIdentifier);
        if (edgeElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Found non-canonical edge pattern!");
        }
        EdgeIdentifier edgeIdentifier = edgeElementIDs.get(0);
        ElementBodyAnalysisContext edgeAnalysisContext = analysisContextMap.get(edgeIdentifier);
        Expression datasetCallExpression = edgeAnalysisContext.getDatasetCallExpression();

        // ...and only one vertex identifier (given that we only have one label).
        List<VertexIdentifier> vertexElementIDs = vertexPatternExpr.generateIdentifiers(graphIdentifier);
        if (vertexElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Found non-canonical vertex pattern!");
        }
        VertexIdentifier vertexIdentifier = vertexElementIDs.get(0);
        if (edgeAnalysisContext.isExpressionInline() && edgeAnalysisContext.isSelectClauseInline()) {
            return new AbstractInlineAction(graphixRewritingContext, edgeAnalysisContext, iterationVar) {
                @Override
                public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
                    // Join our edge iteration variable to our vertex variable.
                    loweringEnvironment.acceptTransformer(lowerList -> {
                        VariableExpr vertexJoinExpr = aliasLookupTable.getJoinAlias(vertexVar);
                        VariableExpr vertexVarCopy = graphixDeepCopyVisitor.visit(vertexJoinExpr, null);
                        VariableExpr iterationVarCopy1 = graphixDeepCopyVisitor.visit(iterationVar, null);
                        VariableExpr iterationVarCopy2 = graphixDeepCopyVisitor.visit(iterationVar, null);
                        Expression vertexEdgeJoin = buildVertexEdgeJoin(
                                buildAccessorList(vertexVarCopy, elementLookupTable.getVertexKey(vertexIdentifier)),
                                buildAccessorList(iterationVarCopy1, edgeKeyAccess.apply(edgeIdentifier)));
                        JoinClause joinClause = new JoinClause(JoinType.INNER, datasetCallExpression, iterationVarCopy2,
                                null, vertexEdgeJoin, null);
                        joinClause.setSourceLocation(edgePatternExpr.getSourceLocation());
                        lowerList.addNonRepresentativeClause(joinClause);
                    });

                    // Inline our edge body.
                    super.apply(loweringEnvironment);

                    // If we have a filter expression, add it as a WHERE clause here.
                    final Expression filterExpr = edgePatternExpr.getEdgeDescriptor().getFilterExpr();
                    if (filterExpr != null) {
                        loweringEnvironment.acceptAction(buildFilterExprAction(filterExpr, edgeVar, iterationVar));
                    }

                    // Bind our intermediate (join) variable and edge variable.
                    loweringEnvironment.acceptTransformer(lowerList -> {
                        VariableExpr iterationVarCopy1 = graphixDeepCopyVisitor.visit(iterationVar, null);
                        VariableExpr iterationVarCopy2 = graphixDeepCopyVisitor.visit(iterationVar, null);
                        VariableExpr intermediateVarCopy = graphixDeepCopyVisitor.visit(intermediateVar, null);
                        LetClause nonRepresentativeBinding = new LetClause(intermediateVarCopy, iterationVarCopy1);
                        lowerList.addNonRepresentativeClause(nonRepresentativeBinding);
                        lowerList.addEdgeBinding(edgeVar, iterationVarCopy2);
                    });
                    aliasLookupTable.addIterationAlias(edgeVar, iterationVar);
                    aliasLookupTable.addJoinAlias(edgeVar, intermediateVar);
                }
            };

        } else if (edgeAnalysisContext.isExpressionInline()) {
            return new AbstractInlineAction(graphixRewritingContext, edgeAnalysisContext, iterationVar) {
                @Override
                public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
                    // Join our edge iteration variable to our vertex variable.
                    loweringEnvironment.acceptTransformer(lowerList -> {
                        VariableExpr vertexJoinExpr = aliasLookupTable.getJoinAlias(vertexVar);
                        VariableExpr vertexVarCopy = graphixDeepCopyVisitor.visit(vertexJoinExpr, null);
                        VariableExpr iterationVarCopy1 = graphixDeepCopyVisitor.visit(iterationVar, null);
                        VariableExpr iterationVarCopy2 = graphixDeepCopyVisitor.visit(iterationVar, null);
                        Expression vertexEdgeJoin = buildVertexEdgeJoin(
                                buildAccessorList(vertexVarCopy, elementLookupTable.getVertexKey(vertexIdentifier)),
                                buildAccessorList(iterationVarCopy1, edgeKeyAccess.apply(edgeIdentifier)));
                        JoinClause joinClause = new JoinClause(JoinType.INNER, datasetCallExpression, iterationVarCopy2,
                                null, vertexEdgeJoin, null);
                        joinClause.setSourceLocation(edgePatternExpr.getSourceLocation());
                        lowerList.addNonRepresentativeClause(joinClause);
                    });

                    // Inline our edge body.
                    super.apply(loweringEnvironment);

                    // If we have a filter expression, add it as a WHERE clause here.
                    final Expression filterExpr = edgePatternExpr.getEdgeDescriptor().getFilterExpr();
                    if (filterExpr != null) {
                        loweringEnvironment.acceptAction(buildFilterExprAction(filterExpr, edgeVar, iterationVar));
                    }

                    // Build a record constructor from our context to bind to our edge variable.
                    loweringEnvironment.acceptTransformer(lowerList -> {
                        VariableExpr intermediateVarCopy = graphixDeepCopyVisitor.visit(intermediateVar, null);
                        RecordConstructor recordConstructor1 = buildRecordConstructor();
                        RecordConstructor recordConstructor2 = buildRecordConstructor();
                        LetClause nonRepresentativeBinding = new LetClause(intermediateVarCopy, recordConstructor1);
                        lowerList.addNonRepresentativeClause(nonRepresentativeBinding);
                        lowerList.addEdgeBinding(edgeVar, recordConstructor2);
                    });
                    aliasLookupTable.addIterationAlias(edgeVar, iterationVar);
                    aliasLookupTable.addJoinAlias(edgeVar, intermediateVar);
                }
            };

        } else {
            GraphElementDeclaration elementDeclaration = elementLookupTable.getElementDecl(edgeIdentifier);
            return loweringEnvironment -> {
                // Join our edge body to our vertex variable.
                loweringEnvironment.acceptTransformer(lowerList -> {
                    VariableExpr vertexJoinExpr = aliasLookupTable.getJoinAlias(vertexVar);
                    VariableExpr iterationVarCopy1 = graphixDeepCopyVisitor.visit(iterationVar, null);
                    VariableExpr iterationVarCopy2 = graphixDeepCopyVisitor.visit(iterationVar, null);
                    Expression vertexEdgeJoin = buildVertexEdgeJoin(
                            buildAccessorList(vertexJoinExpr, elementLookupTable.getVertexKey(vertexIdentifier)),
                            buildAccessorList(iterationVarCopy1, edgeKeyAccess.apply(edgeIdentifier)));
                    ILangExpression declBodyCopy = SqlppRewriteUtil.deepCopy(elementDeclaration.getNormalizedBody());
                    JoinClause joinClause = new JoinClause(JoinType.INNER, (Expression) declBodyCopy, iterationVarCopy2,
                            null, vertexEdgeJoin, null);
                    joinClause.setSourceLocation(edgePatternExpr.getSourceLocation());
                    lowerList.addNonRepresentativeClause(joinClause);
                });

                // If we have a filter expression, add it as a WHERE clause here.
                final Expression filterExpr = edgePatternExpr.getEdgeDescriptor().getFilterExpr();
                if (filterExpr != null) {
                    loweringEnvironment.acceptAction(buildFilterExprAction(filterExpr, edgeVar, iterationVar));
                }

                // Bind our intermediate (join) variable and edge variable.
                loweringEnvironment.acceptTransformer(lowerList -> {
                    VariableExpr iterationVarCopy1 = graphixDeepCopyVisitor.visit(iterationVar, null);
                    VariableExpr iterationVarCopy2 = graphixDeepCopyVisitor.visit(iterationVar, null);
                    VariableExpr intermediateVarCopy = graphixDeepCopyVisitor.visit(intermediateVar, null);
                    LetClause nonRepresentativeBinding = new LetClause(intermediateVarCopy, iterationVarCopy1);
                    lowerList.addNonRepresentativeClause(nonRepresentativeBinding);
                    lowerList.addEdgeBinding(edgeVar, iterationVarCopy2);
                });
                aliasLookupTable.addIterationAlias(edgeVar, iterationVar);
                aliasLookupTable.addJoinAlias(edgeVar, intermediateVar);
            };
        }
    }

    /**
     * Build an {@link IEnvironmentAction} to introduce a WHERE-CLAUSE that will correlate a vertex and edge.
     */
    public IEnvironmentAction buildRawJoinVertexAction(VertexPatternExpr vertexPatternExpr,
            EdgePatternExpr edgePatternExpr, Function<EdgeIdentifier, List<List<String>>> edgeKeyAccess)
            throws CompilationException {
        VariableExpr edgeVar = edgePatternExpr.getEdgeDescriptor().getVariableExpr();
        VariableExpr vertexVar = vertexPatternExpr.getVariableExpr();

        // We should only be working with one edge identifier...
        List<EdgeIdentifier> edgeElementIDs = edgePatternExpr.generateIdentifiers(graphIdentifier);
        if (edgeElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Found non-canonical edge pattern!");
        }
        EdgeIdentifier edgeIdentifier = edgeElementIDs.get(0);

        // ...and only one vertex identifier (given that we only have one label).
        List<VertexIdentifier> vertexElementIDs = vertexPatternExpr.generateIdentifiers(graphIdentifier);
        if (vertexElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Found non-canonical vertex pattern!");
        }
        VertexIdentifier vertexIdentifier = vertexElementIDs.get(0);
        return loweringEnvironment -> {
            // No aliases need to be introduced, we just need to add a WHERE-CONJUNCT.
            VariableExpr edgeJoinExpr = aliasLookupTable.getJoinAlias(edgeVar);
            VariableExpr vertexJoinExpr = aliasLookupTable.getJoinAlias(vertexVar);
            VariableExpr edgeJoinExprCopy = graphixDeepCopyVisitor.visit(edgeJoinExpr, null);
            VariableExpr vertexJoinExprCopy = graphixDeepCopyVisitor.visit(vertexJoinExpr, null);
            loweringEnvironment.acceptTransformer(lowerList -> {
                Expression vertexEdgeJoin = buildVertexEdgeJoin(
                        buildAccessorList(vertexJoinExprCopy, elementLookupTable.getVertexKey(vertexIdentifier)),
                        buildAccessorList(edgeJoinExprCopy, edgeKeyAccess.apply(edgeIdentifier)));
                WhereClause whereClause = new WhereClause(vertexEdgeJoin);
                whereClause.setSourceLocation(edgePatternExpr.getSourceLocation());
                lowerList.addNonRepresentativeClause(whereClause);
            });
        };
    }

    /**
     * Build an {@link IEnvironmentAction} to handle a vertex that is bound to an existing (already introduced) edge.
     * There are three possible {@link IEnvironmentAction}s generated here:
     * <ul>
     *  <li>An action for inlined vertices that have no projections.</li>
     *  <li>An action for inlined vertices that have projections.</li>
     *  <li>An action for non-inlined vertices.</li>
     * </ul>
     */
    public IEnvironmentAction buildBoundVertexAction(VertexPatternExpr vertexPatternExpr,
            EdgePatternExpr edgePatternExpr, Function<EdgeIdentifier, List<List<String>>> edgeKeyAccess)
            throws CompilationException {
        VariableExpr edgeVar = edgePatternExpr.getEdgeDescriptor().getVariableExpr();
        VariableExpr vertexVar = vertexPatternExpr.getVariableExpr();
        VariableExpr iterationVar = graphixRewritingContext.getGraphixVariableCopy(vertexVar);
        VariableExpr intermediateVar = graphixRewritingContext.getGraphixVariableCopy(vertexVar);

        // We should only be working with one edge identifier...
        List<EdgeIdentifier> edgeElementIDs = edgePatternExpr.generateIdentifiers(graphIdentifier);
        if (edgeElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Found non-canonical edge pattern!");
        }
        EdgeIdentifier edgeIdentifier = edgeElementIDs.get(0);

        // ...and only one vertex identifier (given that we only have one label).
        List<VertexIdentifier> vertexElementIDs = vertexPatternExpr.generateIdentifiers(graphIdentifier);
        if (vertexElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Found non-canonical vertex pattern!");
        }
        VertexIdentifier vertexIdentifier = vertexElementIDs.get(0);
        ElementBodyAnalysisContext vertexAnalysisContext = analysisContextMap.get(vertexIdentifier);
        Expression datasetCallExpression = vertexAnalysisContext.getDatasetCallExpression();
        if (vertexAnalysisContext.isExpressionInline() && vertexAnalysisContext.isSelectClauseInline()) {
            return new AbstractInlineAction(graphixRewritingContext, vertexAnalysisContext, iterationVar) {
                @Override
                public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
                    // Join our vertex iteration variable to our edge variable.
                    loweringEnvironment.acceptTransformer(lowerList -> {
                        VariableExpr edgeJoinExpr = aliasLookupTable.getJoinAlias(edgeVar);
                        VariableExpr edgeJoinCopy = graphixDeepCopyVisitor.visit(edgeJoinExpr, null);
                        VariableExpr iterationVarCopy1 = graphixDeepCopyVisitor.visit(iterationVar, null);
                        VariableExpr iterationVarCopy2 = graphixDeepCopyVisitor.visit(iterationVar, null);
                        Expression vertexEdgeJoin = buildVertexEdgeJoin(
                                buildAccessorList(iterationVarCopy1, elementLookupTable.getVertexKey(vertexIdentifier)),
                                buildAccessorList(edgeJoinCopy, edgeKeyAccess.apply(edgeIdentifier)));
                        JoinClause joinClause = new JoinClause(JoinType.INNER, datasetCallExpression, iterationVarCopy2,
                                null, vertexEdgeJoin, null);
                        joinClause.setSourceLocation(vertexPatternExpr.getSourceLocation());
                        lowerList.addNonRepresentativeClause(joinClause);
                    });

                    // Inline our vertex body.
                    super.apply(loweringEnvironment);

                    // If we have a filter expression, add it as a WHERE clause here.
                    final Expression filterExpr = vertexPatternExpr.getFilterExpr();
                    if (filterExpr != null) {
                        loweringEnvironment.acceptAction(buildFilterExprAction(filterExpr, vertexVar, iterationVar));
                    }

                    // Bind our intermediate (join) variable and vertex variable.
                    loweringEnvironment.acceptTransformer(lowerList -> {
                        VariableExpr iterationVarCopy1 = graphixDeepCopyVisitor.visit(iterationVar, null);
                        VariableExpr iterationVarCopy2 = graphixDeepCopyVisitor.visit(iterationVar, null);
                        VariableExpr intermediateVarCopy = graphixDeepCopyVisitor.visit(intermediateVar, null);
                        LetClause nonRepresentativeBinding = new LetClause(intermediateVarCopy, iterationVarCopy1);
                        lowerList.addNonRepresentativeClause(nonRepresentativeBinding);
                        lowerList.addVertexBinding(vertexVar, iterationVarCopy2);
                    });
                    aliasLookupTable.addIterationAlias(vertexVar, iterationVar);
                    aliasLookupTable.addJoinAlias(vertexVar, intermediateVar);
                }
            };

        } else if (vertexAnalysisContext.isExpressionInline()) {
            return new AbstractInlineAction(graphixRewritingContext, vertexAnalysisContext, iterationVar) {
                @Override
                public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
                    // Join our vertex iteration variable to our edge variable.
                    loweringEnvironment.acceptTransformer(lowerList -> {
                        VariableExpr edgeJoinExpr = aliasLookupTable.getJoinAlias(edgeVar);
                        VariableExpr edgeJoinCopy = graphixDeepCopyVisitor.visit(edgeJoinExpr, null);
                        VariableExpr iterationVarCopy1 = graphixDeepCopyVisitor.visit(iterationVar, null);
                        VariableExpr iterationVarCopy2 = graphixDeepCopyVisitor.visit(iterationVar, null);
                        Expression vertexEdgeJoin = buildVertexEdgeJoin(
                                buildAccessorList(iterationVarCopy1, elementLookupTable.getVertexKey(vertexIdentifier)),
                                buildAccessorList(edgeJoinCopy, edgeKeyAccess.apply(edgeIdentifier)));
                        JoinClause joinClause = new JoinClause(JoinType.INNER, datasetCallExpression, iterationVarCopy2,
                                null, vertexEdgeJoin, null);
                        joinClause.setSourceLocation(vertexPatternExpr.getSourceLocation());
                        lowerList.addNonRepresentativeClause(joinClause);
                    });

                    // Inline our vertex body.
                    super.apply(loweringEnvironment);

                    // If we have a filter expression, add it as a WHERE clause here.
                    final Expression filterExpr = vertexPatternExpr.getFilterExpr();
                    if (filterExpr != null) {
                        loweringEnvironment.acceptAction(buildFilterExprAction(filterExpr, vertexVar, iterationVar));
                    }

                    // Build a record constructor from our context to bind to our vertex variable.
                    loweringEnvironment.acceptTransformer(lowerList -> {
                        VariableExpr intermediateVarCopy = graphixDeepCopyVisitor.visit(intermediateVar, null);
                        RecordConstructor recordConstructor1 = buildRecordConstructor();
                        RecordConstructor recordConstructor2 = buildRecordConstructor();
                        LetClause nonRepresentativeBinding = new LetClause(intermediateVarCopy, recordConstructor1);
                        lowerList.addNonRepresentativeClause(nonRepresentativeBinding);
                        lowerList.addVertexBinding(vertexVar, recordConstructor2);
                    });
                    aliasLookupTable.addIterationAlias(vertexVar, iterationVar);
                    aliasLookupTable.addJoinAlias(vertexVar, intermediateVar);
                }
            };

        } else {
            GraphElementDeclaration elementDeclaration = elementLookupTable.getElementDecl(vertexIdentifier);
            return loweringEnvironment -> {
                // Join our vertex body to our edge variable.
                loweringEnvironment.acceptTransformer(lowerList -> {
                    VariableExpr edgeJoinExpr = aliasLookupTable.getJoinAlias(edgeVar);
                    VariableExpr edgeJoinCopy = graphixDeepCopyVisitor.visit(edgeJoinExpr, null);
                    VariableExpr iterationVarCopy1 = graphixDeepCopyVisitor.visit(iterationVar, null);
                    VariableExpr iterationVarCopy2 = graphixDeepCopyVisitor.visit(iterationVar, null);
                    Expression vertexEdgeJoin = buildVertexEdgeJoin(
                            buildAccessorList(iterationVarCopy1, elementLookupTable.getVertexKey(vertexIdentifier)),
                            buildAccessorList(edgeJoinCopy, edgeKeyAccess.apply(edgeIdentifier)));
                    ILangExpression declBodyCopy = SqlppRewriteUtil.deepCopy(elementDeclaration.getNormalizedBody());
                    JoinClause joinClause = new JoinClause(JoinType.INNER, (Expression) declBodyCopy, iterationVarCopy2,
                            null, vertexEdgeJoin, null);
                    joinClause.setSourceLocation(vertexPatternExpr.getSourceLocation());
                    lowerList.addNonRepresentativeClause(joinClause);
                });

                // If we have a filter expression, add it as a WHERE clause here.
                final Expression filterExpr = vertexPatternExpr.getFilterExpr();
                if (filterExpr != null) {
                    loweringEnvironment.acceptAction(buildFilterExprAction(filterExpr, vertexVar, iterationVar));
                }

                // Bind our intermediate (join) variable and vertex variable.
                loweringEnvironment.acceptTransformer(lowerList -> {
                    VariableExpr iterationVarCopy1 = graphixDeepCopyVisitor.visit(iterationVar, null);
                    VariableExpr iterationVarCopy2 = graphixDeepCopyVisitor.visit(iterationVar, null);
                    VariableExpr intermediateVarCopy = graphixDeepCopyVisitor.visit(intermediateVar, null);
                    LetClause nonRepresentativeBinding = new LetClause(intermediateVarCopy, iterationVarCopy1);
                    lowerList.addNonRepresentativeClause(nonRepresentativeBinding);
                    lowerList.addVertexBinding(vertexVar, iterationVarCopy2);
                });
                aliasLookupTable.addIterationAlias(vertexVar, iterationVar);
                aliasLookupTable.addJoinAlias(vertexVar, intermediateVar);
            };
        }
    }
}
