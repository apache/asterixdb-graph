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

import static org.apache.asterix.graphix.lang.rewrites.util.LowerRewritingUtil.buildAccessorList;
import static org.apache.asterix.graphix.lang.rewrites.util.LowerRewritingUtil.buildVertexEdgeJoin;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.clause.CorrLetClause;
import org.apache.asterix.graphix.lang.clause.CorrWhereClause;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrites.common.ElementLookupTable;
import org.apache.asterix.graphix.lang.rewrites.lower.action.AbstractInlineAction;
import org.apache.asterix.graphix.lang.rewrites.lower.action.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrites.lower.action.IsomorphismAction;
import org.apache.asterix.graphix.lang.rewrites.lower.action.PathPatternAction;
import org.apache.asterix.graphix.lang.rewrites.visitor.ElementBodyAnalysisVisitor.ElementBodyAnalysisContext;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.optype.JoinType;

/**
 * Build {@link IEnvironmentAction} instances to manipulate a {@link LoweringEnvironment}.
 */
public class EnvironmentActionFactory {
    private final Map<GraphElementIdentifier, ElementBodyAnalysisContext> analysisContextMap;
    private final ElementLookupTable elementLookupTable;
    private final LoweringAliasLookupTable aliasLookupTable;
    private final GraphixRewritingContext graphixRewritingContext;

    // The following must be provided before any creation methods are used.
    private GraphIdentifier graphIdentifier;

    public EnvironmentActionFactory(Map<GraphElementIdentifier, ElementBodyAnalysisContext> analysisContextMap,
            ElementLookupTable elementLookupTable, LoweringAliasLookupTable aliasLookupTable,
            GraphixRewritingContext graphixRewritingContext) {
        this.analysisContextMap = analysisContextMap;
        this.elementLookupTable = elementLookupTable;
        this.aliasLookupTable = aliasLookupTable;
        this.graphixRewritingContext = graphixRewritingContext;
    }

    public void reset(GraphIdentifier graphIdentifier) {
        this.graphIdentifier = graphIdentifier;
        this.aliasLookupTable.reset();
    }

    /**
     * @see PathPatternAction
     */
    public IEnvironmentAction buildPathPatternAction(PathPatternExpr pathPatternExpr) {
        return new PathPatternAction(pathPatternExpr);
    }

    /**
     * @see IsomorphismAction
     */
    public IEnvironmentAction buildIsomorphismAction(FromGraphClause fromGraphClause) throws CompilationException {
        return new IsomorphismAction(graphixRewritingContext, fromGraphClause, aliasLookupTable);
    }

    /**
     * Build an {@link IEnvironmentAction} to handle a dangling vertex / vertex that is (currently) disconnected.
     * Even though we introduce CROSS-JOINs here, we will not actually perform this CROSS-JOIN if this is the first
     * vertex we are lowering. There are three possible {@link IEnvironmentAction}s generated here:
     * 1. An action for inlined vertices that have no projections.
     * 2. An action for inlined vertices with projections.
     * 3. An action for non-inlined vertices.
     */
    public IEnvironmentAction buildDanglingVertexAction(VertexPatternExpr vertexPatternExpr)
            throws CompilationException {
        VarIdentifier vertexVar = vertexPatternExpr.getVariableExpr().getVar();
        VarIdentifier iterationVar = graphixRewritingContext.getNewGraphixVariable();
        VarIdentifier intermediateVar = graphixRewritingContext.getNewGraphixVariable();

        // We should only be working with one identifier (given that we only have one label).
        List<GraphElementIdentifier> vertexElementIDs = vertexPatternExpr.generateIdentifiers(graphIdentifier);
        if (vertexElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Found non-canonical vertex pattern!");
        }
        GraphElementIdentifier vertexIdentifier = vertexElementIDs.get(0);
        ElementBodyAnalysisContext vertexAnalysisContext = analysisContextMap.get(vertexIdentifier);
        if (vertexAnalysisContext.isExpressionInline() && vertexAnalysisContext.isSelectClauseInline()) {
            return new AbstractInlineAction(graphixRewritingContext, vertexAnalysisContext, iterationVar) {
                @Override
                public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
                    // Introduce our iteration expression.
                    loweringEnvironment.acceptTransformer(clauseSequence -> {
                        CallExpr datasetCallExpression = vertexAnalysisContext.getDatasetCallExpression();
                        JoinClause joinClause = new JoinClause(JoinType.INNER, datasetCallExpression,
                                new VariableExpr(iterationVar), null, new LiteralExpr(TrueLiteral.INSTANCE), null);
                        clauseSequence.addMainClause(joinClause);
                    });

                    // Inline our vertex body.
                    super.apply(loweringEnvironment);

                    // Bind our intermediate (join) variable and vertex variable.
                    loweringEnvironment.acceptTransformer(clauseSequence -> {
                        VariableExpr iterationVarExpr = new VariableExpr(iterationVar);
                        VariableExpr intermediateVarExpr = new VariableExpr(intermediateVar);
                        clauseSequence.addMainClause(new CorrLetClause(iterationVarExpr, intermediateVarExpr, null));
                        clauseSequence.addRepresentativeVertexBinding(vertexVar, new VariableExpr(iterationVar));
                    });
                    aliasLookupTable.putIterationAlias(vertexVar, iterationVar);
                    aliasLookupTable.putJoinAlias(vertexVar, intermediateVar);
                }
            };

        } else if (vertexAnalysisContext.isExpressionInline()) {
            return new AbstractInlineAction(graphixRewritingContext, vertexAnalysisContext, iterationVar) {
                @Override
                public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
                    // Introduce our iteration expression.
                    loweringEnvironment.acceptTransformer(clauseSequence -> {
                        CallExpr datasetCallExpression = vertexAnalysisContext.getDatasetCallExpression();
                        JoinClause joinClause = new JoinClause(JoinType.INNER, datasetCallExpression,
                                new VariableExpr(iterationVar), null, new LiteralExpr(TrueLiteral.INSTANCE), null);
                        clauseSequence.addMainClause(joinClause);
                    });

                    // Inline our vertex body.
                    super.apply(loweringEnvironment);

                    // Build a record constructor from our context to bind to our vertex variable.
                    loweringEnvironment.acceptTransformer(clauseSequence -> {
                        VariableExpr intermediateVarExpr = new VariableExpr(intermediateVar);
                        RecordConstructor recordConstructor1 = buildRecordConstructor();
                        RecordConstructor recordConstructor2 = buildRecordConstructor();
                        clauseSequence.addMainClause(new CorrLetClause(recordConstructor1, intermediateVarExpr, null));
                        clauseSequence.addRepresentativeVertexBinding(vertexVar, recordConstructor2);
                    });
                    aliasLookupTable.putIterationAlias(vertexVar, iterationVar);
                    aliasLookupTable.putJoinAlias(vertexVar, intermediateVar);
                }
            };

        } else {
            GraphElementDeclaration elementDeclaration = elementLookupTable.getElementDecl(vertexIdentifier);
            return loweringEnvironment -> {
                // Introduce our iteration expression.
                loweringEnvironment.acceptTransformer(clauseSequence -> {
                    JoinClause joinClause = new JoinClause(JoinType.INNER, elementDeclaration.getNormalizedBody(),
                            new VariableExpr(iterationVar), null, new LiteralExpr(TrueLiteral.INSTANCE), null);
                    clauseSequence.addMainClause(joinClause);
                });

                // Bind our intermediate (join) variable and vertex variable.
                loweringEnvironment.acceptTransformer(clauseSequence -> {
                    VariableExpr iterationVarExpr = new VariableExpr(iterationVar);
                    VariableExpr intermediateVarExpr = new VariableExpr(intermediateVar);
                    clauseSequence.addMainClause(new CorrLetClause(iterationVarExpr, intermediateVarExpr, null));
                    clauseSequence.addRepresentativeVertexBinding(vertexVar, new VariableExpr(iterationVar));
                });
            };
        }
    }

    /**
     * Build an {@link IEnvironmentAction} to handle an edge that we can fold into (attach from) an already introduced
     * vertex. A folded edge is implicitly inlined. There are two possible {@link IEnvironmentAction}s generated here:
     * 1. An action for inlined, folded edges that have no projections.
     * 2. An action for inlined, folded edges that have projections.
     */
    public IEnvironmentAction buildFoldedEdgeAction(VertexPatternExpr vertexPatternExpr,
            EdgePatternExpr edgePatternExpr) throws CompilationException {
        VarIdentifier vertexVar = vertexPatternExpr.getVariableExpr().getVar();
        VarIdentifier edgeVar = edgePatternExpr.getEdgeDescriptor().getVariableExpr().getVar();
        VarIdentifier intermediateVar = graphixRewritingContext.getNewGraphixVariable();

        // We should only be working with one identifier (given that we only have one label).
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        List<GraphElementIdentifier> edgeElementIDs = edgeDescriptor.generateIdentifiers(graphIdentifier);
        if (edgeElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Found non-canonical edge pattern!");
        }
        GraphElementIdentifier edgeIdentifier = edgeElementIDs.get(0);
        ElementBodyAnalysisContext edgeAnalysisContext = analysisContextMap.get(edgeIdentifier);
        if (edgeAnalysisContext.isSelectClauseInline()) {
            return new AbstractInlineAction(graphixRewritingContext, edgeAnalysisContext, null) {
                @Override
                public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
                    // We want to bind directly to the iteration variable of our vertex, not the join variable.
                    elementVariable = aliasLookupTable.getIterationAlias(vertexVar);

                    // Inline our edge body.
                    super.apply(loweringEnvironment);

                    // Build a binding for our edge variable.
                    loweringEnvironment.acceptTransformer(clauseSequence -> {
                        VariableExpr elementVarExpr = new VariableExpr(elementVariable);
                        VariableExpr intermediateVarExpr = new VariableExpr(intermediateVar);
                        clauseSequence.addMainClause(new CorrLetClause(elementVarExpr, intermediateVarExpr, null));
                        clauseSequence.addRepresentativeEdgeBinding(edgeVar, new VariableExpr(elementVariable));
                    });
                    aliasLookupTable.putIterationAlias(edgeVar, elementVariable);
                    aliasLookupTable.putJoinAlias(edgeVar, intermediateVar);
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

                    // Build a record constructor from our context to bind to our edge and intermediate (join) var.
                    loweringEnvironment.acceptTransformer(clauseSequence -> {
                        VariableExpr intermediateVarExpr = new VariableExpr(intermediateVar);
                        RecordConstructor recordConstructor1 = buildRecordConstructor();
                        RecordConstructor recordConstructor2 = buildRecordConstructor();
                        clauseSequence.addMainClause(new CorrLetClause(recordConstructor1, intermediateVarExpr, null));
                        clauseSequence.addRepresentativeEdgeBinding(edgeVar, recordConstructor2);
                    });
                    aliasLookupTable.putIterationAlias(edgeVar, elementVariable);
                    aliasLookupTable.putJoinAlias(edgeVar, intermediateVar);
                }
            };
        }
    }

    /**
     * Build an {@link IEnvironmentAction} to handle an edge that we cannot fold into an already introduced vertex.
     * There are three possible {@link IEnvironmentAction}s generated here:
     * 1. An action for inlined edges that have no projections.
     * 2. An action for inlined edges that have projections.
     * 3. An action for non-inlined edges.
     */
    public IEnvironmentAction buildNonFoldedEdgeAction(VertexPatternExpr vertexPatternExpr,
            EdgePatternExpr edgePatternExpr, Function<GraphElementIdentifier, List<List<String>>> edgeKeyAccess)
            throws CompilationException {
        VarIdentifier edgeVar = edgePatternExpr.getEdgeDescriptor().getVariableExpr().getVar();
        VarIdentifier vertexVar = vertexPatternExpr.getVariableExpr().getVar();
        VarIdentifier iterationVar = graphixRewritingContext.getNewGraphixVariable();
        VarIdentifier intermediateVar = graphixRewritingContext.getNewGraphixVariable();

        // We should only be working with one edge identifier...
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        List<GraphElementIdentifier> edgeElementIDs = edgeDescriptor.generateIdentifiers(graphIdentifier);
        if (edgeElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Found non-canonical edge pattern!");
        }
        GraphElementIdentifier edgeIdentifier = edgeElementIDs.get(0);
        ElementBodyAnalysisContext edgeAnalysisContext = analysisContextMap.get(edgeIdentifier);
        Expression datasetCallExpression = edgeAnalysisContext.getDatasetCallExpression();

        // ...and only one vertex identifier (given that we only have one label).
        List<GraphElementIdentifier> vertexElementIDs = vertexPatternExpr.generateIdentifiers(graphIdentifier);
        if (vertexElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Found non-canonical vertex pattern!");
        }
        GraphElementIdentifier vertexIdentifier = vertexElementIDs.get(0);
        if (edgeAnalysisContext.isExpressionInline() && edgeAnalysisContext.isSelectClauseInline()) {
            return new AbstractInlineAction(graphixRewritingContext, edgeAnalysisContext, iterationVar) {
                @Override
                public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
                    // Join our edge iteration variable to our vertex variable.
                    loweringEnvironment.acceptTransformer(clauseSequence -> {
                        VariableExpr vertexJoinExpr = new VariableExpr(aliasLookupTable.getJoinAlias(vertexVar));
                        Expression vertexEdgeJoin = buildVertexEdgeJoin(
                                buildAccessorList(vertexJoinExpr, elementLookupTable.getVertexKey(vertexIdentifier)),
                                buildAccessorList(new VariableExpr(iterationVar), edgeKeyAccess.apply(edgeIdentifier)));
                        JoinClause joinClause = new JoinClause(JoinType.INNER, datasetCallExpression,
                                new VariableExpr(iterationVar), null, vertexEdgeJoin, null);
                        clauseSequence.addMainClause(joinClause);
                    });

                    // Inline our edge body.
                    super.apply(loweringEnvironment);

                    // Bind our intermediate (join) variable and edge variable.
                    loweringEnvironment.acceptTransformer(clauseSequence -> {
                        VariableExpr iterationVarExpr = new VariableExpr(iterationVar);
                        VariableExpr intermediateVarExpr = new VariableExpr(intermediateVar);
                        clauseSequence.addMainClause(new CorrLetClause(iterationVarExpr, intermediateVarExpr, null));
                        clauseSequence.addRepresentativeEdgeBinding(edgeVar, new VariableExpr(iterationVar));
                    });
                    aliasLookupTable.putIterationAlias(edgeVar, iterationVar);
                    aliasLookupTable.putJoinAlias(edgeVar, intermediateVar);
                }
            };

        } else if (edgeAnalysisContext.isExpressionInline()) {
            return new AbstractInlineAction(graphixRewritingContext, edgeAnalysisContext, iterationVar) {
                @Override
                public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
                    // Join our edge iteration variable to our vertex variable.
                    loweringEnvironment.acceptTransformer(clauseSequence -> {
                        VariableExpr vertexJoinExpr = new VariableExpr(aliasLookupTable.getJoinAlias(vertexVar));
                        Expression vertexEdgeJoin = buildVertexEdgeJoin(
                                buildAccessorList(vertexJoinExpr, elementLookupTable.getVertexKey(vertexIdentifier)),
                                buildAccessorList(new VariableExpr(iterationVar), edgeKeyAccess.apply(edgeIdentifier)));
                        JoinClause joinClause = new JoinClause(JoinType.INNER, datasetCallExpression,
                                new VariableExpr(iterationVar), null, vertexEdgeJoin, null);
                        clauseSequence.addMainClause(joinClause);
                    });

                    // Inline our edge body.
                    super.apply(loweringEnvironment);

                    // Build a record constructor from our context to bind to our edge variable.
                    loweringEnvironment.acceptTransformer(clauseSequence -> {
                        VariableExpr intermediateVarExpr = new VariableExpr(intermediateVar);
                        RecordConstructor recordConstructor1 = buildRecordConstructor();
                        RecordConstructor recordConstructor2 = buildRecordConstructor();
                        clauseSequence.addMainClause(new CorrLetClause(recordConstructor1, intermediateVarExpr, null));
                        clauseSequence.addRepresentativeEdgeBinding(edgeVar, recordConstructor2);
                    });
                    aliasLookupTable.putIterationAlias(edgeVar, iterationVar);
                    aliasLookupTable.putJoinAlias(edgeVar, intermediateVar);
                }
            };

        } else {
            GraphElementDeclaration elementDeclaration = elementLookupTable.getElementDecl(edgeIdentifier);
            return loweringEnvironment -> {
                // Join our edge body to our vertex variable.
                loweringEnvironment.acceptTransformer(clauseSequence -> {
                    VariableExpr vertexJoinExpr = new VariableExpr(aliasLookupTable.getJoinAlias(vertexVar));
                    Expression vertexEdgeJoin = buildVertexEdgeJoin(
                            buildAccessorList(vertexJoinExpr, elementLookupTable.getVertexKey(vertexIdentifier)),
                            buildAccessorList(new VariableExpr(iterationVar), edgeKeyAccess.apply(edgeIdentifier)));
                    JoinClause joinClause = new JoinClause(JoinType.INNER, elementDeclaration.getNormalizedBody(),
                            new VariableExpr(iterationVar), null, vertexEdgeJoin, null);
                    clauseSequence.addMainClause(joinClause);
                });

                // Bind our intermediate (join) variable and edge variable.
                loweringEnvironment.acceptTransformer(clauseSequence -> {
                    VariableExpr iterationVarExpr = new VariableExpr(iterationVar);
                    VariableExpr intermediateVarExpr = new VariableExpr(intermediateVar);
                    clauseSequence.addMainClause(new CorrLetClause(iterationVarExpr, intermediateVarExpr, null));
                    clauseSequence.addRepresentativeEdgeBinding(edgeVar, new VariableExpr(iterationVar));
                });
                aliasLookupTable.putIterationAlias(edgeVar, iterationVar);
                aliasLookupTable.putJoinAlias(edgeVar, intermediateVar);
            };
        }
    }

    /**
     * Build an {@link IEnvironmentAction} to introduce a WHERE-CLAUSE that will correlate a vertex and edge.
     */
    public IEnvironmentAction buildRawJoinVertexAction(VertexPatternExpr vertexPatternExpr,
            EdgePatternExpr edgePatternExpr, Function<GraphElementIdentifier, List<List<String>>> edgeKeyAccess)
            throws CompilationException {
        VarIdentifier edgeVar = edgePatternExpr.getEdgeDescriptor().getVariableExpr().getVar();
        VarIdentifier vertexVar = vertexPatternExpr.getVariableExpr().getVar();

        // We should only be working with one edge identifier...
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        List<GraphElementIdentifier> edgeElementIDs = edgeDescriptor.generateIdentifiers(graphIdentifier);
        if (edgeElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Found non-canonical edge pattern!");
        }
        GraphElementIdentifier edgeIdentifier = edgeElementIDs.get(0);

        // ...and only one vertex identifier (given that we only have one label).
        List<GraphElementIdentifier> vertexElementIDs = vertexPatternExpr.generateIdentifiers(graphIdentifier);
        if (vertexElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Found non-canonical vertex pattern!");
        }
        GraphElementIdentifier vertexIdentifier = vertexElementIDs.get(0);
        return loweringEnvironment -> {
            // No aliases need to be introduced, we just need to add a WHERE-CONJUNCT.
            VariableExpr edgeJoinExpr = new VariableExpr(aliasLookupTable.getJoinAlias(edgeVar));
            VariableExpr vertexJoinExpr = new VariableExpr(aliasLookupTable.getJoinAlias(vertexVar));
            loweringEnvironment.acceptTransformer(clauseSequence -> {
                Expression vertexEdgeJoin = buildVertexEdgeJoin(
                        buildAccessorList(vertexJoinExpr, elementLookupTable.getVertexKey(vertexIdentifier)),
                        buildAccessorList(edgeJoinExpr, edgeKeyAccess.apply(edgeIdentifier)));
                clauseSequence.addMainClause(new CorrWhereClause(vertexEdgeJoin));
            });
        };
    }

    /**
     * Build an {@link IEnvironmentAction} to handle a vertex that is bound to an existing (already introduced) edge.
     * There are three possible {@link IEnvironmentAction}s generated here:
     * 1. An action for inlined vertices that have no projections.
     * 2. An action for inlined vertices that have projections.
     * 3. An action for non-inlined vertices.
     */
    public IEnvironmentAction buildBoundVertexAction(VertexPatternExpr vertexPatternExpr,
            EdgePatternExpr edgePatternExpr, Function<GraphElementIdentifier, List<List<String>>> edgeKeyAccess)
            throws CompilationException {
        VarIdentifier edgeVar = edgePatternExpr.getEdgeDescriptor().getVariableExpr().getVar();
        VarIdentifier vertexVar = vertexPatternExpr.getVariableExpr().getVar();
        VarIdentifier iterationVar = graphixRewritingContext.getNewGraphixVariable();
        VarIdentifier intermediateVar = graphixRewritingContext.getNewGraphixVariable();

        // We should only be working with one edge identifier...
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        List<GraphElementIdentifier> edgeElementIDs = edgeDescriptor.generateIdentifiers(graphIdentifier);
        if (edgeElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Found non-canonical edge pattern!");
        }
        GraphElementIdentifier edgeIdentifier = edgeElementIDs.get(0);

        // ...and only one vertex identifier (given that we only have one label).
        List<GraphElementIdentifier> vertexElementIDs = vertexPatternExpr.generateIdentifiers(graphIdentifier);
        if (vertexElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Found non-canonical vertex pattern!");
        }
        GraphElementIdentifier vertexIdentifier = vertexElementIDs.get(0);
        ElementBodyAnalysisContext vertexAnalysisContext = analysisContextMap.get(vertexIdentifier);
        Expression datasetCallExpression = vertexAnalysisContext.getDatasetCallExpression();
        if (vertexAnalysisContext.isExpressionInline() && vertexAnalysisContext.isSelectClauseInline()) {
            return new AbstractInlineAction(graphixRewritingContext, vertexAnalysisContext, iterationVar) {
                @Override
                public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
                    // Join our vertex iteration variable to our edge variable.
                    loweringEnvironment.acceptTransformer(clauseSequence -> {
                        VariableExpr edgeJoinExpr = new VariableExpr(aliasLookupTable.getJoinAlias(edgeVar));
                        VariableExpr vertexJoinExpr = new VariableExpr(iterationVar);
                        Expression vertexEdgeJoin = buildVertexEdgeJoin(
                                buildAccessorList(vertexJoinExpr, elementLookupTable.getVertexKey(vertexIdentifier)),
                                buildAccessorList(edgeJoinExpr, edgeKeyAccess.apply(edgeIdentifier)));
                        JoinClause joinClause = new JoinClause(JoinType.INNER, datasetCallExpression,
                                new VariableExpr(iterationVar), null, vertexEdgeJoin, null);
                        clauseSequence.addMainClause(joinClause);
                    });

                    // Inline our vertex body.
                    super.apply(loweringEnvironment);

                    // Bind our intermediate (join) variable and vertex variable.
                    loweringEnvironment.acceptTransformer(clauseSequence -> {
                        VariableExpr iterationVarExpr = new VariableExpr(iterationVar);
                        VariableExpr intermediateVarExpr = new VariableExpr(intermediateVar);
                        clauseSequence.addMainClause(new CorrLetClause(iterationVarExpr, intermediateVarExpr, null));
                        clauseSequence.addRepresentativeVertexBinding(vertexVar, new VariableExpr(iterationVar));
                    });
                    aliasLookupTable.putIterationAlias(vertexVar, iterationVar);
                    aliasLookupTable.putJoinAlias(vertexVar, intermediateVar);
                }
            };

        } else if (vertexAnalysisContext.isExpressionInline()) {
            return new AbstractInlineAction(graphixRewritingContext, vertexAnalysisContext, iterationVar) {
                @Override
                public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
                    // Join our vertex iteration variable to our edge variable.
                    loweringEnvironment.acceptTransformer(clauseSequence -> {
                        VariableExpr edgeJoinExpr = new VariableExpr(aliasLookupTable.getJoinAlias(edgeVar));
                        VariableExpr vertexJoinExpr = new VariableExpr(iterationVar);
                        Expression vertexEdgeJoin = buildVertexEdgeJoin(
                                buildAccessorList(vertexJoinExpr, elementLookupTable.getVertexKey(vertexIdentifier)),
                                buildAccessorList(edgeJoinExpr, edgeKeyAccess.apply(edgeIdentifier)));
                        JoinClause joinClause = new JoinClause(JoinType.INNER, datasetCallExpression,
                                new VariableExpr(iterationVar), null, vertexEdgeJoin, null);
                        clauseSequence.addMainClause(joinClause);
                    });

                    // Inline our vertex body.
                    super.apply(loweringEnvironment);

                    // Build a record constructor from our context to bind to our vertex variable.
                    loweringEnvironment.acceptTransformer(clauseSequence -> {
                        VariableExpr intermediateVarExpr = new VariableExpr(intermediateVar);
                        RecordConstructor recordConstructor1 = buildRecordConstructor();
                        RecordConstructor recordConstructor2 = buildRecordConstructor();
                        clauseSequence.addMainClause(new CorrLetClause(recordConstructor1, intermediateVarExpr, null));
                        clauseSequence.addRepresentativeVertexBinding(vertexVar, recordConstructor2);
                    });
                    aliasLookupTable.putIterationAlias(vertexVar, iterationVar);
                    aliasLookupTable.putJoinAlias(vertexVar, intermediateVar);
                }
            };

        } else {
            GraphElementDeclaration elementDeclaration = elementLookupTable.getElementDecl(vertexIdentifier);
            return loweringEnvironment -> {
                // Join our vertex body to our edge variable.
                loweringEnvironment.acceptTransformer(clauseSequence -> {
                    VariableExpr edgeJoinExpr = new VariableExpr(aliasLookupTable.getJoinAlias(edgeVar));
                    VariableExpr vertexJoinExpr = new VariableExpr(iterationVar);
                    Expression vertexEdgeJoin = buildVertexEdgeJoin(
                            buildAccessorList(vertexJoinExpr, elementLookupTable.getVertexKey(vertexIdentifier)),
                            buildAccessorList(edgeJoinExpr, edgeKeyAccess.apply(edgeIdentifier)));
                    JoinClause joinClause = new JoinClause(JoinType.INNER, elementDeclaration.getNormalizedBody(),
                            new VariableExpr(iterationVar), null, vertexEdgeJoin, null);
                    clauseSequence.addMainClause(joinClause);
                });

                // Bind our intermediate (join) variable and vertex variable.
                loweringEnvironment.acceptTransformer(clauseSequence -> {
                    VariableExpr iterationVarExpr = new VariableExpr(iterationVar);
                    VariableExpr intermediateVarExpr = new VariableExpr(intermediateVar);
                    clauseSequence.addMainClause(new CorrLetClause(iterationVarExpr, intermediateVarExpr, null));
                    clauseSequence.addRepresentativeVertexBinding(vertexVar, new VariableExpr(iterationVar));
                });
                aliasLookupTable.putIterationAlias(vertexVar, iterationVar);
                aliasLookupTable.putJoinAlias(vertexVar, intermediateVar);
            };
        }
    }
}
