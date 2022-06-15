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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.GraphSelectBlock;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrites.common.ElementLookupTable;
import org.apache.asterix.graphix.lang.rewrites.lower.EnvironmentActionFactory;
import org.apache.asterix.graphix.lang.rewrites.lower.LoweringAliasLookupTable;
import org.apache.asterix.graphix.lang.rewrites.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrites.visitor.ElementBodyAnalysisVisitor.ElementBodyAnalysisContext;
import org.apache.asterix.graphix.lang.rewrites.visitor.StructureAnalysisVisitor.StructureContext;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * Rewrite a graph AST to utilize non-graph AST nodes (i.e. replace GRAPH-SELECT-BLOCKs with a SELECT-BLOCK).
 */
public class GraphixLoweringVisitor extends AbstractGraphixQueryVisitor {
    private final Map<FromGraphClause, StructureContext> fromGraphClauseContextMap;
    private final ElementLookupTable elementLookupTable;
    private final GraphixRewritingContext graphixRewritingContext;
    private SelectExpression topLevelSelectExpression;

    // Our stack corresponds to which GRAPH-SELECT-BLOCK we are currently working with.
    private final Map<GraphElementIdentifier, ElementBodyAnalysisContext> analysisContextMap;
    private final Deque<LoweringEnvironment> environmentStack;
    private final LoweringAliasLookupTable aliasLookupTable;
    private final EnvironmentActionFactory environmentActionFactory;

    public GraphixLoweringVisitor(GraphixRewritingContext graphixRewritingContext,
            ElementLookupTable elementLookupTable, Map<FromGraphClause, StructureContext> fromGraphClauseContextMap) {
        this.fromGraphClauseContextMap = Objects.requireNonNull(fromGraphClauseContextMap);
        this.elementLookupTable = Objects.requireNonNull(elementLookupTable);
        this.graphixRewritingContext = Objects.requireNonNull(graphixRewritingContext);
        this.aliasLookupTable = new LoweringAliasLookupTable();
        this.environmentStack = new ArrayDeque<>();

        // All actions on our environment are supplied by the factory below.
        Map<GraphElementIdentifier, ElementBodyAnalysisContext> bodyAnalysisContextMap = new HashMap<>();
        this.environmentActionFactory = new EnvironmentActionFactory(bodyAnalysisContextMap, elementLookupTable,
                aliasLookupTable, graphixRewritingContext);
        this.analysisContextMap = bodyAnalysisContextMap;
    }

    @Override
    public Expression visit(SelectExpression selectExpression, ILangExpression arg) throws CompilationException {
        if (!selectExpression.isSubquery() || topLevelSelectExpression == null) {
            topLevelSelectExpression = selectExpression;
        }
        return super.visit(selectExpression, arg);
    }

    @Override
    public Expression visit(GraphSelectBlock graphSelectBlock, ILangExpression arg) throws CompilationException {
        SelectExpression selectExpression = (SelectExpression) arg;
        if (graphSelectBlock.hasFromGraphClause()) {
            FromGraphClause fromGraphClause = graphSelectBlock.getFromGraphClause();

            // Initialize a new lowering environment.
            GraphIdentifier graphIdentifier = fromGraphClauseContextMap.get(fromGraphClause).getGraphIdentifier();
            LoweringEnvironment newEnvironment = new LoweringEnvironment(graphSelectBlock, graphixRewritingContext);
            environmentActionFactory.reset(graphIdentifier);

            // We will remove the FROM-GRAPH node and replace this with a FROM node on the child visit.
            environmentStack.addLast(newEnvironment);
            super.visit(graphSelectBlock, graphSelectBlock);
            environmentStack.removeLast();

            // See if there are Graphix functions declared anywhere in our query.
            Set<FunctionIdentifier> graphixFunctionSet = new HashSet<>();
            topLevelSelectExpression.accept(new AbstractGraphixQueryVisitor() {
                @Override
                public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
                    FunctionSignature functionSignature = callExpr.getFunctionSignature();
                    if (functionSignature.getDataverseName().equals(GraphixFunctionIdentifiers.GRAPHIX_DV)) {
                        graphixFunctionSet.add(functionSignature.createFunctionIdentifier());
                    }
                    return super.visit(callExpr, arg);
                }
            }, null);

            // If so, then we need to perform a pass for schema enrichment.
            if (!graphixFunctionSet.isEmpty()) {
                SchemaEnrichmentVisitor schemaEnrichmentVisitor = new SchemaEnrichmentVisitor(elementLookupTable,
                        graphIdentifier, graphSelectBlock, graphixFunctionSet);
                selectExpression.accept(schemaEnrichmentVisitor, null);
                if (selectExpression.hasOrderby()) {
                    selectExpression.getOrderbyClause().accept(schemaEnrichmentVisitor, null);
                }
                if (selectExpression.hasLimit()) {
                    selectExpression.getLimitClause().accept(schemaEnrichmentVisitor, null);
                }
            }

        } else {
            super.visit(graphSelectBlock, arg);
        }
        return null;
    }

    @Override
    public Expression visit(FromGraphClause fromGraphClause, ILangExpression arg) throws CompilationException {
        // Perform an analysis pass over each element body. We need to determine what we can and can't inline.
        for (GraphElementDeclaration graphElementDeclaration : elementLookupTable) {
            ElementBodyAnalysisVisitor elementBodyAnalysisVisitor = new ElementBodyAnalysisVisitor();
            GraphElementIdentifier elementIdentifier = graphElementDeclaration.getIdentifier();
            graphElementDeclaration.getNormalizedBody().accept(elementBodyAnalysisVisitor, null);
            analysisContextMap.put(elementIdentifier, elementBodyAnalysisVisitor.getElementBodyAnalysisContext());
        }
        LoweringEnvironment workingEnvironment = environmentStack.getLast();

        // Lower our MATCH-CLAUSEs. We should be working with canonical-ized patterns.
        boolean wasInitialEdgeOrderingEncountered = false;
        StructureContext structureContext = fromGraphClauseContextMap.get(fromGraphClause);
        Deque<List<VertexPatternExpr>> danglingVertexQueue = structureContext.getDanglingVertexQueue();
        Deque<List<PathPatternExpr>> pathPatternQueue = structureContext.getPathPatternQueue();
        for (Iterable<EdgePatternExpr> edgeOrdering : structureContext.getEdgeDependencyGraph()) {
            if (wasInitialEdgeOrderingEncountered) {
                workingEnvironment.beginLeftMatch();
            }
            for (EdgePatternExpr edgePatternExpr : edgeOrdering) {
                edgePatternExpr.accept(this, fromGraphClause);
            }
            for (VertexPatternExpr danglingVertexExpr : danglingVertexQueue.removeFirst()) {
                workingEnvironment.acceptAction(environmentActionFactory.buildDanglingVertexAction(danglingVertexExpr));
            }
            if (wasInitialEdgeOrderingEncountered) {
                workingEnvironment.endLeftMatch(graphixRewritingContext.getWarningCollector());
            }
            for (PathPatternExpr pathPatternExpr : pathPatternQueue.removeFirst()) {
                workingEnvironment.acceptAction(environmentActionFactory.buildPathPatternAction(pathPatternExpr));
            }
            wasInitialEdgeOrderingEncountered = true;
        }
        if (!structureContext.getEdgeDependencyGraph().iterator().hasNext()) {
            for (VertexPatternExpr danglingVertexExpr : danglingVertexQueue.removeFirst()) {
                workingEnvironment.acceptAction(environmentActionFactory.buildDanglingVertexAction(danglingVertexExpr));
            }
            for (PathPatternExpr pathPatternExpr : pathPatternQueue.removeFirst()) {
                workingEnvironment.acceptAction(environmentActionFactory.buildPathPatternAction(pathPatternExpr));
            }
        }
        workingEnvironment.acceptAction(environmentActionFactory.buildIsomorphismAction(fromGraphClause));

        // Finalize our lowering by removing this FROM-GRAPH-CLAUSE from our parent GRAPH-SELECT-BLOCK.
        workingEnvironment.finalizeLowering(fromGraphClause, graphixRewritingContext.getWarningCollector());

        // Add our correlate clauses, if any, to our tail FROM-TERM.
        if (!fromGraphClause.getCorrelateClauses().isEmpty()) {
            GraphSelectBlock graphSelectBlock = (GraphSelectBlock) arg;
            List<FromTerm> fromTerms = graphSelectBlock.getFromClause().getFromTerms();
            FromTerm tailFromTerm = fromTerms.get(fromTerms.size() - 1);
            tailFromTerm.getCorrelateClauses().addAll(fromGraphClause.getCorrelateClauses());
        }
        return null;
    }

    @Override
    public Expression visit(EdgePatternExpr edgePatternExpr, ILangExpression arg) throws CompilationException {
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        LoweringEnvironment lowerEnvironment = environmentStack.getLast();

        // We should only be working with one identifier (given that we only have one label).
        GraphIdentifier graphIdentifier = fromGraphClauseContextMap.get((FromGraphClause) arg).getGraphIdentifier();
        List<GraphElementIdentifier> edgeElementIDs = edgeDescriptor.generateIdentifiers(graphIdentifier);
        if (edgeElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Found non-canonical edge pattern!");
        }
        GraphElementIdentifier edgeIdentifier = edgeElementIDs.get(0);
        ElementBodyAnalysisContext edgeBodyAnalysisContext = analysisContextMap.get(edgeIdentifier);
        DataverseName edgeDataverseName = edgeBodyAnalysisContext.getDataverseName();
        String edgeDatasetName = edgeBodyAnalysisContext.getDatasetName();
        boolean isEdgeInline = edgeBodyAnalysisContext.isExpressionInline();

        // Determine our source and destination vertices.
        VertexPatternExpr sourceVertex, destVertex;
        if (edgeDescriptor.getEdgeDirection() == EdgeDescriptor.EdgeDirection.LEFT_TO_RIGHT) {
            sourceVertex = edgePatternExpr.getLeftVertex();
            destVertex = edgePatternExpr.getRightVertex();

        } else { // edgeDescriptor.getEdgeDirection() == EdgeDescriptor.EdgeDirection.RIGHT_TO_LEFT
            sourceVertex = edgePatternExpr.getRightVertex();
            destVertex = edgePatternExpr.getLeftVertex();
        }

        // Collect information about our source -> edge JOIN.
        GraphElementIdentifier sourceIdentifier = sourceVertex.generateIdentifiers(graphIdentifier).get(0);
        ElementBodyAnalysisContext sourceBodyAnalysisContext = analysisContextMap.get(sourceIdentifier);
        VarIdentifier sourceVertexVariable = sourceVertex.getVariableExpr().getVar();
        List<List<String>> sourceVertexKey = elementLookupTable.getVertexKey(sourceIdentifier);
        List<List<String>> sourceEdgeKey = elementLookupTable.getEdgeSourceKey(edgeIdentifier);
        Function<GraphElementIdentifier, List<List<String>>> sourceKey = elementLookupTable::getEdgeSourceKey;
        boolean isSourceInline = sourceBodyAnalysisContext.isExpressionInline();
        boolean isSourceIntroduced = aliasLookupTable.getIterationAlias(sourceVertexVariable) != null;
        boolean isSourceFolded = isSourceInline && sourceBodyAnalysisContext.getDatasetName().equals(edgeDatasetName)
                && sourceBodyAnalysisContext.getDataverseName().equals(edgeDataverseName)
                && sourceVertexKey.equals(sourceEdgeKey);

        // ...and our dest -> edge JOIN.
        GraphElementIdentifier destIdentifier = destVertex.generateIdentifiers(graphIdentifier).get(0);
        ElementBodyAnalysisContext destBodyAnalysisContext = analysisContextMap.get(destIdentifier);
        VarIdentifier destVertexVariable = destVertex.getVariableExpr().getVar();
        List<List<String>> destVertexKey = elementLookupTable.getVertexKey(destIdentifier);
        List<List<String>> destEdgeKey = elementLookupTable.getEdgeDestKey(edgeIdentifier);
        Function<GraphElementIdentifier, List<List<String>>> destKey = elementLookupTable::getEdgeDestKey;
        boolean isDestInline = destBodyAnalysisContext.isExpressionInline();
        boolean isDestIntroduced = aliasLookupTable.getIterationAlias(destVertexVariable) != null;
        boolean isDestFolded = isDestInline && destBodyAnalysisContext.getDatasetName().equals(edgeDatasetName)
                && destBodyAnalysisContext.getDataverseName().equals(edgeDataverseName)
                && destVertexKey.equals(destEdgeKey);

        // Determine our strategy for lowering our edge.
        if (isEdgeInline && isSourceFolded && !isDestIntroduced) {
            if (!isSourceIntroduced) {
                lowerEnvironment.acceptAction(environmentActionFactory.buildDanglingVertexAction(sourceVertex));
            }
            lowerEnvironment
                    .acceptAction(environmentActionFactory.buildFoldedEdgeAction(sourceVertex, edgePatternExpr));
            lowerEnvironment.acceptAction(
                    environmentActionFactory.buildBoundVertexAction(destVertex, edgePatternExpr, destKey));

        } else if (isEdgeInline && isDestFolded && !isSourceIntroduced) {
            if (!isDestIntroduced) {
                lowerEnvironment.acceptAction(environmentActionFactory.buildDanglingVertexAction(destVertex));
            }
            lowerEnvironment.acceptAction(environmentActionFactory.buildFoldedEdgeAction(destVertex, edgePatternExpr));
            lowerEnvironment.acceptAction(
                    environmentActionFactory.buildBoundVertexAction(sourceVertex, edgePatternExpr, sourceKey));

        } else if (isSourceIntroduced && isDestIntroduced) {
            lowerEnvironment.acceptAction(
                    environmentActionFactory.buildNonFoldedEdgeAction(sourceVertex, edgePatternExpr, sourceKey));
            lowerEnvironment.acceptAction(
                    environmentActionFactory.buildRawJoinVertexAction(destVertex, edgePatternExpr, destKey));

        } else if (isSourceIntroduced) { // !isDestIntroduced
            lowerEnvironment.acceptAction(
                    environmentActionFactory.buildNonFoldedEdgeAction(sourceVertex, edgePatternExpr, sourceKey));
            lowerEnvironment.acceptAction(
                    environmentActionFactory.buildBoundVertexAction(destVertex, edgePatternExpr, destKey));

        } else if (isDestIntroduced) { // !isSourceIntroduced
            lowerEnvironment.acceptAction(
                    environmentActionFactory.buildNonFoldedEdgeAction(destVertex, edgePatternExpr, destKey));
            lowerEnvironment.acceptAction(
                    environmentActionFactory.buildBoundVertexAction(sourceVertex, edgePatternExpr, sourceKey));

        } else { // !isSourceIntroduced && !isDestIntroduced
            // When nothing is introduced, start off from LEFT to RIGHT instead of considering our source and dest.
            VertexPatternExpr leftVertex = edgePatternExpr.getLeftVertex();
            VertexPatternExpr rightVertex = edgePatternExpr.getRightVertex();
            Function<GraphElementIdentifier, List<List<String>>> leftKey;
            Function<GraphElementIdentifier, List<List<String>>> rightKey;
            if (edgeDescriptor.getEdgeDirection() == EdgeDescriptor.EdgeDirection.LEFT_TO_RIGHT) {
                leftKey = sourceKey;
                rightKey = destKey;

            } else { // edgeDescriptor.getEdgeDirection() == EdgeDescriptor.EdgeDirection.RIGHT_TO_LEFT
                leftKey = destKey;
                rightKey = sourceKey;
            }
            lowerEnvironment.acceptAction(environmentActionFactory.buildDanglingVertexAction(leftVertex));
            lowerEnvironment.acceptAction(
                    environmentActionFactory.buildNonFoldedEdgeAction(leftVertex, edgePatternExpr, leftKey));
            lowerEnvironment.acceptAction(
                    environmentActionFactory.buildBoundVertexAction(rightVertex, edgePatternExpr, rightKey));
        }
        return edgePatternExpr;
    }
}