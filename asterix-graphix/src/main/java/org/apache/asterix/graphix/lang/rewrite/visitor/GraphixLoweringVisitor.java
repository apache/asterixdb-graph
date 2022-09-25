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

import static org.apache.asterix.graphix.function.GraphixFunctionIdentifiers.isEdgeFunction;
import static org.apache.asterix.graphix.function.GraphixFunctionIdentifiers.isVertexFunction;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.algebra.compiler.option.IGraphixCompilerOption;
import org.apache.asterix.graphix.algebra.compiler.option.SchemaDecorateEdgeOption;
import org.apache.asterix.graphix.algebra.compiler.option.SchemaDecorateVertexOption;
import org.apache.asterix.graphix.common.metadata.EdgeIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.common.metadata.IElementIdentifier;
import org.apache.asterix.graphix.common.metadata.VertexIdentifier;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.graphix.lang.annotation.LoweringExemptAnnotation;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.LowerListClause;
import org.apache.asterix.graphix.lang.clause.LowerSwitchClause.ClauseInputEnvironment;
import org.apache.asterix.graphix.lang.clause.LowerSwitchClause.ClauseOutputEnvironment;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.optype.MatchType;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.common.BranchLookupTable;
import org.apache.asterix.graphix.lang.rewrite.common.ElementLookupTable;
import org.apache.asterix.graphix.lang.rewrite.lower.AliasLookupTable;
import org.apache.asterix.graphix.lang.rewrite.lower.EnvironmentActionFactory;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseCollection;
import org.apache.asterix.graphix.lang.rewrite.visitor.ElementBodyAnalysisVisitor.ElementBodyAnalysisContext;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Rewrite a graph AST to utilize non-graph AST nodes (i.e. replace FROM-GRAPH-CLAUSEs with a LOWER-{LIST|BFS}-CLAUSE).
 */
public class GraphixLoweringVisitor extends AbstractGraphixQueryVisitor {
    private final GraphixDeepCopyVisitor graphixDeepCopyVisitor;
    private final VariableRemapCloneVisitor variableRemapCloneVisitor;
    private final ElementLookupTable elementLookupTable;
    private final GraphixRewritingContext graphixRewritingContext;
    private SelectExpression topLevelSelectExpression;

    // Our stack corresponds to which GRAPH-SELECT-BLOCK we are currently working with.
    private final Map<IElementIdentifier, ElementBodyAnalysisContext> analysisContextMap;
    private final Deque<LoweringEnvironment> environmentStack;
    private final AliasLookupTable aliasLookupTable;
    private final BranchLookupTable branchLookupTable;
    private final EnvironmentActionFactory actionFactory;

    public GraphixLoweringVisitor(GraphixRewritingContext graphixRewritingContext,
            ElementLookupTable elementLookupTable, BranchLookupTable branchLookupTable) {
        this.branchLookupTable = Objects.requireNonNull(branchLookupTable);
        this.elementLookupTable = Objects.requireNonNull(elementLookupTable);
        this.graphixRewritingContext = Objects.requireNonNull(graphixRewritingContext);
        this.variableRemapCloneVisitor = new VariableRemapCloneVisitor(graphixRewritingContext);
        this.graphixDeepCopyVisitor = new GraphixDeepCopyVisitor();
        this.aliasLookupTable = new AliasLookupTable();
        this.environmentStack = new ArrayDeque<>();
        this.analysisContextMap = new HashMap<>();

        // All actions on our environment are supplied by the factory below.
        this.actionFactory = new EnvironmentActionFactory(analysisContextMap, elementLookupTable, aliasLookupTable,
                graphixRewritingContext);
    }

    @Override
    public Expression visit(Query query, ILangExpression arg) throws CompilationException {
        boolean isTopLevelQuery = query.isTopLevel();
        boolean isSelectExpr = query.getBody().getKind() == Expression.Kind.SELECT_EXPRESSION;
        if (isSelectExpr && (isTopLevelQuery || topLevelSelectExpression == null)) {
            topLevelSelectExpression = (SelectExpression) query.getBody();
        }
        return super.visit(query, arg);
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        SelectExpression selectExpression = (SelectExpression) arg;
        if (selectBlock.hasFromClause() && selectBlock.getFromClause() instanceof FromGraphClause) {
            FromGraphClause fromGraphClause = (FromGraphClause) selectBlock.getFromClause();
            MetadataProvider metadataProvider = graphixRewritingContext.getMetadataProvider();
            GraphIdentifier graphIdentifier = fromGraphClause.getGraphIdentifier(metadataProvider);
            SourceLocation sourceLocation = fromGraphClause.getSourceLocation();

            // Initialize a new lowering environment.
            LoweringEnvironment newEnvironment =
                    new LoweringEnvironment(graphixRewritingContext, graphIdentifier, sourceLocation);
            actionFactory.reset(graphIdentifier);

            // We will remove the FROM-GRAPH node and replace this with a FROM node on the child visit.
            environmentStack.addLast(newEnvironment);
            super.visit(selectBlock, arg);
            environmentStack.removeLast();

            // See if we need to perform a pass for schema enrichment. By default, we decorate "as-needed".
            String schemaDecorateVertexKey = SchemaDecorateVertexOption.OPTION_KEY_NAME;
            String schemaDecorateEdgeKey = SchemaDecorateEdgeOption.OPTION_KEY_NAME;
            IGraphixCompilerOption vertexModeOption = graphixRewritingContext.getSetting(schemaDecorateVertexKey);
            IGraphixCompilerOption edgeModeOption = graphixRewritingContext.getSetting(schemaDecorateEdgeKey);
            SchemaDecorateVertexOption vertexMode = (SchemaDecorateVertexOption) vertexModeOption;
            SchemaDecorateEdgeOption edgeMode = (SchemaDecorateEdgeOption) edgeModeOption;

            // See if there are any Graphix functions used in our query.
            Set<FunctionIdentifier> graphixFunctionSet = new LinkedHashSet<>();
            topLevelSelectExpression.accept(new AbstractGraphixQueryVisitor() {
                @Override
                public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
                    FunctionSignature functionSignature = callExpr.getFunctionSignature();
                    if (functionSignature.getDataverseName().equals(GraphixFunctionIdentifiers.GRAPHIX_DV)) {
                        FunctionIdentifier functionID = functionSignature.createFunctionIdentifier();
                        if ((vertexMode == SchemaDecorateVertexOption.NEVER && isVertexFunction(functionID))
                                || (edgeMode == SchemaDecorateEdgeOption.NEVER && isEdgeFunction(functionID))) {
                            throw new CompilationException(ErrorCode.COMPILATION_ERROR, callExpr.getSourceLocation(),
                                    "Schema-decorate mode has been set to 'NEVER', but schema-decoration is required "
                                            + "to realize the function" + functionSignature + "!");
                        }
                        graphixFunctionSet.add(functionID);
                    }
                    return super.visit(callExpr, arg);
                }
            }, null);

            // Perform a pass for schema enrichment, if needed.
            boolean isVertexModeAlways = vertexMode == SchemaDecorateVertexOption.ALWAYS;
            boolean isEdgeModeAlways = edgeMode == SchemaDecorateEdgeOption.ALWAYS;
            if (!graphixFunctionSet.isEmpty() || isVertexModeAlways || isEdgeModeAlways) {
                SchemaEnrichmentVisitor schemaEnrichmentVisitor = new SchemaEnrichmentVisitor(vertexMode, edgeMode,
                        elementLookupTable, branchLookupTable, graphIdentifier, selectBlock, graphixFunctionSet);
                selectExpression.accept(schemaEnrichmentVisitor, null);
                if (selectExpression.hasOrderby()) {
                    selectExpression.getOrderbyClause().accept(schemaEnrichmentVisitor, null);
                }
                if (selectExpression.hasLimit()) {
                    selectExpression.getLimitClause().accept(schemaEnrichmentVisitor, null);
                }
            }

        } else {
            super.visit(selectBlock, arg);
        }
        return null;
    }

    @Override
    public Expression visit(FromGraphClause fromGraphClause, ILangExpression arg) throws CompilationException {
        // Perform an analysis pass over each element body. We need to determine what we can and can't inline.
        for (GraphElementDeclaration graphElementDeclaration : elementLookupTable) {
            ElementBodyAnalysisVisitor elementBodyAnalysisVisitor = new ElementBodyAnalysisVisitor();
            IElementIdentifier elementIdentifier = graphElementDeclaration.getIdentifier();
            graphElementDeclaration.getNormalizedBody().accept(elementBodyAnalysisVisitor, null);
            analysisContextMap.put(elementIdentifier, elementBodyAnalysisVisitor.getElementBodyAnalysisContext());
        }
        LoweringEnvironment workingEnvironment = environmentStack.getLast();

        // TODO (GLENN): Perform smarter analysis to determine when a vertex / edge is inlineable w/ LEFT-MATCH.
        Stream<MatchClause> matchClauseStream = fromGraphClause.getMatchClauses().stream();
        workingEnvironment.setInlineLegal(matchClauseStream.noneMatch(c -> c.getMatchType() == MatchType.LEFTOUTER));

        // Lower our MATCH-CLAUSEs. We should be working with canonical-ized patterns.
        for (MatchClause matchClause : fromGraphClause.getMatchClauses()) {
            if (matchClause.getMatchType() == MatchType.LEFTOUTER) {
                workingEnvironment.beginLeftMatch();
            }
            for (PathPatternExpr pathPatternExpr : matchClause.getPathExpressions()) {
                for (EdgePatternExpr edgePatternExpr : pathPatternExpr.getEdgeExpressions()) {
                    edgePatternExpr.accept(this, fromGraphClause);
                }
                for (VertexPatternExpr vertexPatternExpr : pathPatternExpr.getVertexExpressions()) {
                    VariableExpr vertexVariableExpr = vertexPatternExpr.getVariableExpr();
                    if (aliasLookupTable.getIterationAlias(vertexVariableExpr) != null) {
                        continue;
                    }
                    workingEnvironment.acceptAction(actionFactory.buildDanglingVertexAction(vertexPatternExpr));
                }
                workingEnvironment.acceptAction(actionFactory.buildPathPatternAction(pathPatternExpr));
            }
            if (matchClause.getMatchType() == MatchType.LEFTOUTER) {
                workingEnvironment.endLeftMatch();
            }
        }
        workingEnvironment.acceptAction(actionFactory.buildMatchSemanticAction(fromGraphClause));

        // Finalize our lowering by moving our lower list to our environment.
        workingEnvironment.endLowering(fromGraphClause);

        // Add our correlate clauses, if any, to our tail FROM-TERM.
        if (!fromGraphClause.getCorrelateClauses().isEmpty()) {
            LowerListClause lowerClause = (LowerListClause) fromGraphClause.getLowerClause();
            ClauseCollection clauseCollection = lowerClause.getClauseCollection();
            fromGraphClause.getCorrelateClauses().forEach(clauseCollection::addUserDefinedCorrelateClause);
        }
        return null;
    }

    @Override
    public Expression visit(EdgePatternExpr edgePatternExpr, ILangExpression arg) throws CompilationException {
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        if (edgeDescriptor.getPatternType() == EdgeDescriptor.PatternType.EDGE) {
            lowerCanonicalExpandedEdge(edgePatternExpr, environmentStack.getLast());

        } else { // edgeDescriptor.getPatternType() == EdgeDescriptor.PatternType.PATH
            lowerCanonicalExpandedPath(edgePatternExpr, environmentStack.getLast());
        }
        return edgePatternExpr;
    }

    private void lowerCanonicalExpandedEdge(EdgePatternExpr edgePatternExpr, LoweringEnvironment environment)
            throws CompilationException {
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();

        // We should be working with a canonical edge.
        GraphIdentifier graphIdentifier = environment.getGraphIdentifier();
        List<EdgeIdentifier> edgeElementIDs = edgePatternExpr.generateIdentifiers(graphIdentifier);
        if (edgeElementIDs.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                    "Encountered non-fixed-point edge pattern!");
        }
        EdgeIdentifier edgeIdentifier = edgeElementIDs.get(0);
        ElementBodyAnalysisContext edgeBodyAnalysisContext = analysisContextMap.get(edgeIdentifier);
        DataverseName edgeDataverseName = edgeBodyAnalysisContext.getDataverseName();
        String edgeDatasetName = edgeBodyAnalysisContext.getDatasetName();

        // Determine our source and destination vertices.
        VertexPatternExpr sourceVertex, destVertex;
        if (edgeDescriptor.getEdgeDirection() == EdgeDescriptor.EdgeDirection.LEFT_TO_RIGHT) {
            sourceVertex = edgePatternExpr.getLeftVertex();
            destVertex = edgePatternExpr.getRightVertex();

        } else { // edgeDescriptor.getEdgeDirection() == EdgeDescriptor.EdgeDirection.RIGHT_TO_LEFT
            sourceVertex = edgePatternExpr.getRightVertex();
            destVertex = edgePatternExpr.getLeftVertex();
        }

        // Collect information about our source vertex.
        VertexIdentifier sourceIdentifier = sourceVertex.generateIdentifiers(graphIdentifier).get(0);
        ElementBodyAnalysisContext sourceBodyAnalysisContext = analysisContextMap.get(sourceIdentifier);
        VariableExpr sourceVertexVariable = sourceVertex.getVariableExpr();
        List<List<String>> sourceVertexKey = elementLookupTable.getVertexKey(sourceIdentifier);
        Function<EdgeIdentifier, List<List<String>>> sourceKey = elementLookupTable::getEdgeSourceKey;
        boolean isSourceInline = sourceBodyAnalysisContext.isExpressionInline() && environment.isInlineLegal();
        boolean isSourceIntroduced = aliasLookupTable.getIterationAlias(sourceVertexVariable) != null;

        // ...and our destination vertex...
        VertexIdentifier destIdentifier = destVertex.generateIdentifiers(graphIdentifier).get(0);
        ElementBodyAnalysisContext destBodyAnalysisContext = analysisContextMap.get(destIdentifier);
        VariableExpr destVertexVariable = destVertex.getVariableExpr();
        List<List<String>> destVertexKey = elementLookupTable.getVertexKey(destIdentifier);
        Function<EdgeIdentifier, List<List<String>>> destKey = elementLookupTable::getEdgeDestKey;
        boolean isDestInline = destBodyAnalysisContext.isExpressionInline() && environment.isInlineLegal();
        boolean isDestIntroduced = aliasLookupTable.getIterationAlias(destVertexVariable) != null;

        // ...and our edge.
        List<List<String>> sourceEdgeKey = elementLookupTable.getEdgeSourceKey(edgeIdentifier);
        List<List<String>> destEdgeKey = elementLookupTable.getEdgeDestKey(edgeIdentifier);
        String sourceBodyDatasetName = sourceBodyAnalysisContext.getDatasetName();
        String destBodyDatasetName = destBodyAnalysisContext.getDatasetName();
        boolean isEdgeInline = edgeBodyAnalysisContext.isExpressionInline() && environment.isInlineLegal();
        boolean isSourceFolded = isSourceInline && sourceBodyDatasetName.equals(edgeDatasetName)
                && sourceBodyAnalysisContext.getDataverseName().equals(edgeDataverseName)
                && sourceVertexKey.equals(sourceEdgeKey);
        boolean isDestFolded = isDestInline && destBodyDatasetName.equals(edgeDatasetName)
                && destBodyAnalysisContext.getDataverseName().equals(edgeDataverseName)
                && destVertexKey.equals(destEdgeKey);

        // Condition our strategy on which vertices are currently introduced.
        if (isEdgeInline && isSourceFolded) {
            if (!isSourceIntroduced) {
                environment.acceptAction(actionFactory.buildDanglingVertexAction(sourceVertex));
            }
            environment.acceptAction(actionFactory.buildFoldedEdgeAction(sourceVertex, edgePatternExpr));
            environment.acceptAction(
                    !isDestIntroduced ? actionFactory.buildBoundVertexAction(destVertex, edgePatternExpr, destKey)
                            : actionFactory.buildRawJoinVertexAction(destVertex, edgePatternExpr, destKey));

        } else if (isEdgeInline && isDestFolded) {
            if (!isDestIntroduced) {
                environment.acceptAction(actionFactory.buildDanglingVertexAction(destVertex));
            }
            environment.acceptAction(actionFactory.buildFoldedEdgeAction(destVertex, edgePatternExpr));
            environment.acceptAction(
                    !isSourceIntroduced ? actionFactory.buildBoundVertexAction(sourceVertex, edgePatternExpr, sourceKey)
                            : actionFactory.buildRawJoinVertexAction(sourceVertex, edgePatternExpr, sourceKey));

        } else if (isSourceIntroduced && isDestIntroduced) {
            environment.acceptAction(actionFactory.buildNonFoldedEdgeAction(sourceVertex, edgePatternExpr, sourceKey));
            environment.acceptAction(actionFactory.buildRawJoinVertexAction(destVertex, edgePatternExpr, destKey));

        } else if (isSourceIntroduced) { // !isDestIntroduced
            environment.acceptAction(actionFactory.buildNonFoldedEdgeAction(sourceVertex, edgePatternExpr, sourceKey));
            environment.acceptAction(actionFactory.buildBoundVertexAction(destVertex, edgePatternExpr, destKey));

        } else if (isDestIntroduced) { // !isSourceIntroduced
            environment.acceptAction(actionFactory.buildNonFoldedEdgeAction(destVertex, edgePatternExpr, destKey));
            environment.acceptAction(actionFactory.buildBoundVertexAction(sourceVertex, edgePatternExpr, sourceKey));

        } else { // !isSourceIntroduced && !isDestIntroduced
            // When nothing is introduced, start off from LEFT to RIGHT instead of considering our source and dest.
            VertexPatternExpr leftVertex = edgePatternExpr.getLeftVertex();
            VertexPatternExpr rightVertex = edgePatternExpr.getRightVertex();
            Function<EdgeIdentifier, List<List<String>>> leftKey;
            Function<EdgeIdentifier, List<List<String>>> rightKey;
            if (edgeDescriptor.getEdgeDirection() == EdgeDescriptor.EdgeDirection.LEFT_TO_RIGHT) {
                leftKey = sourceKey;
                rightKey = destKey;

            } else { // edgeDescriptor.getEdgeDirection() == EdgeDescriptor.EdgeDirection.RIGHT_TO_LEFT
                leftKey = destKey;
                rightKey = sourceKey;
            }
            environment.acceptAction(actionFactory.buildDanglingVertexAction(leftVertex));
            environment.acceptAction(actionFactory.buildNonFoldedEdgeAction(leftVertex, edgePatternExpr, leftKey));
            environment.acceptAction(actionFactory.buildBoundVertexAction(rightVertex, edgePatternExpr, rightKey));
        }
    }

    private void lowerCanonicalExpandedPath(EdgePatternExpr edgePatternExpr, LoweringEnvironment environment)
            throws CompilationException {
        // Determine the starting vertex of our path.
        VariableExpr leftVertexVariable = edgePatternExpr.getLeftVertex().getVariableExpr();
        VariableExpr rightVertexVariable = edgePatternExpr.getRightVertex().getVariableExpr();
        boolean isLeftVertexIntroduced = aliasLookupTable.getIterationAlias(leftVertexVariable) != null;
        boolean isRightVertexIntroduced = aliasLookupTable.getIterationAlias(rightVertexVariable) != null;
        boolean isJoiningLeftToRight = isLeftVertexIntroduced || !isRightVertexIntroduced;
        VertexPatternExpr inputVertex, outputVertex;
        if (isJoiningLeftToRight) {
            inputVertex = edgePatternExpr.getLeftVertex();
            outputVertex = edgePatternExpr.getRightVertex();

        } else {
            inputVertex = edgePatternExpr.getRightVertex();
            outputVertex = edgePatternExpr.getLeftVertex();
        }
        VariableExpr inputVertexVariable = inputVertex.getVariableExpr();
        VariableExpr outputVertexVariable = outputVertex.getVariableExpr();

        // If we need to, introduce our left vertex (only occurs if nothing has been introduced).
        if (!isLeftVertexIntroduced && !isRightVertexIntroduced) {
            environment.acceptAction(actionFactory.buildDanglingVertexAction(edgePatternExpr.getLeftVertex()));
        }

        // Our input vertex must be introduced eagerly to be given to our graph clause as input.
        VariableExpr inputClauseVariable = graphixRewritingContext.getGraphixVariableCopy(inputVertexVariable);
        environment.acceptTransformer(lowerList -> {
            // Find the representative vertex associated with our input.
            Expression representativeVertexExpr = null;
            for (LetClause vertexBinding : lowerList.getRepresentativeVertexBindings()) {
                if (vertexBinding.getVarExpr().equals(inputVertex.getVariableExpr())) {
                    representativeVertexExpr = vertexBinding.getBindingExpr();
                    break;
                }
            }
            if (representativeVertexExpr == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Input vertex not found!");
            }

            // Once all variables of our representative LET-CLAUSE have been introduced, introduce our input binding.
            Set<VariableExpr> usedVariables = SqlppVariableUtil.getFreeVariables(representativeVertexExpr);
            ListIterator<AbstractClause> nonRepresentativeClauseIterator =
                    lowerList.getNonRepresentativeClauses().listIterator();
            while (nonRepresentativeClauseIterator.hasNext()) {
                AbstractClause workingClause = nonRepresentativeClauseIterator.next();
                List<VariableExpr> bindingVariables = SqlppVariableUtil.getBindingVariables(workingClause);
                bindingVariables.forEach(usedVariables::remove);
                if (usedVariables.isEmpty()) {
                    VariableExpr inputClauseVariableCopy = graphixDeepCopyVisitor.visit(inputClauseVariable, null);
                    LetClause clauseInputBinding = new LetClause(inputClauseVariableCopy, representativeVertexExpr);
                    nonRepresentativeClauseIterator.add(clauseInputBinding);
                    lowerList.addEagerVertexBinding(inputVertex.getVariableExpr(), clauseInputBinding);
                    break;
                }
            }
        });

        // Lower each of our branches.
        environment.beginBranches();
        Map<ElementLabel, VariableExpr> labelInputVariableMap = new HashMap<>();
        Map<ElementLabel, VariableExpr> labelOutputVariableMap = new HashMap<>();
        for (EdgePatternExpr branch : branchLookupTable.getBranches(edgePatternExpr)) {
            VertexPatternExpr inputBranchVertex, outputBranchVertex;
            if (isJoiningLeftToRight) {
                inputBranchVertex = branch.getLeftVertex();
                outputBranchVertex = branch.getRightVertex();

            } else {
                inputBranchVertex = branch.getRightVertex();
                outputBranchVertex = branch.getLeftVertex();
            }

            // Introduce the alias-- but do not lower our source vertex.
            environment.beginTempLowerList();
            environment.acceptAction(actionFactory.buildDanglingVertexAction(inputBranchVertex));
            environment.endTempLowerList();
            synchronizeVariables(environment, inputBranchVertex, labelInputVariableMap);
            inputBranchVertex.addHint(LoweringExemptAnnotation.INSTANCE);

            // Lower our branch, having introduced our source vertex.
            lowerCanonicalExpandedEdge(branch, environment);
            synchronizeVariables(environment, outputBranchVertex, labelOutputVariableMap);
            environment.flushBranch(branch, isJoiningLeftToRight);
        }

        // Our output vertex will be aliased by the following:
        VariableExpr outputVertexIterationAlias = graphixRewritingContext.getGraphixVariableCopy(outputVertexVariable);
        VariableExpr outputVertexJoinAlias = graphixRewritingContext.getGraphixVariableCopy(outputVertexVariable);
        aliasLookupTable.addIterationAlias(outputVertexVariable, outputVertexIterationAlias);
        aliasLookupTable.addJoinAlias(outputVertexVariable, outputVertexJoinAlias);

        // Introduce a SWITCH node, finalizing our path lowering.
        VariableExpr pathVariable = edgePatternExpr.getEdgeDescriptor().getVariableExpr();
        VariableExpr outputClauseVariable = graphixRewritingContext.getGraphixVariableCopy(outputVertexVariable);
        ClauseOutputEnvironment outputEnvironment = new ClauseOutputEnvironment(outputClauseVariable,
                aliasLookupTable.getIterationAlias(outputVertexVariable),
                aliasLookupTable.getJoinAlias(outputVertexVariable), pathVariable,
                outputVertex.getLabels().iterator().next());
        ClauseInputEnvironment inputEnvironment = new ClauseInputEnvironment(
                graphixDeepCopyVisitor.visit(inputClauseVariable, null), inputVertex.getLabels().iterator().next());
        environment.endBranches(outputEnvironment, inputEnvironment, labelInputVariableMap, labelOutputVariableMap,
                aliasLookupTable, edgePatternExpr.getSourceLocation());

        // Expose our output vertex and path to the remainder of our query.
        environment.acceptTransformer(lowerList -> {
            VariableExpr iterationVariableCopy1 = graphixDeepCopyVisitor.visit(outputVertexIterationAlias, null);
            VariableExpr iterationVariableCopy2 = graphixDeepCopyVisitor.visit(outputVertexIterationAlias, null);
            VariableExpr joinVariableCopy = graphixDeepCopyVisitor.visit(outputVertexJoinAlias, null);
            Expression iterationVariableAccess = outputEnvironment.buildIterationVariableAccess();
            Expression joinVariableAccess = outputEnvironment.buildJoinVariableAccess();
            lowerList.addNonRepresentativeClause(new LetClause(iterationVariableCopy1, iterationVariableAccess));
            lowerList.addNonRepresentativeClause(new LetClause(joinVariableCopy, joinVariableAccess));
            lowerList.addVertexBinding(outputVertexVariable, iterationVariableCopy2);
            lowerList.addPathBinding(pathVariable, outputEnvironment.buildPathVariableAccess());
        });
    }

    private void synchronizeVariables(LoweringEnvironment workingEnvironment, VertexPatternExpr branchVertex,
            Map<ElementLabel, VariableExpr> labelVariableMap) throws CompilationException {
        VariableExpr vertexVariable = branchVertex.getVariableExpr();
        VariableExpr iterationAlias = aliasLookupTable.getIterationAlias(vertexVariable);
        VariableExpr joinAlias = aliasLookupTable.getJoinAlias(vertexVariable);
        ElementLabel elementLabel = branchVertex.getLabels().iterator().next();
        if (labelVariableMap.containsKey(elementLabel)) {
            VariableExpr targetVertexVariable = labelVariableMap.get(elementLabel);
            VariableExpr targetIterationAlias = aliasLookupTable.getIterationAlias(targetVertexVariable);
            VariableExpr targetJoinAlias = aliasLookupTable.getJoinAlias(targetVertexVariable);
            variableRemapCloneVisitor.resetSubstitutions();
            variableRemapCloneVisitor.addSubstitution(iterationAlias, targetIterationAlias);
            variableRemapCloneVisitor.addSubstitution(joinAlias, targetJoinAlias);
            variableRemapCloneVisitor.addSubstitution(vertexVariable, targetVertexVariable);

            workingEnvironment.acceptTransformer(lowerList -> {
                // Transform our non-representative clauses.
                ListIterator<AbstractClause> nonRepresentativeIterator =
                        lowerList.getNonRepresentativeClauses().listIterator();
                while (nonRepresentativeIterator.hasNext()) {
                    AbstractClause workingClause = nonRepresentativeIterator.next();
                    nonRepresentativeIterator.set((AbstractClause) variableRemapCloneVisitor.substitute(workingClause));
                }

                // Transform our vertex bindings.
                ListIterator<LetClause> vertexBindingIterator =
                        lowerList.getRepresentativeVertexBindings().listIterator();
                while (vertexBindingIterator.hasNext()) {
                    LetClause vertexBinding = vertexBindingIterator.next();
                    vertexBindingIterator.set((LetClause) variableRemapCloneVisitor.substitute(vertexBinding));
                }

                // Transform our edge bindings.
                ListIterator<LetClause> edgeBindingIterator = lowerList.getRepresentativeEdgeBindings().listIterator();
                while (edgeBindingIterator.hasNext()) {
                    LetClause edgeBinding = edgeBindingIterator.next();
                    edgeBindingIterator.set((LetClause) variableRemapCloneVisitor.substitute(edgeBinding));
                }
            });
            branchVertex.setVariableExpr(targetVertexVariable);

        } else {
            labelVariableMap.put(elementLabel, vertexVariable);
        }
    }
}
