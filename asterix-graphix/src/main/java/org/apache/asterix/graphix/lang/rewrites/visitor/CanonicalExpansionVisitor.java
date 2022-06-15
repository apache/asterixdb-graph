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

import static org.apache.asterix.graphix.extension.GraphixMetadataExtension.getGraph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.GraphSelectBlock;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrites.canonical.DanglingVertexExpander;
import org.apache.asterix.graphix.lang.rewrites.canonical.EdgeSubPathExpander;
import org.apache.asterix.graphix.lang.rewrites.resolve.SchemaKnowledgeTable;
import org.apache.asterix.graphix.lang.statement.DeclareGraphStatement;
import org.apache.asterix.graphix.metadata.entity.schema.Graph;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.SetOpType;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

/**
 * Expand a single {@link SelectSetOperation} with {@link VertexPatternExpr} and {@link EdgePatternExpr} nodes into
 * several UNION-ALL branches of "canonical form". This preprocessing step allows for simpler lowering logic (and
 * therefore, potentially more efficient plans) at the cost of a wider AST.
 *
 * @see DanglingVertexExpander
 * @see EdgeSubPathExpander
 */
public class CanonicalExpansionVisitor extends AbstractGraphixQueryVisitor {
    private final GraphixRewritingContext graphixRewritingContext;
    private final Deque<CanonicalPatternEnvironment> environmentStack;
    private final DanglingVertexExpander danglingVertexExpander;
    private final EdgeSubPathExpander edgeSubPathExpander;

    // To avoid "over-expanding", we require knowledge of our schema.
    private final Map<GraphIdentifier, DeclareGraphStatement> declaredGraphs;
    private final MetadataProvider metadataProvider;

    private static class CanonicalPatternEnvironment {
        private List<GraphSelectBlock> graphSelectBlockList;
        private List<SetOperationInput> setOperationInputList;
        private SchemaKnowledgeTable schemaKnowledgeTable;
        private boolean wasExpanded = false;

        // The following is collected for our post-canonicalization pass.
        private GraphSelectBlock selectBlockExpansionSource;
        private Set<SetOperationInput> generatedSetInputs;
        private List<VarIdentifier> userLiveVariables;
    }

    public CanonicalExpansionVisitor(GraphixRewritingContext graphixRewritingContext) {
        this.metadataProvider = graphixRewritingContext.getMetadataProvider();
        this.declaredGraphs = graphixRewritingContext.getDeclaredGraphs();
        this.danglingVertexExpander = new DanglingVertexExpander();
        this.edgeSubPathExpander = new EdgeSubPathExpander(graphixRewritingContext::getNewGraphixVariable);
        this.graphixRewritingContext = graphixRewritingContext;

        // Keep an empty environment in the stack, in case we have a non-SELECT expression.
        this.environmentStack = new ArrayDeque<>();
        this.environmentStack.addLast(new CanonicalPatternEnvironment());
    }

    @Override
    public Expression visit(SelectExpression selectExpression, ILangExpression arg) throws CompilationException {
        environmentStack.addLast(new CanonicalPatternEnvironment());
        super.visit(selectExpression, arg);

        // If expansion has occurred, we need to perform clean-up if we have output modifiers / grouping.
        CanonicalPatternEnvironment workingEnvironment = environmentStack.removeLast();
        SelectExpression workingSelectExpression = selectExpression;
        if (workingEnvironment.wasExpanded && (selectExpression.hasLimit() || selectExpression.hasOrderby()
                || workingEnvironment.graphSelectBlockList.stream().anyMatch(SelectBlock::hasGroupbyClause))) {
            PostCanonicalizationVisitor postCanonicalizationVisitor = new PostCanonicalizationVisitor(
                    graphixRewritingContext, workingEnvironment.selectBlockExpansionSource,
                    workingEnvironment.generatedSetInputs, workingEnvironment.userLiveVariables);
            workingSelectExpression = (SelectExpression) postCanonicalizationVisitor.visit(selectExpression, arg);
        }
        return workingSelectExpression;
    }

    @Override
    public Expression visit(SelectSetOperation selectSetOperation, ILangExpression arg) throws CompilationException {
        CanonicalPatternEnvironment workingEnvironment = environmentStack.getLast();
        workingEnvironment.setOperationInputList = new ArrayList<>();
        selectSetOperation.getLeftInput().accept(this, arg);
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            right.getSetOperationRightInput().accept(this, arg);
        }
        for (int i = 0; i < workingEnvironment.setOperationInputList.size(); i++) {
            SetOperationInput setOperationInput = workingEnvironment.setOperationInputList.get(i);
            if (i == 0) {
                selectSetOperation.getLeftInput().setSelectBlock(setOperationInput.getSelectBlock());
                selectSetOperation.getLeftInput().setSubquery(setOperationInput.getSubquery());

            } else if (selectSetOperation.getRightInputs().size() > i) {
                SetOperationInput setOperationRightInput =
                        selectSetOperation.getRightInputs().get(i - 1).getSetOperationRightInput();
                setOperationRightInput.setSelectBlock(setOperationInput.getSelectBlock());
                setOperationRightInput.setSubquery(setOperationRightInput.getSubquery());

            } else {
                SetOperationRight setOperationRight = new SetOperationRight(SetOpType.UNION, false, setOperationInput);
                selectSetOperation.getRightInputs().add(setOperationRight);
            }
        }
        return null;
    }

    @Override
    public Expression visit(GraphSelectBlock graphSelectBlock, ILangExpression arg) throws CompilationException {
        CanonicalPatternEnvironment workingEnvironment = environmentStack.getLast();
        workingEnvironment.graphSelectBlockList = new ArrayList<>();
        workingEnvironment.graphSelectBlockList.add(graphSelectBlock);
        super.visit(graphSelectBlock, arg);

        if (workingEnvironment.wasExpanded) {
            // If we have expanded, then we need to keep track of this GRAPH-SELECT-BLOCK.
            workingEnvironment.selectBlockExpansionSource = new GraphixDeepCopyVisitor().visit(graphSelectBlock, null);

            // We also need to collect all live variables up to this point.
            workingEnvironment.userLiveVariables = new ArrayList<>();
            for (MatchClause matchClause : graphSelectBlock.getFromGraphClause().getMatchClauses()) {
                for (PathPatternExpr pathExpression : matchClause.getPathExpressions()) {
                    if (pathExpression.getVariableExpr() != null) {
                        workingEnvironment.userLiveVariables.add(pathExpression.getVariableExpr().getVar());
                    }
                    for (VertexPatternExpr vertexExpression : pathExpression.getVertexExpressions()) {
                        VarIdentifier vertexVariable = vertexExpression.getVariableExpr().getVar();
                        if (!GraphixRewritingContext.isGraphixVariable(vertexVariable)) {
                            workingEnvironment.userLiveVariables.add(vertexVariable);
                        }
                    }
                    for (EdgePatternExpr edgeExpression : pathExpression.getEdgeExpressions()) {
                        VarIdentifier edgeVariable = edgeExpression.getEdgeDescriptor().getVariableExpr().getVar();
                        if (!GraphixRewritingContext.isGraphixVariable(edgeVariable)) {
                            workingEnvironment.userLiveVariables.add(edgeVariable);
                        }
                    }
                }
            }
            if (!graphSelectBlock.getFromGraphClause().getCorrelateClauses().isEmpty()) {
                FromGraphClause fromGraphClause = graphSelectBlock.getFromGraphClause();
                List<AbstractBinaryCorrelateClause> correlateClauses = fromGraphClause.getCorrelateClauses();
                for (AbstractBinaryCorrelateClause correlateClause : correlateClauses) {
                    VarIdentifier bindingVariable = correlateClause.getRightVariable().getVar();
                    if (!GraphixRewritingContext.isGraphixVariable(bindingVariable)) {
                        workingEnvironment.userLiveVariables.add(bindingVariable);
                    }
                }
            }
            if (graphSelectBlock.hasLetWhereClauses()) {
                for (AbstractClause abstractClause : graphSelectBlock.getLetWhereList()) {
                    if (abstractClause.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                        LetClause letClause = (LetClause) abstractClause;
                        VarIdentifier bindingVariable = letClause.getVarExpr().getVar();
                        if (!GraphixRewritingContext.isGraphixVariable(bindingVariable)) {
                            workingEnvironment.userLiveVariables.add(bindingVariable);
                        }
                    }
                }
            }
        }
        return null;
    }

    @Override
    public Expression visit(FromGraphClause fromGraphClause, ILangExpression arg) throws CompilationException {
        CanonicalPatternEnvironment workingEnvironment = environmentStack.getLast();

        // Establish our schema knowledge.
        if (fromGraphClause.getGraphConstructor() == null) {
            DataverseName dataverseName = (fromGraphClause.getDataverseName() == null)
                    ? metadataProvider.getDefaultDataverseName() : fromGraphClause.getDataverseName();
            Identifier graphName = fromGraphClause.getGraphName();

            // First, try to find our graph inside our declared graph set.
            GraphIdentifier graphIdentifier = new GraphIdentifier(dataverseName, graphName.getValue());
            DeclareGraphStatement declaredGraph = declaredGraphs.get(graphIdentifier);
            if (declaredGraph != null) {
                workingEnvironment.schemaKnowledgeTable = new SchemaKnowledgeTable(declaredGraph.getGraphConstructor());

            } else {
                // Otherwise, fetch the graph from our metadata.
                try {
                    Graph graphFromMetadata =
                            getGraph(metadataProvider.getMetadataTxnContext(), dataverseName, graphName.getValue());
                    if (graphFromMetadata == null) {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, fromGraphClause.getSourceLocation(),
                                "Graph " + graphName.getValue() + " does not exist.");
                    }
                    workingEnvironment.schemaKnowledgeTable = new SchemaKnowledgeTable(graphFromMetadata);

                } catch (AlgebricksException e) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, fromGraphClause.getSourceLocation(),
                            "Graph " + graphName.getValue() + " does not exist.");
                }
            }

        } else {
            workingEnvironment.schemaKnowledgeTable = new SchemaKnowledgeTable(fromGraphClause.getGraphConstructor());
        }
        super.visit(fromGraphClause, arg);

        // Create SOI from our GSBs back to our immediate ancestor SELECT-EXPR.
        if (workingEnvironment.graphSelectBlockList.size() > 1) {
            workingEnvironment.generatedSetInputs = new HashSet<>();
            workingEnvironment.wasExpanded = true;
        }
        for (GraphSelectBlock graphSelectBlock : workingEnvironment.graphSelectBlockList) {
            SetOperationInput setOperationInput = new SetOperationInput(graphSelectBlock, null);
            workingEnvironment.setOperationInputList.add(setOperationInput);
            if (workingEnvironment.wasExpanded) {
                workingEnvironment.generatedSetInputs.add(setOperationInput);
            }
        }
        return null;
    }

    @Override
    public Expression visit(PathPatternExpr pathPatternExpr, ILangExpression arg) throws CompilationException {
        CanonicalPatternEnvironment workingEnvironment = environmentStack.getLast();
        Set<VertexPatternExpr> connectedVertices = new HashSet<>();
        for (EdgePatternExpr edgeExpression : pathPatternExpr.getEdgeExpressions()) {
            connectedVertices.add(edgeExpression.getLeftVertex());
            connectedVertices.add(edgeExpression.getRightVertex());
            edgeSubPathExpander.resetSchema(workingEnvironment.schemaKnowledgeTable);
            edgeSubPathExpander.apply(edgeExpression, workingEnvironment.graphSelectBlockList);
        }
        for (VertexPatternExpr vertexExpression : pathPatternExpr.getVertexExpressions()) {
            if (!connectedVertices.contains(vertexExpression)) {
                danglingVertexExpander.apply(vertexExpression, workingEnvironment.graphSelectBlockList);
            }
        }
        return pathPatternExpr;
    }
}
