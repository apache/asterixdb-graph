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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.algebra.compiler.provider.GraphixCompilationProvider;
import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.GraphSelectBlock;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrites.assembly.IExprAssembly;
import org.apache.asterix.graphix.lang.rewrites.common.EdgeDependencyGraph;
import org.apache.asterix.graphix.lang.rewrites.common.ElementLookupTable;
import org.apache.asterix.graphix.lang.rewrites.lower.GraphixLowerSupplier;
import org.apache.asterix.graphix.lang.rewrites.lower.LowerSupplierContext;
import org.apache.asterix.graphix.lang.rewrites.lower.LowerSupplierNode;
import org.apache.asterix.graphix.lang.rewrites.lower.assembly.ExpandEdgeLowerAssembly;
import org.apache.asterix.graphix.lang.rewrites.visitor.ElementAnalysisVisitor.FromGraphClauseContext;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;

/**
 * Rewrite a graph AST to utilize non-graph AST nodes (i.e. replace the GRAPH-SELECT-BLOCK with a SELECT-BLOCK).
 *
 * @see org.apache.asterix.graphix.lang.rewrites.lower.GraphixLowerSupplier
 */
public class GraphixLoweringVisitor extends AbstractGraphixQueryVisitor {
    private final Map<FromGraphClause, FromGraphClauseContext> fromGraphClauseContextMap;
    private final ElementLookupTable<GraphElementIdentifier> elementLookupTable;
    private final GraphixRewritingContext graphixRewritingContext;

    // In addition to the parent GRAPH-SELECT-BLOCK, we must keep track of the parent SELECT-EXPR.
    private SelectExpression selectExpression;

    public GraphixLoweringVisitor(Map<FromGraphClause, FromGraphClauseContext> fromGraphClauseContextMap,
            ElementLookupTable<GraphElementIdentifier> elementLookupTable,
            GraphixRewritingContext graphixRewritingContext) {
        this.fromGraphClauseContextMap = fromGraphClauseContextMap;
        this.elementLookupTable = elementLookupTable;
        this.graphixRewritingContext = graphixRewritingContext;
    }

    @Override
    public Expression visit(GraphSelectBlock graphSelectBlock, ILangExpression arg) throws CompilationException {
        boolean hadFromGraphClause = false;
        if (graphSelectBlock.hasFromGraphClause()) {
            // We will remove the FROM-GRAPH node and replace this with a FROM node on the child visit.
            selectExpression = (SelectExpression) arg;
            hadFromGraphClause = true;
            graphSelectBlock.getFromGraphClause().accept(this, graphSelectBlock);
        }
        if (graphSelectBlock.hasLetWhereClauses()) {
            for (AbstractClause clause : graphSelectBlock.getLetWhereList()) {
                clause.accept(this, arg);
            }
        }
        if (graphSelectBlock.hasGroupbyClause()) {
            graphSelectBlock.getGroupbyClause().accept(this, arg);
        }
        if (graphSelectBlock.hasLetHavingClausesAfterGroupby()) {
            for (AbstractClause clause : graphSelectBlock.getLetHavingListAfterGroupby()) {
                clause.accept(this, arg);
            }
        }
        if (hadFromGraphClause && graphSelectBlock.getSelectClause().selectRegular()) {
            // A special case: if we have SELECT *, modify this to VAR.*.
            if (graphSelectBlock.getSelectClause().getSelectRegular().getProjections().stream()
                    .allMatch(p -> p.getKind() == Projection.Kind.STAR)) {
                FromTerm fromTerm = graphSelectBlock.getFromClause().getFromTerms().get(0);
                Projection projection = new Projection(Projection.Kind.VAR_STAR, fromTerm.getLeftVariable(), null);
                SelectRegular selectRegular = new SelectRegular(List.of(projection));
                graphSelectBlock.getSelectClause().setSelectRegular(selectRegular);
            }
        }
        graphSelectBlock.getSelectClause().accept(this, arg);
        return null;
    }

    @Override
    public Expression visit(LetClause letClause, ILangExpression arg) throws CompilationException {
        // We also need to visit the binding variable here (in case we need to remove the GRAPH-VARIABLE-EXPR).
        letClause.setVarExpr((VariableExpr) letClause.getVarExpr().accept(this, arg));
        return super.visit(letClause, arg);
    }

    @Override
    public Expression visit(FromGraphClause fromGraphClause, ILangExpression arg) throws CompilationException {
        GraphSelectBlock parentGraphSelect = (GraphSelectBlock) arg;

        // Build the context for our lowering strategy.
        FromGraphClauseContext fromGraphClauseContext = fromGraphClauseContextMap.get(fromGraphClause);
        MetadataProvider metadataProvider = graphixRewritingContext.getMetadataProvider();
        DataverseName dataverseName = (fromGraphClause.getDataverseName() == null)
                ? metadataProvider.getDefaultDataverseName() : fromGraphClause.getDataverseName();
        String graphName = (fromGraphClause.getGraphName() != null) ? fromGraphClause.getGraphName().getValue()
                : fromGraphClause.getGraphConstructor().getInstanceID();
        List<PathPatternExpr> pathPatternExprList = fromGraphClause.getMatchClauses().stream()
                .map(MatchClause::getPathExpressions).flatMap(Collection::stream).collect(Collectors.toList());
        GraphIdentifier graphIdentifier = new GraphIdentifier(dataverseName, graphName);
        LowerSupplierContext lowerSupplierContext = new LowerSupplierContext(graphixRewritingContext, graphIdentifier,
                fromGraphClauseContext.getDanglingVertices(), fromGraphClauseContext.getOptionalVariables(),
                pathPatternExprList, elementLookupTable);

        // Determine our lowering strategy. By default, we will fully expand any variable-length edges.
        String strategyMetadataKeyName = GraphixCompilationProvider.EDGE_STRATEGY_METADATA_CONFIG;
        Function<EdgePatternExpr, IExprAssembly<LowerSupplierNode>> edgeHandlingStrategy;
        if (metadataProvider.getConfig().containsKey(strategyMetadataKeyName)) {
            String strategyProperty = metadataProvider.getProperty(strategyMetadataKeyName);
            if (strategyProperty.equalsIgnoreCase(ExpandEdgeLowerAssembly.METADATA_CONFIG_NAME)) {
                edgeHandlingStrategy = e -> new ExpandEdgeLowerAssembly(e, lowerSupplierContext);

            } else {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, fromGraphClause.getSourceLocation(),
                        "Unknown edge handling strategy specified: " + strategyProperty);
            }

        } else {
            edgeHandlingStrategy = e -> new ExpandEdgeLowerAssembly(e, lowerSupplierContext);
        }

        // Create a DAG of lowering nodes. We are interested in the tail of the generated DAG.
        List<LowerSupplierNode> lowerSupplierNodeList = new ArrayList<>();
        EdgeDependencyGraph edgeDependencyGraph = fromGraphClauseContext.getEdgeDependencyGraph();
        new GraphixLowerSupplier(lowerSupplierContext, edgeDependencyGraph, edgeHandlingStrategy)
                .invoke(lowerSupplierNodeList::add);
        LowerSupplierNode tailLowerNode = lowerSupplierNodeList.get(lowerSupplierNodeList.size() - 1);

        // Build our FROM-TERM, which will reference the tail lower node.
        VariableExpr qualifyingVariable = tailLowerNode.getBindingVar();
        FromTerm workingFromTerm = new FromTerm(qualifyingVariable, new VariableExpr(qualifyingVariable.getVar()), null,
                fromGraphClause.getCorrelateClauses());
        parentGraphSelect.setFromGraphClause(null);

        // Introduce the variables from our assembly into our current SELECT-BLOCK.
        List<AbstractClause> lowerNodeVariables = tailLowerNode.getProjectionList().stream().map(p -> {
            FieldAccessor fieldAccessor = new FieldAccessor(qualifyingVariable, new Identifier(p.getName()));
            String identifierName = SqlppVariableUtil.toInternalVariableName(p.getName());
            VariableExpr variableExpr = new VariableExpr(new VarIdentifier(identifierName));
            return new LetClause(variableExpr, fieldAccessor);
        }).collect(Collectors.toList());
        List<AbstractClause> newLetWhereList = new ArrayList<>();
        newLetWhereList.addAll(lowerNodeVariables);
        newLetWhereList.addAll(parentGraphSelect.getLetWhereList());
        parentGraphSelect.getLetWhereList().clear();
        parentGraphSelect.getLetWhereList().addAll(newLetWhereList);

        // Qualify the graph element variables in our correlate clauses.
        Supplier<AbstractGraphixQueryVisitor> qualifyingVisitorSupplier = () -> new AbstractGraphixQueryVisitor() {
            private boolean isAtTopLevel = true;

            @Override
            public Expression visit(SelectExpression selectExpression, ILangExpression arg)
                    throws CompilationException {
                // Introduce our variables into the SELECT-EXPR inside a correlated clause.
                List<LetClause> innerLowerNodeVariables = new ArrayList<>();
                for (AbstractClause abstractClause : lowerNodeVariables) {
                    innerLowerNodeVariables.add((LetClause) SqlppRewriteUtil.deepCopy(abstractClause));
                }
                List<LetClause> newLetClauseList = new ArrayList<>();
                newLetClauseList.addAll(innerLowerNodeVariables);
                newLetClauseList.addAll(selectExpression.getLetList());
                selectExpression.getLetList().clear();
                selectExpression.getLetList().addAll(newLetClauseList);

                // Indicate that we should not qualify any of the variables in this SELECT.
                boolean wasAtTopLevel = isAtTopLevel;
                isAtTopLevel = false;
                Expression resultOfVisit = super.visit(selectExpression, arg);
                isAtTopLevel = wasAtTopLevel;
                return resultOfVisit;
            }

            @Override
            public Expression visit(VariableExpr variableExpr, ILangExpression arg) throws CompilationException {
                String variableName = SqlppVariableUtil.toUserDefinedName(variableExpr.getVar().getValue());
                if (isAtTopLevel
                        && tailLowerNode.getProjectionList().stream().anyMatch(p -> p.getName().equals(variableName))) {
                    return new FieldAccessor(qualifyingVariable, new Identifier(variableName));
                }
                return super.visit(variableExpr, arg);
            }
        };
        for (AbstractBinaryCorrelateClause correlateClause : workingFromTerm.getCorrelateClauses()) {
            AbstractGraphixQueryVisitor variableVisitor = qualifyingVisitorSupplier.get();
            if (correlateClause.getClauseType() == Clause.ClauseType.JOIN_CLAUSE) {
                // The expression bound to a JOIN clause cannot see the graph element variables of this FROM-GRAPH.
                JoinClause joinClause = (JoinClause) correlateClause;
                joinClause.setConditionExpression(joinClause.getConditionExpression().accept(variableVisitor, null));

            } else if (correlateClause.getClauseType() == Clause.ClauseType.UNNEST_CLAUSE) {
                UnnestClause unnestClause = (UnnestClause) correlateClause;
                unnestClause.setRightExpression(unnestClause.getRightExpression().accept(variableVisitor, null));

            } else { // (correlateClause.getClauseType() == Clause.ClauseType.NEST_CLAUSE) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, correlateClause.getSourceLocation(),
                        "Nest clause has not been implemented.");
            }
        }

        // Finalize our lowering: replace our FROM-GRAPH-CLAUSE with a FROM-CLAUSE and add our LET-CLAUSE list.
        parentGraphSelect.setFromClause(new FromClause(Collections.singletonList(workingFromTerm)));
        lowerSupplierNodeList.forEach(g -> selectExpression.getLetList().add(g.buildLetClause()));
        return null;
    }
}