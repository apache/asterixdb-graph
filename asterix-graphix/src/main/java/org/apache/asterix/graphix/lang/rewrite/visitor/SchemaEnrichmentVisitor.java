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
import static org.apache.asterix.graphix.function.GraphixFunctionMap.getFunctionPrepare;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.algebra.compiler.option.SchemaDecorateEdgeOption;
import org.apache.asterix.graphix.algebra.compiler.option.SchemaDecorateVertexOption;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.graphix.function.prepare.IFunctionPrepare;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.LowerListClause;
import org.apache.asterix.graphix.lang.clause.LowerSwitchClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.common.BranchLookupTable;
import org.apache.asterix.graphix.lang.rewrite.common.ElementLookupTable;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseCollection;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * Perform a pass to enrich any {@link LetClause} and {@link LowerSwitchClause} nodes with necessary schema
 * information. Note that this process may overestimate the amount of enrichment actually required (to minimize this
 * difference requires some form of equivalence classes @ the rewriter level).
 */
public class SchemaEnrichmentVisitor extends AbstractGraphixQueryVisitor {
    private final ElementLookupTable elementLookupTable;
    private final Set<FunctionIdentifier> functionIdentifiers;
    private final Map<VariableExpr, Expression> expressionMap;
    private final SelectBlock workingSelectBlock;
    private final GraphIdentifier graphIdentifier;
    private final BranchLookupTable branchLookupTable;

    public SchemaEnrichmentVisitor(SchemaDecorateVertexOption vertexMode, SchemaDecorateEdgeOption edgeMode,
            ElementLookupTable elementLookupTable, BranchLookupTable branchLookupTable, GraphIdentifier graphIdentifier,
            SelectBlock workingSelectBlock, Set<FunctionIdentifier> functionIDs) {
        this.graphIdentifier = graphIdentifier;
        this.elementLookupTable = elementLookupTable;
        this.workingSelectBlock = workingSelectBlock;
        this.branchLookupTable = branchLookupTable;
        this.expressionMap = new HashMap<>();

        // If we always need to perform schema-enrichment, then we add all of our schema functions to this set.
        this.functionIdentifiers = functionIDs;
        if (vertexMode == SchemaDecorateVertexOption.ALWAYS) {
            this.functionIdentifiers.add(GraphixFunctionIdentifiers.ELEMENT_LABEL);
            this.functionIdentifiers.add(GraphixFunctionIdentifiers.VERTEX_DETAIL);
        }
        if (edgeMode == SchemaDecorateEdgeOption.ALWAYS) {
            this.functionIdentifiers.add(GraphixFunctionIdentifiers.ELEMENT_LABEL);
            this.functionIdentifiers.add(GraphixFunctionIdentifiers.EDGE_DETAIL);
            this.functionIdentifiers.add(GraphixFunctionIdentifiers.EDGE_DIRECTION);
            this.functionIdentifiers.add(GraphixFunctionIdentifiers.EDGE_SOURCE_VERTEX);
            this.functionIdentifiers.add(GraphixFunctionIdentifiers.EDGE_DEST_VERTEX);
        }
    }

    private void visitClauseCollection(ClauseCollection clauseCollection) throws CompilationException {
        for (FunctionIdentifier functionIdentifier : functionIdentifiers) {
            IFunctionPrepare functionPrepare = getFunctionPrepare(functionIdentifier);
            for (LetClause representativeEdgeBinding : clauseCollection.getRepresentativeEdgeBindings()) {
                VariableExpr rightVar = representativeEdgeBinding.getVarExpr();
                Expression graphExpr = expressionMap.get(rightVar);
                if (isEdgeFunction(functionIdentifier)) {
                    Expression outputExpr = functionPrepare.prepare(representativeEdgeBinding.getBindingExpr(),
                            graphExpr, graphIdentifier, elementLookupTable);
                    representativeEdgeBinding.setBindingExpr(outputExpr);
                }
            }
            for (LetClause representativeVertexBinding : clauseCollection.getRepresentativeVertexBindings()) {
                VariableExpr rightVar = representativeVertexBinding.getVarExpr();
                Expression graphExpr = expressionMap.get(rightVar);
                if (isVertexFunction(functionIdentifier)) {
                    Expression outputExpr = functionPrepare.prepare(representativeVertexBinding.getBindingExpr(),
                            graphExpr, graphIdentifier, elementLookupTable);
                    representativeVertexBinding.setBindingExpr(outputExpr);
                }
            }
            for (Pair<VariableExpr, LetClause> eagerVertexBinding : clauseCollection.getEagerVertexBindings()) {
                Expression graphExpr = expressionMap.get(eagerVertexBinding.first);
                if (isVertexFunction(functionIdentifier)) {
                    Expression outputExpr = functionPrepare.prepare(eagerVertexBinding.second.getBindingExpr(),
                            graphExpr, graphIdentifier, elementLookupTable);
                    eagerVertexBinding.second.setBindingExpr(outputExpr);
                }
            }
            for (AbstractClause nonRepresentativeClause : clauseCollection.getNonRepresentativeClauses()) {
                if (nonRepresentativeClause.getClauseType() == Clause.ClauseType.EXTENSION) {
                    LowerSwitchClause lowerSwitchClause = (LowerSwitchClause) nonRepresentativeClause;
                    for (ClauseCollection innerClauseCollection : lowerSwitchClause.getCollectionLookupTable()) {
                        visitClauseCollection(innerClauseCollection);
                    }
                }
            }
        }
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        // Visit our immediate MATCH AST nodes (do not visit lower levels).
        FromGraphClause fromGraphClause = (FromGraphClause) selectBlock.getFromClause();
        for (MatchClause matchClause : fromGraphClause.getMatchClauses()) {
            matchClause.accept(this, arg);
        }

        // We are going to enrich these LET-CLAUSEs & FIXED-POINT-CLAUSEs w/ the necessary schema.
        if (workingSelectBlock.equals(selectBlock)) {
            LowerListClause lowerClause = (LowerListClause) fromGraphClause.getLowerClause();
            ClauseCollection clauseCollection = lowerClause.getClauseCollection();
            visitClauseCollection(clauseCollection);
        }
        return null;
    }

    @Override
    public Expression visit(VertexPatternExpr vertexPatternExpr, ILangExpression arg) throws CompilationException {
        VariableExpr variableExpr = vertexPatternExpr.getVariableExpr();
        if (variableExpr != null) {
            expressionMap.put(variableExpr, vertexPatternExpr);
        }
        return super.visit(vertexPatternExpr, arg);
    }

    @Override
    public Expression visit(EdgePatternExpr edgePatternExpr, ILangExpression arg) throws CompilationException {
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        VariableExpr variableExpr = edgeDescriptor.getVariableExpr();
        if (variableExpr != null) {
            expressionMap.put(variableExpr, edgePatternExpr);
        }
        if (branchLookupTable.getBranches(edgePatternExpr) != null) {
            for (EdgePatternExpr branch : branchLookupTable.getBranches(edgePatternExpr)) {
                branch.getLeftVertex().accept(this, arg);
                branch.getRightVertex().accept(this, arg);
                branch.accept(this, arg);
            }
        }
        edgePatternExpr.getLeftVertex().accept(this, arg);
        edgePatternExpr.getRightVertex().accept(this, arg);
        return edgePatternExpr;
    }
}
