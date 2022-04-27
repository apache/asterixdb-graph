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
package org.apache.asterix.graphix.lang.rewrites.lower.assembly;

import static org.apache.asterix.graphix.lang.rewrites.lower.assembly.IsomorphismLowerAssembly.getMatchEvaluationKind;
import static org.apache.asterix.graphix.lang.rewrites.util.ClauseRewritingUtil.buildConnectedClauses;
import static org.apache.asterix.graphix.lang.rewrites.util.EdgeRewritingUtil.buildEdgeKeyBuilder;
import static org.apache.asterix.graphix.lang.rewrites.util.EdgeRewritingUtil.buildEdgeLabelPredicateBuilder;
import static org.apache.asterix.graphix.lang.rewrites.util.EdgeRewritingUtil.buildVertexEdgeJoin;
import static org.apache.asterix.graphix.lang.rewrites.util.LowerRewritingUtil.buildAccessorList;
import static org.apache.asterix.graphix.lang.rewrites.util.LowerRewritingUtil.buildSetOpInputWithFrom;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.assembly.ExprAssembler;
import org.apache.asterix.graphix.lang.rewrites.expand.DirectedFixedPathExpansion;
import org.apache.asterix.graphix.lang.rewrites.expand.DirectedVarPathExpansion;
import org.apache.asterix.graphix.lang.rewrites.expand.IEdgePatternExpansion;
import org.apache.asterix.graphix.lang.rewrites.expand.PathEnumerationEnvironment;
import org.apache.asterix.graphix.lang.rewrites.expand.UndirectedEdgeExpansion;
import org.apache.asterix.graphix.lang.rewrites.expand.UndirectedFixedPathExpansion;
import org.apache.asterix.graphix.lang.rewrites.expand.UndirectedVarPathExpansion;
import org.apache.asterix.graphix.lang.rewrites.lower.LowerSupplierContext;
import org.apache.asterix.graphix.lang.rewrites.record.EdgeRecord;
import org.apache.asterix.graphix.lang.rewrites.record.VertexRecord;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.optype.SetOpType;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;

/**
 * Edge expansion lowering assembly for handling {@link EdgePatternExpr} lowering.
 * - The output FROM-TERM is a collection of 2-way JOINs between the intermediate lowered edge ({@link EdgeRecord}
 * instance) and the corresponding vertices (as {@link VertexRecord} instances). The order of which vertex is JOINed
 * first depends on which vertex is the source and which vertex is the destination. In the case of a bidirectional edge
 * and/or sub-paths, we return a FROM-TERM that is the UNION of simple directed 1-hop edges.
 * - The output projections are minimally composed of the edge variable expression and the two corresponding vertex
 * variable expressions. If we have a bidirectional edge and/or a sub-path, then we must qualify all of our projections
 * to account for the additional subtree (i.e. the UNION-ALL).
 */
public class ExpandEdgeLowerAssembly extends AbstractLowerAssembly {
    public final static String METADATA_CONFIG_NAME = "expand-edge";

    private final Deque<FromTermEnvironment> environmentStack;
    private final EdgePatternExpr inputEdgePatternExpr;

    // The lists below are specific to each FROM-TERM chain.
    private final Deque<List<LetClause>> generatedReboundExpressions = new ArrayDeque<>();
    private final Deque<List<Expression>> generatedEdgeConjuncts = new ArrayDeque<>();
    private final Deque<List<Expression>> generatedVertexConjuncts = new ArrayDeque<>();

    // We will introduce this after we collect each FROM-TERM chain.
    private VariableExpr qualifyingIdentifier;

    public ExpandEdgeLowerAssembly(EdgePatternExpr inputEdgePatternExpr, LowerSupplierContext lowerSupplierContext) {
        super(lowerSupplierContext);
        this.inputEdgePatternExpr = inputEdgePatternExpr;
        this.environmentStack = new ArrayDeque<>();
    }

    private Iterable<Iterable<EdgePatternExpr>> expandEdgePatternExpr(EdgePatternExpr edgePatternExpr) {
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();

        if (edgeDescriptor.getEdgeDirection() != EdgeDescriptor.EdgeDirection.UNDIRECTED
                && edgeDescriptor.getPatternType() == EdgeDescriptor.PatternType.EDGE) {
            // We have a single directed edge. We do not need to break this up.
            return List.of(List.of(edgePatternExpr));

        } else if (edgeDescriptor.getEdgeDirection() == EdgeDescriptor.EdgeDirection.UNDIRECTED
                && edgeDescriptor.getPatternType() == EdgeDescriptor.PatternType.EDGE) {
            return new UndirectedEdgeExpansion().expand(edgePatternExpr);

        } else { // edgeDescriptor.getEdgeClass() == EdgeDescriptor.PatternType.PATH
            PathEnumerationEnvironment decompositionEnvironment = new PathEnumerationEnvironment(edgePatternExpr,
                    lowerSupplierContext, generatedVertexConjuncts::addLast, generatedEdgeConjuncts::addLast,
                    generatedReboundExpressions::addLast);

            IEdgePatternExpansion edgePatternDecomposition;
            if (edgeDescriptor.getEdgeDirection() != EdgeDescriptor.EdgeDirection.UNDIRECTED
                    && Objects.equals(edgeDescriptor.getMinimumHops(), edgeDescriptor.getMaximumHops())) {
                edgePatternDecomposition = new DirectedFixedPathExpansion(decompositionEnvironment);

            } else if (edgeDescriptor.getEdgeDirection() == EdgeDescriptor.EdgeDirection.UNDIRECTED
                    && Objects.equals(edgeDescriptor.getMinimumHops(), edgeDescriptor.getMaximumHops())) {
                edgePatternDecomposition = new UndirectedFixedPathExpansion(decompositionEnvironment);

            } else if (edgeDescriptor.getEdgeDirection() != EdgeDescriptor.EdgeDirection.UNDIRECTED
                    && !Objects.equals(edgeDescriptor.getMinimumHops(), edgeDescriptor.getMaximumHops())) {
                edgePatternDecomposition = new DirectedVarPathExpansion(decompositionEnvironment);

            } else { // == ...UNDIRECTED && !Objects.equals(...getMinimumHops(), ...getMaximumHops())
                edgePatternDecomposition = new UndirectedVarPathExpansion(decompositionEnvironment);
            }
            return edgePatternDecomposition.expand(edgePatternExpr);
        }
    }

    @Override
    protected List<Projection> buildOutputProjections() {
        VariableExpr leftVarExpr = inputEdgePatternExpr.getLeftVertex().getVariableExpr();
        VariableExpr rightVarExpr = inputEdgePatternExpr.getRightVertex().getVariableExpr();
        VariableExpr edgeVarExpr = inputEdgePatternExpr.getEdgeDescriptor().getVariableExpr();

        // Qualify our output projections (if necessary).
        AbstractExpression leftVar = leftVarExpr, rightVar = rightVarExpr, edgeVar = edgeVarExpr;
        if (qualifyingIdentifier != null) {
            VarIdentifier leftVarID = SqlppVariableUtil.toUserDefinedVariableName(leftVarExpr.getVar());
            VarIdentifier rightVarID = SqlppVariableUtil.toUserDefinedVariableName(rightVarExpr.getVar());
            VarIdentifier edgeVarID = SqlppVariableUtil.toUserDefinedVariableName(edgeVarExpr.getVar());
            leftVar = new FieldAccessor(qualifyingIdentifier, leftVarID);
            rightVar = new FieldAccessor(qualifyingIdentifier, rightVarID);
            edgeVar = new FieldAccessor(qualifyingIdentifier, edgeVarID);
        }

        // Determine the name to assign to our projections.
        String leftVarAs = SqlppVariableUtil.toUserDefinedName(leftVarExpr.getVar().getValue());
        String rightVarAs = SqlppVariableUtil.toUserDefinedName(rightVarExpr.getVar().getValue());
        String edgeVarAs = SqlppVariableUtil.toUserDefinedName(edgeVarExpr.getVar().getValue());

        // Add our vertices and our edge.
        List<Projection> projectionList = new ArrayList<>();
        projectionList.add(new Projection(Projection.Kind.NAMED_EXPR, leftVar, leftVarAs));
        projectionList.add(new Projection(Projection.Kind.NAMED_EXPR, rightVar, rightVarAs));
        projectionList.add(new Projection(Projection.Kind.NAMED_EXPR, edgeVar, edgeVarAs));

        return projectionList;
    }

    @Override
    protected FromTerm buildOutputFromTerm() throws CompilationException {
        for (Iterable<EdgePatternExpr> edgePatternCollection : expandEdgePatternExpr(inputEdgePatternExpr)) {
            // Build the initial FROM-TERM for this set of EDGE-PATTERN-EXPR.
            ExprAssembler<FromTerm> fromTermAssembler = new ExprAssembler<>();
            fromTermAssembler.bind(inputFromTerm -> {
                VertexPatternExpr leftPattern = edgePatternCollection.iterator().next().getLeftVertex();
                VertexRecord leftVertexRecord = new VertexRecord(leftPattern, graphIdentifier, lowerSupplierContext);
                VarIdentifier leadingIdentifier = new VarIdentifier(leftVertexRecord.getVarExpr().getVar());
                VariableExpr leadingVariable = new VariableExpr(leadingIdentifier);
                return new FromTerm(leftVertexRecord.getExpression(), leadingVariable, null, null);
            });

            // Build our correlated clauses from this collection.
            for (EdgePatternExpr edgePatternExpr : edgePatternCollection) {
                EdgeRecord edgeRecord = new EdgeRecord(edgePatternExpr, graphIdentifier, lowerSupplierContext);

                // Lower our left and right vertices.
                VertexPatternExpr leftPattern = edgePatternExpr.getLeftVertex();
                VertexPatternExpr rightPattern = edgePatternExpr.getRightVertex();
                VertexRecord leftRecord = new VertexRecord(leftPattern, graphIdentifier, lowerSupplierContext);
                VertexRecord rightRecord = new VertexRecord(rightPattern, graphIdentifier, lowerSupplierContext);

                // Attach the correlated clauses associated with this edge.
                EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
                fromTermAssembler.bind(inputFromTerm -> {
                    // We need to determine which fields to access for our edge.
                    Function<GraphElementIdentifier, ElementLabel> leftEdgeLabelAccess;
                    Function<GraphElementIdentifier, ElementLabel> rightEdgeLabelAccess;
                    Function<GraphElementIdentifier, List<List<String>>> leftEdgeKeyAccess;
                    Function<GraphElementIdentifier, List<List<String>>> rightEdgeKeyAccess;
                    if (edgeDescriptor.getEdgeDirection() == EdgeDescriptor.EdgeDirection.LEFT_TO_RIGHT) {
                        leftEdgeKeyAccess = elementLookupTable::getEdgeSourceKeys;
                        rightEdgeKeyAccess = elementLookupTable::getEdgeDestKeys;
                        leftEdgeLabelAccess = elementLookupTable::getEdgeSourceLabel;
                        rightEdgeLabelAccess = elementLookupTable::getEdgeDestLabel;

                    } else {
                        leftEdgeKeyAccess = elementLookupTable::getEdgeDestKeys;
                        rightEdgeKeyAccess = elementLookupTable::getEdgeSourceKeys;
                        leftEdgeLabelAccess = elementLookupTable::getEdgeDestLabel;
                        rightEdgeLabelAccess = elementLookupTable::getEdgeSourceLabel;
                    }

                    // Build the join predicate to connect our left vertex to our edge...
                    VariableExpr leftVar = new VariableExpr(leftRecord.getVarExpr().getVar());
                    VariableExpr edgeVar = new VariableExpr(edgeRecord.getVarExpr().getVar());
                    Expression leftToEdgeJoinPredicate =
                            buildVertexEdgeJoin(leftRecord.getElementIdentifiers(), edgeRecord.getElementIdentifiers(),
                                    buildEdgeLabelPredicateBuilder(leftVar, edgeVar, leftEdgeLabelAccess),
                                    (v, e) -> v.getElementLabel().equals(leftEdgeLabelAccess.apply(e)),
                                    v -> buildAccessorList(leftVar, elementLookupTable.getVertexKey(v)),
                                    buildEdgeKeyBuilder(edgeVar, elementLookupTable, leftEdgeKeyAccess));

                    // ...and attach the INNER-JOIN node to our FROM-TERM.
                    VariableExpr leftToEdgeVar = new VariableExpr(new VarIdentifier(edgeVar.getVar()));
                    inputFromTerm.getCorrelateClauses().add(new JoinClause(JoinType.INNER, edgeRecord.getExpression(),
                            leftToEdgeVar, null, leftToEdgeJoinPredicate, null));

                    // Build the join predicate to connect our right vertex to our edge...
                    VariableExpr rightVar = new VariableExpr(rightRecord.getVarExpr().getVar());
                    Expression rightToEdgeJoinPredicate =
                            buildVertexEdgeJoin(rightRecord.getElementIdentifiers(), edgeRecord.getElementIdentifiers(),
                                    buildEdgeLabelPredicateBuilder(rightVar, edgeVar, rightEdgeLabelAccess),
                                    (v, e) -> v.getElementLabel().equals(rightEdgeLabelAccess.apply(e)),
                                    v -> buildAccessorList(rightVar, elementLookupTable.getVertexKey(v)),
                                    buildEdgeKeyBuilder(edgeVar, elementLookupTable, rightEdgeKeyAccess));

                    // ...and attach the INNER-JOIN node to our FROM-TERM.
                    VariableExpr edgeToRightVar = new VariableExpr(new VarIdentifier(rightVar.getVar()));
                    inputFromTerm.getCorrelateClauses().add(new JoinClause(JoinType.INNER, rightRecord.getExpression(),
                            edgeToRightVar, null, rightToEdgeJoinPredicate, null));

                    return inputFromTerm;
                });
            }

            // Save our state onto our stack.
            FromTermEnvironment fromTermEnvironment = new FromTermEnvironment();
            fromTermEnvironment.fromTermAssembler = fromTermAssembler;
            fromTermEnvironment.outputProjections = buildOutputProjections();
            fromTermEnvironment.outputLetWhereClauses = buildLetWhereClauses();
            environmentStack.addLast(fromTermEnvironment);
        }

        // At this point, we should have a collection of FROM-TERMs. Build SET-OP-INPUT for each FROM-TERM.
        Iterator<FromTermEnvironment> environmentIterator = environmentStack.iterator();
        Deque<SetOperationInput> setOpInputStack =
                environmentStack.stream().map(e -> e.fromTermAssembler.getLast()).map(f -> {
                    FromTermEnvironment fromTermEnvironment = environmentIterator.next();
                    List<Projection> outputProjections = fromTermEnvironment.outputProjections;
                    List<AbstractClause> outputLetWhereClauses = fromTermEnvironment.outputLetWhereClauses;
                    return buildSetOpInputWithFrom(f, outputProjections, outputLetWhereClauses);
                }).collect(Collectors.toCollection(ArrayDeque::new));

        // UNION-ALL each SET-OP-INPUT (if we have any).
        SetOperationInput leftSetOpInput = setOpInputStack.removeFirst();
        SelectSetOperation selectSetOperation = new SelectSetOperation(leftSetOpInput, setOpInputStack.stream()
                .map(s -> new SetOperationRight(SetOpType.UNION, false, s)).collect(Collectors.toList()));

        // We return a new FROM-TERM which will bind the SELECT-EXPR above to our qualifying var.
        qualifyingIdentifier = new VariableExpr(lowerSupplierContext.getNewVariable());
        SelectExpression selectExpression = new SelectExpression(null, selectSetOperation, null, null, true);
        return new FromTerm(selectExpression, qualifyingIdentifier, null, null);
    }

    private List<AbstractClause> buildLetWhereClauses() throws CompilationException {
        // Determine if any isomorphism is desired.
        List<Expression> isomorphismConjuncts = new ArrayList<>();
        switch (getMatchEvaluationKind(lowerSupplierContext.getMetadataProvider())) {
            case TOTAL_ISOMORPHISM:
            case VERTEX_ISOMORPHISM:
                if (!generatedVertexConjuncts.isEmpty()) {
                    isomorphismConjuncts = generatedVertexConjuncts.removeFirst();
                }
                break;

            case EDGE_ISOMORPHISM:
                if (!generatedEdgeConjuncts.isEmpty()) {
                    isomorphismConjuncts = generatedEdgeConjuncts.removeFirst();
                }
                break;
        }

        // Finally, build our LET-WHERE list.
        List<AbstractClause> letWhereClauseList = new ArrayList<>();
        if (!isomorphismConjuncts.isEmpty()) {
            letWhereClauseList.add(new WhereClause(buildConnectedClauses(isomorphismConjuncts, OperatorType.AND)));
        }
        if (!generatedReboundExpressions.isEmpty() && !generatedReboundExpressions.getLast().isEmpty()) {
            letWhereClauseList.addAll(generatedReboundExpressions.removeFirst());
        }
        return letWhereClauseList;
    }

    // If multiple FROM-TERMs are required, our environment stack will have more than one element.
    private static class FromTermEnvironment {
        ExprAssembler<FromTerm> fromTermAssembler;
        List<Projection> outputProjections;
        List<AbstractClause> outputLetWhereClauses;
    }
}
