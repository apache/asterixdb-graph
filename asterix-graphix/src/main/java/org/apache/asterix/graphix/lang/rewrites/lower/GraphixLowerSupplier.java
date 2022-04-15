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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.assembly.ExprAssembler;
import org.apache.asterix.graphix.lang.rewrites.assembly.IExprAssembly;
import org.apache.asterix.graphix.lang.rewrites.common.EdgeDependencyGraph;
import org.apache.asterix.graphix.lang.rewrites.lower.assembly.DanglingVertexLowerAssembly;
import org.apache.asterix.graphix.lang.rewrites.lower.assembly.IsomorphismLowerAssembly;
import org.apache.asterix.graphix.lang.rewrites.lower.assembly.NamedPathLowerAssembly;
import org.apache.asterix.graphix.lang.rewrites.util.EdgeRewritingUtil;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.optype.JoinType;

/**
 * Supplier for lower AST nodes (i.e. SQL++ AST nodes). We perform the following process:
 * 1. Given the edge-dependency graph, fetch all supplied orderings of {@link EdgePatternExpr}. We will create a
 * separate assembly branch for each ordering.
 * 2. Once each assembly branch has been built, construct a left-deep LEFT-OUTER-JOIN tree out of the tail of each
 * assembly. This will result in one representative "junction" {@link LowerSupplierNode} node.
 * 3. If there are dangling vertices, merge the junction node from #2 with a lower node that represents all dangling
 * vertices.
 * 4. Gather all projected vertices and edges and attach isomorphism conjuncts to the tail {@link LowerSupplierNode}
 * node. Unlike our previous steps, this does not produce a new lower node.
 * 5. If there are any named paths (i.e. not sub-paths within {@link EdgePatternExpr}}, modify the tail
 * {@link LowerSupplierNode} to define a LET-CLAUSE that binds the path variable, and a projection to output said
 * path variable. Similar to the previous step, this does not produce a new lower node.
 */
public class GraphixLowerSupplier {
    private final Function<EdgePatternExpr, IExprAssembly<LowerSupplierNode>> lowerStrategyNodeSupplier;
    private final LowerSupplierContext lowerSupplierContext;
    private final EdgeDependencyGraph edgeDependencyGraph;

    public GraphixLowerSupplier(LowerSupplierContext lowerSupplierContext, EdgeDependencyGraph edgeDependencyGraph,
            Function<EdgePatternExpr, IExprAssembly<LowerSupplierNode>> lowerStrategyNodeSupplier) {
        this.edgeDependencyGraph = edgeDependencyGraph;
        this.lowerSupplierContext = lowerSupplierContext;
        this.lowerStrategyNodeSupplier = lowerStrategyNodeSupplier;
    }

    public void invoke(Consumer<LowerSupplierNode> lowerNodeConsumer) throws CompilationException {
        // Handle each of our edge orderings. Additionally, hand off each generated LET-CLAUSE to our caller.
        List<ExprAssembler<LowerSupplierNode>> edgeAssemblers = new ArrayList<>();
        for (Iterable<EdgePatternExpr> edgeOrdering : edgeDependencyGraph) {
            ExprAssembler<LowerSupplierNode> edgeAssembler = new ExprAssembler<>();
            for (EdgePatternExpr edgePatternExpr : edgeOrdering) {
                edgeAssembler.bind(lowerStrategyNodeSupplier.apply(edgePatternExpr));
            }
            edgeAssembler.iterator().forEachRemaining(lowerNodeConsumer);
            edgeAssemblers.add(edgeAssembler);
        }

        // Join each parallel assembly with a LEFT-OUTER-JOIN.
        LowerSupplierNode tailLowerNode = edgeAssemblers.stream().sequential().map(ExprAssembler::getLast)
                .reduce(buildLowerNodeMerger(JoinType.LEFTOUTER, lowerNodeConsumer)).orElse(null);

        // If dangling vertices exist, merge the previous node with the dangling-vertex assembly.
        if (!lowerSupplierContext.getDanglingVertices().isEmpty()) {
            ExprAssembler<LowerSupplierNode> danglingVertexAssembler = new ExprAssembler<>();
            danglingVertexAssembler.bind(new DanglingVertexLowerAssembly(lowerSupplierContext));
            lowerNodeConsumer.accept(danglingVertexAssembler.getLast());

            // Connect this assembler to the edge assembly tail.
            if (tailLowerNode != null) {
                tailLowerNode = buildLowerNodeMerger(JoinType.INNER, lowerNodeConsumer).apply(tailLowerNode,
                        danglingVertexAssembler.getLast());

            } else {
                tailLowerNode = danglingVertexAssembler.getLast();
            }
        }

        // Attach our isomorphism conjuncts (and disjuncts) to the tail node (if necessary).
        IsomorphismLowerAssembly isomorphismLowerAssembly = new IsomorphismLowerAssembly(
                lowerSupplierContext.getDanglingVertices(), lowerSupplierContext.getOptionalVariables(),
                edgeDependencyGraph, lowerSupplierContext.getMetadataProvider());
        tailLowerNode = isomorphismLowerAssembly.apply(Objects.requireNonNull(tailLowerNode));

        // If we have any named paths (i.e. not a sub-path in an EDGE-PATTERN-EXPR), add them to the tail lower node.
        List<PathPatternExpr> pathPatternExprList = lowerSupplierContext.getPathPatternExprList();
        NamedPathLowerAssembly namedPathLowerAssembly = new NamedPathLowerAssembly(pathPatternExprList);
        namedPathLowerAssembly.apply(tailLowerNode);
    }

    private BinaryOperator<LowerSupplierNode> buildLowerNodeMerger(JoinType joinType,
            Consumer<LowerSupplierNode> mergerCallbackConsumer) {
        return (n1, n2) -> {
            // Build a reference to our N1 node.
            VariableExpr n1BindingVar = n1.getBindingVar();
            VariableExpr n1ReferenceVar = new VariableExpr(lowerSupplierContext.getNewVariable());

            // Build a reference to our N2 node.
            VariableExpr n2BindingVar = n2.getBindingVar();
            VariableExpr n2ReferenceVar = new VariableExpr(lowerSupplierContext.getNewVariable());

            // Build the JOIN-CLAUSE, and add the reference to N2 in a correlated clause to a FROM-TERM of N1.
            Expression whereConjunct = null;
            for (Projection n2Projection : n2.getProjectionList()) {
                Expression toProjectionExpr = new FieldAccessor(n2ReferenceVar, new Identifier(n2Projection.getName()));
                whereConjunct = EdgeRewritingUtil.buildInputAssemblyJoin(n1, n1ReferenceVar, toProjectionExpr,
                        whereConjunct, n -> n.equals(n2Projection.getName()));
            }
            List<AbstractBinaryCorrelateClause> correlateClauses = new ArrayList<>();
            correlateClauses.add(new JoinClause(joinType, n2BindingVar, n2ReferenceVar, null, whereConjunct,
                    (joinType == JoinType.INNER) ? null : Literal.Type.MISSING));
            FromTerm fromTerm = new FromTerm(n1BindingVar, n1ReferenceVar, null, correlateClauses);

            // Combine the projection list from N1 and N2. Avoid duplicates.
            List<Projection> projectionList = n1.getProjectionList().stream().map(p -> {
                String n1VarName = p.getName();
                VariableExpr n1ProjectionVar = new VariableExpr(new VarIdentifier(n1VarName));
                FieldAccessor n1ProjectionAccess = new FieldAccessor(n1ReferenceVar, n1ProjectionVar.getVar());
                return new Projection(Projection.Kind.NAMED_EXPR, n1ProjectionAccess, n1VarName);
            }).collect(Collectors.toList());
            for (Projection n2Projection : n2.getProjectionList()) {
                String n2VarName = n2Projection.getName();
                if (projectionList.stream().map(Projection::getName).noneMatch(n2VarName::equals)) {
                    VariableExpr n2ProjectionVar = new VariableExpr(new VarIdentifier(n2VarName));
                    FieldAccessor n2ProjectionAccess = new FieldAccessor(n2ReferenceVar, n2ProjectionVar.getVar());
                    projectionList.add(new Projection(Projection.Kind.NAMED_EXPR, n2ProjectionAccess, n2VarName));
                }
            }

            // Assemble the new lowering node.
            LowerSupplierNode outputLowerSupplierNode = new LowerSupplierNode(Collections.singletonList(fromTerm), null,
                    projectionList, new VariableExpr(lowerSupplierContext.getNewVariable()));
            mergerCallbackConsumer.accept(outputLowerSupplierNode);
            return outputLowerSupplierNode;
        };
    }
}
