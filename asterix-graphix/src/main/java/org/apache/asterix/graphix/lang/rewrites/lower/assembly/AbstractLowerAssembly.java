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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.rewrites.assembly.IExprAssembly;
import org.apache.asterix.graphix.lang.rewrites.common.ElementLookupTable;
import org.apache.asterix.graphix.lang.rewrites.lower.LowerSupplierContext;
import org.apache.asterix.graphix.lang.rewrites.lower.LowerSupplierNode;
import org.apache.asterix.graphix.lang.rewrites.util.EdgeRewritingUtil;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.Projection;

/**
 * Abstract lowering assembly for creating a new {@link LowerSupplierNode} out of some input {@link LowerSupplierNode}.
 * There are two parts to function application here: (1) building a new FROM-TERM and (2) building a list of
 * projections- both of which will be used to create a FROM-CLAUSE that will be wrapped in a LET-CLAUSE.
 */
public abstract class AbstractLowerAssembly implements IExprAssembly<LowerSupplierNode> {
    protected final ElementLookupTable<GraphElementIdentifier> elementLookupTable;
    protected final LowerSupplierContext lowerSupplierContext;
    protected final GraphIdentifier graphIdentifier;

    public AbstractLowerAssembly(LowerSupplierContext lowerSupplierContext) {
        this.elementLookupTable = lowerSupplierContext.getElementLookupTable();
        this.graphIdentifier = lowerSupplierContext.getGraphIdentifier();
        this.lowerSupplierContext = lowerSupplierContext;
    }

    protected abstract List<Projection> buildOutputProjections() throws CompilationException;

    protected abstract FromTerm buildOutputFromTerm() throws CompilationException;

    @Override
    public LowerSupplierNode apply(LowerSupplierNode inputNode) throws CompilationException {
        // Build the output FROM-TERM and PROJECTION list.
        List<FromTerm> fromTermList = new ArrayList<>();
        FromTerm outputFromTerm = buildOutputFromTerm();
        List<Projection> projectionList = buildOutputProjections();
        fromTermList.add(outputFromTerm);

        Expression joinConjuncts = null;
        if (inputNode != null) {
            // Build a FROM-TERM using our input.
            VariableExpr bindingInputLetVar = inputNode.getBindingVar();
            VariableExpr leadingVariable = new VariableExpr(lowerSupplierContext.getNewVariable());
            FromTerm inputFromTerm = new FromTerm(bindingInputLetVar, leadingVariable, null, null);
            fromTermList.add(inputFromTerm);

            // If we find any projections from our input that match our output, add the predicate here.
            for (Projection outputProjection : projectionList) {
                joinConjuncts = EdgeRewritingUtil.buildInputAssemblyJoin(inputNode, leadingVariable,
                        outputProjection.getExpression(), joinConjuncts, n -> n.equals(outputProjection.getName()));
            }

            // Add all projections from our input, if they do not already exist.
            for (Projection inputProjection : inputNode.getProjectionList()) {
                VarIdentifier inputProjectionVariable = new VarIdentifier(inputProjection.getName());
                VariableExpr inputProjectionVariableExpr = new VariableExpr(inputProjectionVariable);
                if (projectionList.stream().map(Projection::getName)
                        .noneMatch(p -> inputProjectionVariableExpr.getVar().getValue().equals(p))) {
                    Projection outputProjection = new Projection(Projection.Kind.NAMED_EXPR,
                            new FieldAccessor(leadingVariable, inputProjectionVariableExpr.getVar()),
                            inputProjectionVariableExpr.getVar().getValue());
                    projectionList.add(outputProjection);
                }
            }
        }

        // Finally, return the constructed lowering node.
        List<AbstractClause> letWhereClauseList = new ArrayList<>();
        if (joinConjuncts != null) {
            letWhereClauseList.add(new WhereClause(joinConjuncts));
        }
        VariableExpr bindingOutputVar = new VariableExpr(lowerSupplierContext.getNewVariable());
        return new LowerSupplierNode(fromTermList, letWhereClauseList, projectionList, bindingOutputVar);
    }
}
