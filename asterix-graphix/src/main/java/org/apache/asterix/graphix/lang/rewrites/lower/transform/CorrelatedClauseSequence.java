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
package org.apache.asterix.graphix.lang.rewrites.lower.transform;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.apache.asterix.graphix.lang.clause.CorrLetClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;

/**
 * A "general" list for maintaining a sequence of {@link AbstractBinaryCorrelateClause} AST nodes. We have:
 * 1. A list of main clauses, which our callers have free rein to manipulate.
 * 2. A list of deferred clauses, which will be inserted at the tail end of the final list.
 * 3. A list of vertex bindings, which will be inserted immediately after the main clause list.
 * 4. A list of edge bindings, which will be inserted immediately after the vertex binding list.
 */
public class CorrelatedClauseSequence implements Iterable<AbstractBinaryCorrelateClause> {
    private final LinkedList<AbstractBinaryCorrelateClause> deferredCorrelatedClauseSequence = new LinkedList<>();
    private final LinkedList<AbstractBinaryCorrelateClause> mainCorrelatedClauseSequence = new LinkedList<>();
    private final List<CorrLetClause> representativeVertexBindings = new ArrayList<>();
    private final List<CorrLetClause> representativeEdgeBindings = new ArrayList<>();

    public void addMainClause(AbstractBinaryCorrelateClause correlateClause) {
        mainCorrelatedClauseSequence.addLast(correlateClause);
    }

    public void addDeferredClause(AbstractBinaryCorrelateClause correlateClause) {
        deferredCorrelatedClauseSequence.add(correlateClause);
    }

    public void addRepresentativeVertexBinding(VarIdentifier representativeVariable, Expression bindingExpression) {
        VariableExpr representativeVariableExpr = new VariableExpr(representativeVariable);
        representativeVertexBindings.add(new CorrLetClause(bindingExpression, representativeVariableExpr, null));
    }

    public void addRepresentativeEdgeBinding(VarIdentifier representativeVariable, Expression bindingExpression) {
        VariableExpr representativeVariableExpr = new VariableExpr(representativeVariable);
        representativeEdgeBindings.add(new CorrLetClause(bindingExpression, representativeVariableExpr, null));
    }

    public AbstractBinaryCorrelateClause popMainClauseSequence() {
        return mainCorrelatedClauseSequence.removeFirst();
    }

    public ListIterator<AbstractBinaryCorrelateClause> getMainIterator() {
        return mainCorrelatedClauseSequence.listIterator();
    }

    public List<CorrLetClause> getRepresentativeVertexBindings() {
        return representativeVertexBindings;
    }

    public List<CorrLetClause> getRepresentativeEdgeBindings() {
        return representativeEdgeBindings;
    }

    @Override
    public Iterator<AbstractBinaryCorrelateClause> iterator() {
        List<AbstractBinaryCorrelateClause> correlateClauses = new ArrayList<>();
        correlateClauses.addAll(mainCorrelatedClauseSequence);
        correlateClauses.addAll(representativeVertexBindings);
        correlateClauses.addAll(representativeEdgeBindings);
        correlateClauses.addAll(deferredCorrelatedClauseSequence);
        return correlateClauses.listIterator();
    }
}
