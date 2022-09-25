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
package org.apache.asterix.graphix.lang.rewrite.lower.struct;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.clause.LowerSwitchClause;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class ClauseCollection implements Iterable<AbstractClause> {
    private final List<LetClause> representativeVertexBindings = new ArrayList<>();
    private final List<LetClause> representativeEdgeBindings = new ArrayList<>();
    private final List<LetClause> representativePathBindings = new ArrayList<>();
    private final List<AbstractClause> nonRepresentativeClauses = new LinkedList<>();
    private final List<Pair<VariableExpr, LetClause>> eagerVertexBindings = new ArrayList<>();
    private final List<AbstractBinaryCorrelateClause> userCorrelateClauses = new ArrayList<>();
    private final SourceLocation sourceLocation;

    public ClauseCollection(SourceLocation sourceLocation) {
        this.sourceLocation = sourceLocation;
    }

    public void addVertexBinding(VariableExpr bindingVar, Expression boundExpression) throws CompilationException {
        if (representativeVertexBindings.stream().anyMatch(v -> v.getVarExpr().equals(bindingVar))) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Duplicate vertex binding found!");
        }
        representativeVertexBindings.add(new LetClause(bindingVar, boundExpression));
    }

    public void addEdgeBinding(VariableExpr bindingVar, Expression boundExpression) throws CompilationException {
        if (representativeEdgeBindings.stream().anyMatch(v -> v.getVarExpr().equals(bindingVar))) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Duplicate edge binding found!");
        }
        representativeEdgeBindings.add(new LetClause(bindingVar, boundExpression));
    }

    public void addPathBinding(VariableExpr bindingVar, Expression boundExpression) throws CompilationException {
        if (representativePathBindings.stream().anyMatch(v -> v.getVarExpr().equals(bindingVar))) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Duplicate path binding found!");
        }
        representativePathBindings.add(new LetClause(bindingVar, boundExpression));
    }

    @SuppressWarnings("fallthrough")
    public void addNonRepresentativeClause(AbstractClause abstractClause) throws CompilationException {
        switch (abstractClause.getClauseType()) {
            case JOIN_CLAUSE:
            case UNNEST_CLAUSE:
            case LET_CLAUSE:
            case WHERE_CLAUSE:
                nonRepresentativeClauses.add(abstractClause);
                break;

            case EXTENSION:
                if (abstractClause instanceof LowerSwitchClause) {
                    nonRepresentativeClauses.add(abstractClause);
                    break;
                }

            default:
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, abstractClause.getSourceLocation(),
                        "Illegal clause inserted into the lower clause list!");
        }
    }

    public void addEagerVertexBinding(VariableExpr representativeVariable, LetClause vertexBinding)
            throws CompilationException {
        // An eager vertex binding should be found in the non-representative clauses...
        boolean isIllegalVertexBinding = true;
        for (AbstractClause nonRepresentativeClause : nonRepresentativeClauses) {
            if (nonRepresentativeClause.getClauseType() != Clause.ClauseType.LET_CLAUSE) {
                continue;
            }
            LetClause nonRepresentativeLetClause = (LetClause) nonRepresentativeClause;
            if (nonRepresentativeLetClause.getBindingExpr().equals(vertexBinding.getBindingExpr())) {
                isIllegalVertexBinding = false;
                break;
            }
        }
        if (isIllegalVertexBinding) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Illegal eager vertex binding added!");
        }

        // ... and the representative vertex bindings.
        isIllegalVertexBinding = true;
        for (LetClause representativeVertexBinding : representativeVertexBindings) {
            if (representativeVertexBinding.getBindingExpr().equals(vertexBinding.getBindingExpr())
                    && representativeVariable.equals(representativeVertexBinding.getVarExpr())) {
                isIllegalVertexBinding = false;
                break;
            }
        }
        if (isIllegalVertexBinding) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Illegal eager vertex binding added!");
        }
        eagerVertexBindings.add(new Pair<>(representativeVariable, vertexBinding));
    }

    public void addUserDefinedCorrelateClause(AbstractBinaryCorrelateClause correlateClause) {
        userCorrelateClauses.add(correlateClause);
    }

    public List<LetClause> getRepresentativeVertexBindings() {
        return representativeVertexBindings;
    }

    public List<Pair<VariableExpr, LetClause>> getEagerVertexBindings() {
        return eagerVertexBindings;
    }

    public List<LetClause> getRepresentativeEdgeBindings() {
        return representativeEdgeBindings;
    }

    public List<LetClause> getRepresentativePathBindings() {
        return representativePathBindings;
    }

    public List<AbstractClause> getNonRepresentativeClauses() {
        return nonRepresentativeClauses;
    }

    public List<AbstractBinaryCorrelateClause> getUserDefinedCorrelateClauses() {
        return userCorrelateClauses;
    }

    public SourceLocation getSourceLocation() {
        return sourceLocation;
    }

    @Override
    public Iterator<AbstractClause> iterator() {
        Iterator<AbstractClause> nonRepresentativeIterator = nonRepresentativeClauses.iterator();
        Iterator<LetClause> representativeVertexIterator = representativeVertexBindings.iterator();
        Iterator<LetClause> representativeEdgeIterator = representativeEdgeBindings.iterator();
        Iterator<LetClause> representativePathIterator = representativePathBindings.iterator();
        Iterator<AbstractBinaryCorrelateClause> userCorrelateIterator = userCorrelateClauses.iterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return nonRepresentativeIterator.hasNext() || representativeVertexIterator.hasNext()
                        || representativeEdgeIterator.hasNext() || representativePathIterator.hasNext()
                        || userCorrelateIterator.hasNext();
            }

            @Override
            public AbstractClause next() {
                if (nonRepresentativeIterator.hasNext()) {
                    return nonRepresentativeIterator.next();
                }
                if (representativeVertexIterator.hasNext()) {
                    return representativeVertexIterator.next();
                }
                if (representativeEdgeIterator.hasNext()) {
                    return representativeEdgeIterator.next();
                }
                if (representativePathIterator.hasNext()) {
                    return representativePathIterator.next();
                }
                if (userCorrelateIterator.hasNext()) {
                    return userCorrelateIterator.next();
                }
                throw new IllegalStateException();
            }
        };
    }

    @Override
    public int hashCode() {
        return Objects.hash(nonRepresentativeClauses, representativeVertexBindings, representativeEdgeBindings,
                eagerVertexBindings, representativePathBindings, userCorrelateClauses, sourceLocation);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ClauseCollection)) {
            return false;
        }
        ClauseCollection that = (ClauseCollection) object;
        return Objects.equals(this.nonRepresentativeClauses, that.nonRepresentativeClauses)
                && Objects.equals(this.representativeVertexBindings, that.representativeVertexBindings)
                && Objects.equals(this.representativeEdgeBindings, that.representativeEdgeBindings)
                && Objects.equals(this.representativePathBindings, that.representativePathBindings)
                && Objects.equals(this.eagerVertexBindings, that.eagerVertexBindings)
                && Objects.equals(this.userCorrelateClauses, that.userCorrelateClauses)
                && Objects.equals(this.sourceLocation, that.sourceLocation);
    }

    @Override
    public String toString() {
        final Function<List<LetClause>, String> bindingPrinter = letClauses -> letClauses.stream()
                .map(b -> b.getVarExpr().getVar().toString()).collect(Collectors.joining(","));
        return String.format("clause-collection:\n\t%s\n\t%s\n\t%s\n",
                "vertices: " + bindingPrinter.apply(representativeVertexBindings),
                "edges: " + bindingPrinter.apply(representativeEdgeBindings),
                "paths: " + bindingPrinter.apply(representativePathBindings));
    }
}
