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
package org.apache.asterix.graphix.lang.clause;

import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.lang.expression.GraphConstructor;
import org.apache.asterix.graphix.lang.rewrites.visitor.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;

/**
 * The logical starting AST node for Graphix queries. A FROM-GRAPH node includes the following:
 * - Either a {@link GraphConstructor} OR a [dataverse, graph name] pair. The former indicates that we are dealing with
 * an anonymous graph, while the latter indicates that we must search our metadata for the graph.
 * - A list of {@link MatchClause} nodes, with a minimum size of one. The first MATCH node type must always be LEADING.
 * - A list of {@link AbstractBinaryCorrelateClause} nodes, which may be empty. These include UNNEST and explicit JOINs.
 */
public class FromGraphClause extends AbstractClause {
    // A FROM-MATCH must either have a graph constructor...
    private final GraphConstructor graphConstructor;

    // Or a reference to a named graph (both cannot be specified).
    private final DataverseName dataverse;
    private final Identifier name;

    // Every FROM-MATCH **MUST** include at-least a single MATCH clause. Correlated clauses are optional.
    private final List<MatchClause> matchClauses;
    private final List<AbstractBinaryCorrelateClause> correlateClauses;

    public FromGraphClause(DataverseName dataverse, Identifier name, List<MatchClause> matchClauses,
            List<AbstractBinaryCorrelateClause> correlateClauses) {
        this.graphConstructor = null;
        this.dataverse = dataverse;
        this.name = Objects.requireNonNull(name);
        this.matchClauses = Objects.requireNonNull(matchClauses);
        this.correlateClauses = Objects.requireNonNull(correlateClauses);

        if (matchClauses.isEmpty()) {
            throw new IllegalArgumentException("FROM-MATCH requires at least one MATCH clause.");
        }
    }

    public FromGraphClause(GraphConstructor graphConstructor, List<MatchClause> matchClauses,
            List<AbstractBinaryCorrelateClause> correlateClauses) {
        this.graphConstructor = Objects.requireNonNull(graphConstructor);
        this.dataverse = null;
        this.name = null;
        this.matchClauses = Objects.requireNonNull(matchClauses);
        this.correlateClauses = Objects.requireNonNull(correlateClauses);

        if (matchClauses.isEmpty()) {
            throw new IllegalArgumentException("FROM-MATCH requires at least one MATCH clause.");
        }
    }

    public GraphConstructor getGraphConstructor() {
        return graphConstructor;
    }

    public DataverseName getDataverseName() {
        return dataverse;
    }

    public Identifier getGraphName() {
        return name;
    }

    public List<MatchClause> getMatchClauses() {
        return matchClauses;
    }

    public List<AbstractBinaryCorrelateClause> getCorrelateClauses() {
        return correlateClauses;
    }

    @Override
    public ClauseType getClauseType() {
        return null;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public String toString() {
        return (graphConstructor != null) ? graphConstructor.toString()
                : ((dataverse == null) ? name.getValue() : (dataverse + "." + name));
    }

    @Override
    public int hashCode() {
        return Objects.hash(graphConstructor, dataverse, name, matchClauses, correlateClauses);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof FromGraphClause)) {
            return false;
        }
        FromGraphClause that = (FromGraphClause) object;
        return Objects.equals(graphConstructor, that.graphConstructor) && Objects.equals(dataverse, that.dataverse)
                && Objects.equals(name, that.name) && matchClauses.equals(that.matchClauses)
                && Objects.equals(correlateClauses, that.correlateClauses);
    }
}
