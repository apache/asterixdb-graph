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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.expression.GraphConstructor;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.AbstractExtensionClause;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;

/**
 * The logical starting AST node for Graphix queries. Lowering a Graphix AST involves setting the
 * {@link AbstractExtensionClause}, initially set to null. A FROM-GRAPH node includes the following:
 * <ul>
 *  <li>Either a {@link GraphConstructor} OR a [dataverse, graph name] pair. The former indicates that we are dealing
 *  with an anonymous graph, while the latter indicates that we must search our metadata for the graph.</li>
 *  <li>A list of {@link MatchClause} nodes, with a minimum size of one. The first MATCH node type must always be
 *  LEADING.</li>
 *  <li>A list of {@link AbstractBinaryCorrelateClause} nodes, which may be empty. These include UNNEST and explicit
 *  JOINs.</li>
 * </ul>
 */
public class FromGraphClause extends FromClause {
    // A non-lowered FROM-GRAPH-CLAUSE must either have a graph constructor...
    private final GraphConstructor graphConstructor;

    // Or a reference to a named graph (both cannot be specified).
    private final DataverseName dataverse;
    private final Identifier name;

    // Every non-lowered FROM-GRAPH-CLAUSE **MUST** include at-least a single MATCH clause.
    private final List<MatchClause> matchClauses;
    private final List<AbstractBinaryCorrelateClause> correlateClauses;

    // After lowering, we should have built an extension clause of some sort.
    private AbstractExtensionClause lowerClause = null;

    public FromGraphClause(DataverseName dataverse, Identifier name, List<MatchClause> matchClauses,
            List<AbstractBinaryCorrelateClause> correlateClauses) {
        super(Collections.emptyList());
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
        super(Collections.emptyList());
        this.graphConstructor = Objects.requireNonNull(graphConstructor);
        this.dataverse = null;
        this.name = null;
        this.matchClauses = Objects.requireNonNull(matchClauses);
        this.correlateClauses = Objects.requireNonNull(correlateClauses);
        if (matchClauses.isEmpty()) {
            throw new IllegalArgumentException("FROM-MATCH requires at least one MATCH clause.");
        }
    }

    public FromGraphClause(AbstractExtensionClause lowerClause) {
        super(Collections.emptyList());
        this.lowerClause = Objects.requireNonNull(lowerClause);
        this.graphConstructor = null;
        this.dataverse = null;
        this.name = null;
        this.matchClauses = Collections.emptyList();
        this.correlateClauses = Collections.emptyList();
    }

    public GraphIdentifier getGraphIdentifier(MetadataProvider metadataProvider) {
        DataverseName dataverseName = metadataProvider.getDefaultDataverseName();
        if (this.dataverse != null) {
            dataverseName = this.dataverse;
        }
        return (graphConstructor == null) ? new GraphIdentifier(dataverseName, name.getValue())
                : new GraphIdentifier(dataverseName, graphConstructor.getInstanceID());
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

    public AbstractExtensionClause getLowerClause() {
        return lowerClause;
    }

    public void setLowerClause(AbstractExtensionClause lowerClause) {
        this.lowerClause = lowerClause;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        if (visitor instanceof IGraphixLangVisitor) {
            return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);

        } else if (lowerClause != null) {
            return visitor.visit(lowerClause.getVisitorExtension(), arg);

        } else {
            return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
        }
    }

    @Override
    public String toString() {
        if (lowerClause != null) {
            return lowerClause.toString();

        } else if (graphConstructor != null) {
            return graphConstructor.toString();

        } else if (dataverse != null && name != null) {
            return dataverse + "." + name.getValue();

        } else if (dataverse == null && name != null) {
            return name.getValue();
        }
        throw new IllegalStateException();
    }

    @Override
    public int hashCode() {
        return Objects.hash(graphConstructor, dataverse, name, matchClauses, correlateClauses, lowerClause);
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
                && Objects.equals(correlateClauses, that.correlateClauses)
                && Objects.equals(lowerClause, that.lowerClause);
    }
}
