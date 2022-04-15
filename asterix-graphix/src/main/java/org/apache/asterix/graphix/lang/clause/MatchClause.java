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
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.optype.MatchType;
import org.apache.asterix.graphix.lang.rewrites.visitor.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

/**
 * Container for a collection of {@link PathPatternExpr} nodes.
 * - A MATCH node has three types: LEADING (indicating that this node is first), INNER (indicating that this node is not
 * first, but all patterns must be matched), and LEFTOUTER (indicating that this node is optionally matched).
 * - Under isomorphism semantics, two patterns in different MATCH nodes (one pattern in a LEADING MATCH node and
 * one pattern in an INNER MATCH node) are equivalent to two patterns in a single LEADING MATCH node. See
 * {@link org.apache.asterix.graphix.lang.rewrites.lower.assembly.IsomorphismLowerAssembly} for more detail.
 */
public class MatchClause extends AbstractClause {
    private final List<PathPatternExpr> pathExpressions;
    private final MatchType matchType;

    public MatchClause(List<PathPatternExpr> pathExpressions, MatchType matchType) {
        this.pathExpressions = Objects.requireNonNull(pathExpressions);
        this.matchType = matchType;
    }

    public List<PathPatternExpr> getPathExpressions() {
        return pathExpressions;
    }

    public MatchType getMatchType() {
        return matchType;
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
        String pathString = pathExpressions.stream().map(PathPatternExpr::toString).collect(Collectors.joining("\n"));
        return matchType.toString() + " " + pathString;

    }
}
