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
package org.apache.asterix.graphix.lang.statement;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.algebra.extension.ExtensionStatement;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.rewrites.visitor.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.api.client.IHyracksClientConnection;

/**
 * A declaration for a single graph element (vertex or edge), which cannot be explicitly specified by the user.
 * - This is analogous to {@link org.apache.asterix.lang.common.statement.ViewDecl} for views and
 * {@link org.apache.asterix.lang.common.statement.FunctionDecl} for functions, in that we use this class to store the
 * directly parsed AST and a normalized AST for the bodies themselves.
 * - Unlike views and functions, a single graph element may have more than one body. Graph element declarations start
 * off with one body, and it is up to the caller to manage multiple bodies.
 */
public final class GraphElementDecl extends ExtensionStatement {
    private final GraphElementIdentifier identifier;
    private final List<Expression> bodies = new ArrayList<>();
    private final List<Expression> normalizedBodies = new ArrayList<>();

    public GraphElementDecl(GraphElementIdentifier identifier, Expression body) {
        this.identifier = Objects.requireNonNull(identifier);
        this.bodies.add(Objects.requireNonNull(body));
    }

    public GraphElementIdentifier getIdentifier() {
        return identifier;
    }

    public GraphIdentifier getGraphIdentifier() {
        return identifier.getGraphIdentifier();
    }

    public List<Expression> getBodies() {
        return bodies;
    }

    public List<Expression> getNormalizedBodies() {
        return normalizedBodies;
    }

    @Override
    public byte getCategory() {
        return Category.QUERY;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public String getName() {
        return GraphElementDecl.class.getName();
    }

    @Override
    public void handle(IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            IRequestParameters requestParameters, MetadataProvider metadataProvider, int resultSetId) throws Exception {
        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, getSourceLocation(),
                "Handling a GraphElementDecl (this should not be possible).");
    }
}
