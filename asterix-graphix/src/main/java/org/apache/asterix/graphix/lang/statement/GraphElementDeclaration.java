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

import java.util.Objects;

import org.apache.asterix.algebra.extension.ExtensionStatement;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.common.metadata.IElementIdentifier;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.api.client.IHyracksClientConnection;

/**
 * A declaration for a single graph element (vertex or edge), which cannot be explicitly specified by the user. We
 * use this class to store the directly parsed AST and a normalized AST for the bodies themselves.
 */
public final class GraphElementDeclaration extends ExtensionStatement {
    private final IElementIdentifier identifier;
    private final Expression rawBody;
    private Expression normalizedBody;

    public GraphElementDeclaration(IElementIdentifier identifier, Expression rawBody) {
        this.identifier = Objects.requireNonNull(identifier);
        this.rawBody = Objects.requireNonNull(rawBody);
    }

    public IElementIdentifier getIdentifier() {
        return identifier;
    }

    public GraphIdentifier getGraphIdentifier() {
        return identifier.getGraphIdentifier();
    }

    public Expression getRawBody() {
        return rawBody;
    }

    public Expression getNormalizedBody() {
        return normalizedBody;
    }

    public void setNormalizedBody(Expression normalizedBody) {
        this.normalizedBody = normalizedBody;
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
        return GraphElementDeclaration.class.getName();
    }

    @Override
    public void handle(IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            IRequestParameters requestParameters, MetadataProvider metadataProvider, int resultSetId) throws Exception {
        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, getSourceLocation(),
                "Handling a GraphElementDeclaration (this should not be possible).");
    }
}
