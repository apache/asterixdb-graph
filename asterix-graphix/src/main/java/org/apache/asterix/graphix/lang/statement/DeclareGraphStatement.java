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
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.app.translator.GraphixQueryTranslator;
import org.apache.asterix.graphix.lang.expression.GraphConstructor;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.api.client.IHyracksClientConnection;

/**
 * Statement for storing a {@link GraphConstructor} instance in our <i>context</i> instead of our metadata.
 */
public class DeclareGraphStatement extends ExtensionStatement {
    private final GraphConstructor graphConstructor;
    private final String graphName;

    // Our dataverse name is taken from the active declared dataverse.
    private DataverseName dataverseName;

    public DeclareGraphStatement(String graphName, GraphConstructor graphConstructor) {
        this.graphName = Objects.requireNonNull(graphName);
        this.graphConstructor = Objects.requireNonNull(graphConstructor);
    }

    public String getGraphName() {
        return graphName;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public GraphConstructor getGraphConstructor() {
        return graphConstructor;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    @Override
    public String getName() {
        return DeclareGraphStatement.class.getName();
    }

    @Override
    public void handle(IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            IRequestParameters requestParameters, MetadataProvider metadataProvider, int resultSetId) throws Exception {
        this.dataverseName = statementExecutor.getActiveDataverseName(null);
        metadataProvider.validateDatabaseObjectName(dataverseName, graphName, this.getSourceLocation());
        GraphixQueryTranslator graphixQueryTranslator = (GraphixQueryTranslator) statementExecutor;
        graphixQueryTranslator.addDeclaredGraph(this);
    }
}
