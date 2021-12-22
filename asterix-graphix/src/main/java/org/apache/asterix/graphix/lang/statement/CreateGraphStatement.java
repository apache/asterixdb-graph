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

import java.util.List;

import org.apache.asterix.algebra.extension.ExtensionStatement;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.lang.expression.GraphConstructor;
import org.apache.asterix.graphix.lang.rewrites.visitor.IGraphixLangVisitor;
import org.apache.asterix.graphix.lang.util.GraphStatementHandlingUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class CreateGraphStatement extends ExtensionStatement {
    private final GraphConstructor graphConstructor;
    private final DataverseName dataverseName;
    private final String graphName;
    private final boolean replaceIfExists;
    private final boolean ifNotExists;

    public CreateGraphStatement(DataverseName dataverseName, String graphName, boolean replaceIfExists,
            boolean ifNotExists, GraphConstructor graphConstructor) {
        this.dataverseName = dataverseName;
        this.graphName = graphName;
        this.replaceIfExists = replaceIfExists;
        this.ifNotExists = ifNotExists;
        this.graphConstructor = graphConstructor;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getGraphName() {
        return graphName;
    }

    public boolean isReplaceIfExists() {
        return replaceIfExists;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public List<GraphConstructor.VertexElement> getVertexElements() {
        return graphConstructor.getVertexElements();
    }

    public List<GraphConstructor.EdgeElement> getEdgeElements() {
        return graphConstructor.getEdgeElements();
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
        return CreateGraphStatement.class.getName();
    }

    @Override
    public void handle(IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            IRequestParameters requestParameters, MetadataProvider metadataProvider, int resultSetId) throws Exception {
        metadataProvider.validateDatabaseObjectName(dataverseName, graphName, this.getSourceLocation());
        DataverseName activeDataverseName = statementExecutor.getActiveDataverseName(this.dataverseName);
        GraphStatementHandlingUtil.acquireGraphWriteLocks(metadataProvider, activeDataverseName, graphName);
        try {
            GraphStatementHandlingUtil.handleCreateGraph(this, metadataProvider, statementExecutor,
                    activeDataverseName);

        } catch (Exception e) {
            QueryTranslator.abort(e, e, metadataProvider.getMetadataTxnContext());
            throw HyracksDataException.create(e);

        } finally {
            metadataProvider.getLocks().unlock();
        }
    }
}
