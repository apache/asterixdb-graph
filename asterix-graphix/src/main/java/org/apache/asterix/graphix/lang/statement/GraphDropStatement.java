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
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.lang.util.GraphStatementHandlingUtil;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Statement for removing a {@link org.apache.asterix.graphix.lang.expression.GraphConstructor} instance from our
 * metadata.
 * <ul>
 *  <li>A DROP GRAPH statement MUST always include a graph name.</li>
 *  <li>We can specify "DROP ... IF EXISTS" to drop the graph if it exists, and not raise an error if the graph
 *  doesn't exist,</li>
 * </ul>
 */
public class GraphDropStatement extends ExtensionStatement {
    private final DataverseName dataverseName;
    private final String graphName;
    private final boolean ifExists;

    public GraphDropStatement(DataverseName dataverseName, String graphName, boolean ifExists) {
        this.dataverseName = dataverseName;
        this.graphName = Objects.requireNonNull(graphName);
        this.ifExists = ifExists;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getGraphName() {
        return graphName;
    }

    public boolean getIfExists() {
        return ifExists;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public String getName() {
        return GraphDropStatement.class.getName();
    }

    @Override
    public void handle(IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            IRequestParameters requestParameters, MetadataProvider metadataProvider, int resultSetId) throws Exception {
        metadataProvider.validateDatabaseObjectName(dataverseName, graphName, this.getSourceLocation());
        DataverseName activeDataverseName = statementExecutor.getActiveDataverseName(this.dataverseName);
        GraphStatementHandlingUtil.acquireGraphExtensionWriteLocks(metadataProvider, activeDataverseName, graphName);
        try {
            GraphStatementHandlingUtil.handleGraphDrop(this, metadataProvider, activeDataverseName);

        } catch (Exception e) {
            QueryTranslator.abort(e, e, metadataProvider.getMetadataTxnContext());
            throw HyracksDataException.create(e);

        } finally {
            metadataProvider.getLocks().unlock();
        }
    }
}
