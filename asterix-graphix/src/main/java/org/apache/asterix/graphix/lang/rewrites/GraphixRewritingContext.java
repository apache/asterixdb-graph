/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.graphix.lang.rewrites;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.statement.DeclareGraphStatement;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.ViewDecl;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.api.exceptions.IWarningCollector;

/**
 * Wrapper class for {@link LangRewritingContext} and for Graphix specific rewriting.
 */
public class GraphixRewritingContext extends LangRewritingContext {
    private final Map<GraphIdentifier, DeclareGraphStatement> declaredGraphs = new HashMap<>();

    public GraphixRewritingContext(MetadataProvider metadataProvider, List<FunctionDecl> declaredFunctions,
            List<ViewDecl> declaredViews, Set<DeclareGraphStatement> declareGraphStatements,
            IWarningCollector warningCollector, int varCounter) {
        super(metadataProvider, declaredFunctions, declaredViews, warningCollector, varCounter);
        declareGraphStatements.forEach(d -> {
            GraphIdentifier graphIdentifier = new GraphIdentifier(d.getDataverseName(), d.getGraphName());
            this.declaredGraphs.put(graphIdentifier, d);
        });
    }

    public Map<GraphIdentifier, DeclareGraphStatement> getDeclaredGraphs() {
        return declaredGraphs;
    }

    public VarIdentifier getNewGraphixVariable() {
        VarIdentifier langRewriteGeneratedVar = newVariable();
        String graphixGeneratedName = "#GG_" + langRewriteGeneratedVar.getValue().substring(1);
        return new VarIdentifier(graphixGeneratedName, langRewriteGeneratedVar.getId());
    }

    public static boolean isGraphixVariable(VarIdentifier varIdentifier) {
        return varIdentifier.getValue().startsWith("#GG_");
    }
}
