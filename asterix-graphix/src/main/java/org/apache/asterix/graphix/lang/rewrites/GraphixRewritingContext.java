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

import java.util.Map;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.api.exceptions.IWarningCollector;

/**
 * Wrapper class for {@link org.apache.asterix.lang.common.rewrites.LangRewritingContext}. We are particularly
 * interested in creating our own system-generated variables (distinct from those generated in the SQL++ rewrites).
 */
public class GraphixRewritingContext {
    private final LangRewritingContext langRewritingContext;

    public GraphixRewritingContext(LangRewritingContext langRewritingContext) {
        this.langRewritingContext = langRewritingContext;
    }

    public MetadataProvider getMetadataProvider() {
        return langRewritingContext.getMetadataProvider();
    }

    public IWarningCollector getWarningCollector() {
        return langRewritingContext.getWarningCollector();
    }

    public LangRewritingContext getLangRewritingContext() {
        return langRewritingContext;
    }

    public Map<FunctionSignature, FunctionDecl> getDeclaredFunctions() {
        return langRewritingContext.getDeclaredFunctions();
    }

    public VarIdentifier getNewVariable() {
        VarIdentifier langRewriteGeneratedVar = langRewritingContext.newVariable();
        String graphixGeneratedName = "#GG_" + langRewriteGeneratedVar.getValue().substring(1);
        return new VarIdentifier(graphixGeneratedName, langRewriteGeneratedVar.getId());
    }
}
