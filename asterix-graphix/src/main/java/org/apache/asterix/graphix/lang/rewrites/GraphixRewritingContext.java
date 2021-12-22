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
package org.apache.asterix.graphix.lang.rewrites;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.lang.statement.GraphElementDecl;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.ViewDecl;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.api.exceptions.IWarningCollector;

public class GraphixRewritingContext {
    private final Map<GraphElementIdentifier, GraphElementDecl> graphElementDeclMap = new HashMap<>();
    private final LangRewritingContext langRewritingContext;

    public GraphixRewritingContext(MetadataProvider metadataProvider, List<FunctionDecl> declaredFunctions,
            List<ViewDecl> declaredViews, List<GraphElementDecl> declaredGraphElements,
            IWarningCollector warningCollector, int varCounter) {
        if (declaredGraphElements != null) {
            declaredGraphElements.forEach(e -> graphElementDeclMap.put(e.getIdentifier(), e));
        }
        langRewritingContext = new LangRewritingContext(metadataProvider, declaredFunctions, declaredViews,
                warningCollector, varCounter);
    }

    public Map<GraphElementIdentifier, GraphElementDecl> getDeclaredGraphElements() {
        return graphElementDeclMap;
    }

    public LangRewritingContext getLangRewritingContext() {
        return langRewritingContext;
    }

    public MetadataProvider getMetadataProvider() {
        return langRewritingContext.getMetadataProvider();
    }
}
