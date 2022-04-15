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
package org.apache.asterix.graphix.function;

import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * Resolve any functions found in {@link GraphixFunctionIdentifiers}. If any user-defined functions have the same name
 * as a Graphix function, we defer resolution to the SQL++ rewriter. If we find a system defined function, we also
 * defer resolution to the SQL++ rewriter.
 */
public class GraphixFunctionResolver {
    private final Map<FunctionSignature, FunctionDecl> declaredFunctionMap;
    private final MetadataProvider metadataProvider;

    public GraphixFunctionResolver(MetadataProvider metadataProvider,
            Map<FunctionSignature, FunctionDecl> declaredFunctionMap) {
        this.declaredFunctionMap = declaredFunctionMap;
        this.metadataProvider = metadataProvider;
    }

    public FunctionSignature resolve(FunctionSignature functionSignature) throws CompilationException {
        DataverseName workingDataverseName = functionSignature.getDataverseName();
        if (workingDataverseName == null) {
            workingDataverseName = metadataProvider.getDefaultDataverseName();
        }

        // Attempt to **find** if a user-defined function exists. We do not resolve these calls here.
        if (!workingDataverseName.equals(FunctionConstants.ASTERIX_DV)
                && !workingDataverseName.equals(FunctionConstants.ALGEBRICKS_DV)
                && !workingDataverseName.equals(GraphixFunctionIdentifiers.GRAPHIX_DV)) {
            FunctionDecl functionDecl;

            // First, try resolve the call with the given number of arguments.
            FunctionSignature signatureWithDataverse = functionSignature;
            if (functionSignature.getDataverseName() == null) {
                signatureWithDataverse = new FunctionSignature(workingDataverseName, functionSignature.getName(),
                        functionSignature.getArity());
            }
            functionDecl = declaredFunctionMap.get(signatureWithDataverse);

            // If this has failed, retry with a variable number of arguments.
            if (functionDecl == null) {
                FunctionSignature signatureWithVarArgs = new FunctionSignature(workingDataverseName,
                        functionSignature.getName(), FunctionIdentifier.VARARGS);
                functionDecl = declaredFunctionMap.get(signatureWithVarArgs);
            }

            if (functionDecl != null) {
                return null;
            }
        }

        // We could not find a declared user-defined function. See if this is a Graphix-function call.
        String functionName = functionSignature.getName().toLowerCase().replaceAll("_", "-");
        FunctionIdentifier graphixFunctionIdentifier = GraphixFunctionIdentifiers.getFunctionIdentifier(functionName);
        if (graphixFunctionIdentifier == null) {
            graphixFunctionIdentifier = GraphixFunctionAliases.getFunctionIdentifier(functionName);
        }
        if (graphixFunctionIdentifier != null) {
            return new FunctionSignature(graphixFunctionIdentifier);
        }

        // This is either a SQL++ built-in function or an error. Defer this to the SQL++ rewrites.
        return null;
    }
}
