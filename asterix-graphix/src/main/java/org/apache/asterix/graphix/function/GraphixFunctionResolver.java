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
import java.util.function.BiFunction;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.util.CommonFunctionMapUtil;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * Resolve any functions found in {@link GraphixFunctionIdentifiers}. If any user-defined functions have the same name
 * as a Graphix function, we delegate resolution to the SQL++ resolver. If we find a system defined function, we also
 * defer resolution to the SQL++ resolver.
 */
public class GraphixFunctionResolver {
    private final BiFunction<String, Integer, FunctionSignature> builtInFunctionResolver;
    private final Map<FunctionSignature, FunctionDecl> declaredFunctionMap;
    private final MetadataProvider metadataProvider;

    public GraphixFunctionResolver(MetadataProvider metadataProvider,
            Map<FunctionSignature, FunctionDecl> declaredFunctionMap) {
        this.builtInFunctionResolver = FunctionUtil.createBuiltinFunctionResolver(metadataProvider);
        this.declaredFunctionMap = declaredFunctionMap;
        this.metadataProvider = metadataProvider;
    }

    public FunctionSignature resolve(CallExpr callExpr, boolean allowNonStoredUDFCalls) throws CompilationException {
        FunctionSignature functionSignature = callExpr.getFunctionSignature();
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
            FunctionSignature signatureWithVarArgs = new FunctionSignature(workingDataverseName,
                    functionSignature.getName(), FunctionIdentifier.VARARGS);
            if (functionDecl == null) {
                functionDecl = declaredFunctionMap.get(signatureWithVarArgs);
            }

            // We have found a function-declaration in our map. Return this...
            if (functionDecl != null) {
                if (!allowNonStoredUDFCalls && !functionDecl.isStored()) {
                    throw new CompilationException(ErrorCode.ILLEGAL_FUNCTION_USE, callExpr.getFunctionSignature(),
                            functionDecl.getSignature().toString());
                }
                return functionDecl.getSignature();
            }

            // ...otherwise, we need to search our metadata.
            try {
                Function function = metadataProvider.lookupUserDefinedFunction(signatureWithDataverse);
                if (function == null) {
                    function = metadataProvider.lookupUserDefinedFunction(signatureWithVarArgs);
                }
                if (function != null) {
                    return function.getSignature();
                }

            } catch (AlgebricksException e) {
                throw new CompilationException(ErrorCode.UNKNOWN_FUNCTION, e, callExpr.getSourceLocation(),
                        functionSignature.toString());
            }

            // If the dataverse was specified, we also need to make sure that this dataverse exists.
            if (functionSignature.getDataverseName() != null) {
                Dataverse dataverse;
                try {
                    dataverse = metadataProvider.findDataverse(functionSignature.getDataverseName());
                    if (dataverse == null) {
                        throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, callExpr.getSourceLocation(),
                                functionSignature.getDataverseName());
                    }

                } catch (AlgebricksException e) {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, e, callExpr.getSourceLocation(),
                            functionSignature.getDataverseName());
                }
            }
        }

        // We could not find a user-defined function. See if this is a Graphix-function call.
        String graphixName = functionSignature.getName().toLowerCase().replaceAll("_", "-");
        FunctionIdentifier graphixFunctionIdentifier = GraphixFunctionIdentifiers.getFunctionIdentifier(graphixName);
        if (graphixFunctionIdentifier == null) {
            graphixFunctionIdentifier = GraphixFunctionAliases.getFunctionIdentifier(graphixName);
        }
        if (graphixFunctionIdentifier != null) {
            return new FunctionSignature(graphixFunctionIdentifier);
        }

        // This is neither a Graphix function nor a user-defined function. Attempt to resolve to a built-in function.
        String builtInName = functionSignature.getName().toLowerCase();
        String mappedName = CommonFunctionMapUtil.getFunctionMapping(builtInName);
        if (mappedName != null) {
            builtInName = mappedName;
        }
        int functionArity = functionSignature.getArity();
        FunctionSignature builtInSignature = builtInFunctionResolver.apply(builtInName, functionArity);
        if (builtInSignature != null) {
            return builtInSignature;
        }

        // If we could not resolve this, try see if this is an aggregate function or window function.
        builtInSignature = new FunctionSignature(FunctionConstants.ASTERIX_DV, builtInName, functionArity);
        if (FunctionMapUtil.isSql92AggregateFunction(builtInSignature)
                || FunctionMapUtil.isCoreAggregateFunction(builtInSignature)
                || BuiltinFunctions.getWindowFunction(builtInSignature.createFunctionIdentifier()) != null) {
            return builtInSignature;
        }

        // ...otherwise, this is an unknown function.
        throw new CompilationException(ErrorCode.UNKNOWN_FUNCTION, callExpr.getSourceLocation(),
                functionSignature.toString());
    }
}
