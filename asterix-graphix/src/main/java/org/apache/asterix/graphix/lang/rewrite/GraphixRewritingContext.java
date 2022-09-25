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
package org.apache.asterix.graphix.lang.rewrite;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.algebra.compiler.option.ElementEvaluationOption;
import org.apache.asterix.graphix.algebra.compiler.option.IGraphixCompilerOption;
import org.apache.asterix.graphix.algebra.compiler.option.SchemaDecorateEdgeOption;
import org.apache.asterix.graphix.algebra.compiler.option.SchemaDecorateVertexOption;
import org.apache.asterix.graphix.algebra.compiler.option.SemanticsNavigationOption;
import org.apache.asterix.graphix.algebra.compiler.option.SemanticsPatternOption;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.statement.DeclareGraphStatement;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.ViewDecl;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.api.exceptions.IWarningCollector;

/**
 * Wrapper class for {@link LangRewritingContext} and for Graphix specific rewriting.
 */
public class GraphixRewritingContext extends LangRewritingContext {
    private final Map<GraphIdentifier, DeclareGraphStatement> declaredGraphs = new HashMap<>();
    private final Map<String, Integer> uniqueCopyCounter = new HashMap<>();
    private final Map<String, String> configFileOptions;

    public GraphixRewritingContext(MetadataProvider metadataProvider, List<FunctionDecl> declaredFunctions,
            List<ViewDecl> declaredViews, Set<DeclareGraphStatement> declareGraphStatements,
            IWarningCollector warningCollector, int varCounter, Map<String, String> configFileOptions) {
        super(metadataProvider, declaredFunctions, declaredViews, warningCollector, varCounter);
        declareGraphStatements.forEach(d -> {
            GraphIdentifier graphIdentifier = new GraphIdentifier(d.getDataverseName(), d.getGraphName());
            this.declaredGraphs.put(graphIdentifier, d);
        });
        this.configFileOptions = configFileOptions;
    }

    public Map<GraphIdentifier, DeclareGraphStatement> getDeclaredGraphs() {
        return declaredGraphs;
    }

    public VariableExpr getGraphixVariableCopy(String existingIdentifierValue) {
        uniqueCopyCounter.put(existingIdentifierValue, uniqueCopyCounter.getOrDefault(existingIdentifierValue, 0) + 1);
        int currentCount = uniqueCopyCounter.get(existingIdentifierValue);
        String variableName = String.format("#GGVC(%s,%s)", existingIdentifierValue, currentCount);
        return new VariableExpr(new VarIdentifier(variableName));
    }

    public VariableExpr getGraphixVariableCopy(VariableExpr existingVariable) {
        VarIdentifier existingIdentifier = existingVariable.getVar();
        String variableName = SqlppVariableUtil.toUserDefinedVariableName(existingIdentifier).getValue();
        return getGraphixVariableCopy(variableName);
    }

    public IGraphixCompilerOption getSetting(String settingName) throws CompilationException {
        IGraphixCompilerOption[] enumValues;
        switch (settingName) {
            case ElementEvaluationOption.OPTION_KEY_NAME:
                enumValues = ElementEvaluationOption.values();
                return parseSetting(settingName, enumValues, ElementEvaluationOption.OPTION_DEFAULT);

            case SchemaDecorateEdgeOption.OPTION_KEY_NAME:
                enumValues = SchemaDecorateEdgeOption.values();
                return parseSetting(settingName, enumValues, SchemaDecorateEdgeOption.OPTION_DEFAULT);

            case SchemaDecorateVertexOption.OPTION_KEY_NAME:
                enumValues = SchemaDecorateVertexOption.values();
                return parseSetting(settingName, enumValues, SchemaDecorateVertexOption.OPTION_DEFAULT);

            case SemanticsNavigationOption.OPTION_KEY_NAME:
                enumValues = SemanticsNavigationOption.values();
                return parseSetting(settingName, enumValues, SemanticsNavigationOption.OPTION_DEFAULT);

            case SemanticsPatternOption.OPTION_KEY_NAME:
                enumValues = SemanticsPatternOption.values();
                return parseSetting(settingName, enumValues, SemanticsPatternOption.OPTION_DEFAULT);

            default:
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Illegal setting requested!");
        }
    }

    private IGraphixCompilerOption parseSetting(String settingName, IGraphixCompilerOption[] settingValues,
            IGraphixCompilerOption defaultValue) throws CompilationException {
        // Always check our metadata configuration first.
        Object metadataConfigValue = getMetadataProvider().getConfig().get(settingName);
        if (metadataConfigValue != null) {
            String configValueString = ((String) metadataConfigValue).toLowerCase(Locale.ROOT);
            return Stream.of(settingValues).filter(o -> o.getOptionValue().equals(configValueString)).findFirst()
                    .orElseThrow(() -> new CompilationException(ErrorCode.PARAMETER_NO_VALUE, configValueString));
        }

        // If our setting is not in metadata, check our config file.
        String configFileValue = configFileOptions.get(settingName);
        if (configFileValue != null) {
            return Stream.of(settingValues).filter(o -> o.getOptionValue().equals(configFileValue)).findFirst()
                    .orElseThrow(() -> new CompilationException(ErrorCode.PARAMETER_NO_VALUE, configFileValue));
        }

        // Otherwise, return our default value.
        return defaultValue;
    }
}
