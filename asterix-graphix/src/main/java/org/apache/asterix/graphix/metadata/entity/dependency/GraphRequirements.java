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
package org.apache.asterix.graphix.metadata.entity.dependency;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.om.base.AGeneratedUUID;
import org.apache.hyracks.algebricks.common.utils.Triple;

/**
 * A collection of dependencies (datasets, synonyms, functions, and graphs) associated with a
 * {@link org.apache.asterix.graphix.metadata.entity.schema.Graph} instance.
 */
public class GraphRequirements implements IEntityRequirements {
    private static final long serialVersionUID = 1L;

    // A graph potentially depends on datasets, synonyms, functions, and graphs.
    private final Set<DependencyIdentifier> graphRequirements;
    private final DataverseName dataverseName;
    private final String graphName;

    // Physically, our requirements are indexed by the string below. Logically, we ignore this.
    private String primaryKeyValue;

    public GraphRequirements(DataverseName dataverseName, String graphName) throws IOException {
        this.graphRequirements = new HashSet<>();
        this.dataverseName = Objects.requireNonNull(dataverseName);
        this.graphName = Objects.requireNonNull(graphName);

        // Generate a unique primary key from a AUUID.
        StringBuilder sb = new StringBuilder();
        new AGeneratedUUID().appendLiteralOnly(sb);
        this.primaryKeyValue = sb.toString();
    }

    public GraphRequirements(DataverseName dataverseName, String graphName, Set<DependencyIdentifier> graphRequirements,
            String primaryKeyValue) {
        this.graphRequirements = Objects.requireNonNull(graphRequirements);
        this.dataverseName = Objects.requireNonNull(dataverseName);
        this.graphName = Objects.requireNonNull(graphName);
        this.primaryKeyValue = Objects.requireNonNull(primaryKeyValue);
    }

    public void loadNonGraphDependencies(Expression body, IQueryRewriter queryRewriter) throws CompilationException {
        // Collect our dependencies as triples.
        List<Triple<DataverseName, String, String>> datasetDependencies = new ArrayList<>();
        List<Triple<DataverseName, String, String>> synonymDependencies = new ArrayList<>();
        List<Triple<DataverseName, String, String>> functionDependencies = new ArrayList<>();
        ExpressionUtils.collectDependencies(body, queryRewriter, datasetDependencies, synonymDependencies,
                functionDependencies);

        // Transform and load into our requirements list.
        datasetDependencies.stream()
                .map(t -> new DependencyIdentifier(t.first, t.second, DependencyIdentifier.Kind.DATASET))
                .forEach(graphRequirements::add);
        synonymDependencies.stream()
                .map(t -> new DependencyIdentifier(t.first, t.second, DependencyIdentifier.Kind.SYNONYM))
                .forEach(graphRequirements::add);
        functionDependencies.stream()
                .map(t -> new DependencyIdentifier(t.first, t.second, t.third, DependencyIdentifier.Kind.FUNCTION))
                .forEach(graphRequirements::add);
    }

    public void loadGraphDependencies(Collection<DependencyIdentifier> graphDependencies) {
        graphRequirements.addAll(graphDependencies);
    }

    public void setPrimaryKeyValue(String primaryKeyValue) {
        this.primaryKeyValue = primaryKeyValue;
    }

    @Override
    public String getPrimaryKeyValue() {
        return primaryKeyValue;
    }

    @Override
    public DataverseName getDataverseName() {
        return dataverseName;
    }

    @Override
    public String getEntityName() {
        return graphName;
    }

    @Override
    public String getDisplayName() {
        return MetadataUtil.getFullyQualifiedDisplayName(dataverseName, graphName);
    }

    @Override
    public DependentKind getDependentKind() {
        return DependentKind.GRAPH;
    }

    @Override
    public Iterator<DependencyIdentifier> iterator() {
        return graphRequirements.iterator();
    }
}
