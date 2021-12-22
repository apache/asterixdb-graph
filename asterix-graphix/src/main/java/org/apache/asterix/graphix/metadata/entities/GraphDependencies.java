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
package org.apache.asterix.graphix.metadata.entities;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.metadata.entities.DependencyKind;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;

/**
 * Helper class to manage dependencies for a graph metadata entity.
 */
public class GraphDependencies {
    private final List<Triple<DataverseName, String, String>> datasetDependencies;
    private final List<Triple<DataverseName, String, String>> synonymDependencies;
    private final List<Triple<DataverseName, String, String>> functionDependencies;

    public GraphDependencies(List<List<Triple<DataverseName, String, String>>> listRepresentation) {
        datasetDependencies = listRepresentation.get(0);
        synonymDependencies = listRepresentation.get(1);
        functionDependencies = listRepresentation.get(2);
    }

    public GraphDependencies() {
        datasetDependencies = new ArrayList<>();
        synonymDependencies = new ArrayList<>();
        functionDependencies = new ArrayList<>();
    }

    public List<List<Triple<DataverseName, String, String>>> getListRepresentation() {
        return List.of(datasetDependencies, synonymDependencies, functionDependencies);
    }

    public Iterator<Pair<DependencyKind, Triple<DataverseName, String, String>>> getIterator() {
        List<Pair<DependencyKind, Triple<DataverseName, String, String>>> resultant = new ArrayList<>();
        for (Triple<DataverseName, String, String> datasetDependency : datasetDependencies) {
            resultant.add(new Pair<>(DependencyKind.DATASET, datasetDependency));
        }
        for (Triple<DataverseName, String, String> synonymDependency : synonymDependencies) {
            resultant.add(new Pair<>(DependencyKind.SYNONYM, synonymDependency));
        }
        for (Triple<DataverseName, String, String> functionDependency : functionDependencies) {
            resultant.add(new Pair<>(DependencyKind.FUNCTION, functionDependency));
        }
        return resultant.listIterator();
    }

    public void collectDependencies(Expression expression, IQueryRewriter queryRewriter) throws CompilationException {
        ExpressionUtils.collectDependencies(expression, queryRewriter, datasetDependencies, synonymDependencies,
                functionDependencies);
    }
}
