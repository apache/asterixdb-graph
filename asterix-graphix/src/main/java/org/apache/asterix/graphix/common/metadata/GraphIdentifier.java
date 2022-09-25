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
package org.apache.asterix.graphix.common.metadata;

import java.io.Serializable;
import java.util.Objects;

import org.apache.asterix.common.metadata.DataverseName;

/**
 * A unique identifier for a graph. A graph is uniquely identified by:
 * <ul>
 *  <li>The dataverse associated with the graph. A graph identifier must always belong to some dataverse.</li>
 *  <li>The name of the graph. Anonymous graphs should have a name generated from its respective GRAPH-CONSTRUCTOR.</li>
 * </ul>
 */
public class GraphIdentifier implements Serializable {
    private static final long serialVersionUID = 1L;
    private final DataverseName dataverseName;
    private final String graphName;

    public GraphIdentifier(DataverseName dataverseName, String graphName) {
        this.dataverseName = Objects.requireNonNull(dataverseName);
        this.graphName = Objects.requireNonNull(graphName);
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getGraphName() {
        return graphName;
    }

    @Override
    public String toString() {
        return dataverseName + "." + graphName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof GraphIdentifier) {
            GraphIdentifier that = (GraphIdentifier) o;
            return Objects.equals(this.dataverseName, that.dataverseName) && Objects.equals(graphName, that.graphName);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataverseName, graphName);
    }
}
