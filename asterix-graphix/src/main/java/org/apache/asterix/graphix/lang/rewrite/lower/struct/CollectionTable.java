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
package org.apache.asterix.graphix.lang.rewrite.lower.struct;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class CollectionTable implements Iterable<ClauseCollection> {
    private final Map<ElementLabel, List<Entry>> collectionMap = new HashMap<>();
    private Map<ElementLabel, StateContainer> inputMap;
    private Map<ElementLabel, StateContainer> outputMap;

    public static class Entry {
        private final ElementLabel edgeLabel;
        private final ElementLabel destinationLabel;
        private final ClauseCollection clauseCollection;
        private final EdgeDescriptor.EdgeDirection edgeDirection;

        private Entry(ElementLabel edgeLabel, ElementLabel destinationLabel, ClauseCollection clauseCollection,
                EdgeDescriptor.EdgeDirection edgeDirection) {
            this.edgeLabel = edgeLabel;
            this.destinationLabel = destinationLabel;
            this.clauseCollection = clauseCollection;
            this.edgeDirection = edgeDirection;
        }

        public ElementLabel getEdgeLabel() {
            return edgeLabel;
        }

        public ElementLabel getDestinationLabel() {
            return destinationLabel;
        }

        public ClauseCollection getClauseCollection() {
            return clauseCollection;
        }

        public EdgeDescriptor.EdgeDirection getEdgeDirection() {
            return edgeDirection;
        }

        @Override
        public int hashCode() {
            return Objects.hash(edgeLabel, destinationLabel, clauseCollection, edgeDirection);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof Entry)) {
                return false;
            }
            Entry that = (Entry) object;
            return Objects.equals(this.edgeLabel, that.edgeLabel)
                    && Objects.equals(this.destinationLabel, that.destinationLabel)
                    && Objects.equals(this.clauseCollection, that.clauseCollection)
                    && Objects.equals(this.edgeDirection, that.edgeDirection);
        }
    }

    public void setInputMap(Map<ElementLabel, StateContainer> inputMap) {
        this.inputMap = inputMap;
    }

    public void setOutputMap(Map<ElementLabel, StateContainer> outputMap) {
        this.outputMap = outputMap;
    }

    public void putCollection(EdgePatternExpr edgePatternExpr, boolean isEvaluatingFromLeft,
            ClauseCollection clauseCollection) {
        // Determine our source and destination labels.
        ElementLabel startLabel, edgeLabel, endLabel;
        if (isEvaluatingFromLeft) {
            startLabel = edgePatternExpr.getLeftVertex().getLabels().iterator().next();
            endLabel = edgePatternExpr.getRightVertex().getLabels().iterator().next();

        } else {
            startLabel = edgePatternExpr.getRightVertex().getLabels().iterator().next();
            endLabel = edgePatternExpr.getLeftVertex().getLabels().iterator().next();
        }
        edgeLabel = edgePatternExpr.getEdgeDescriptor().getEdgeLabels().iterator().next();

        // Insert our collection.
        EdgeDescriptor.EdgeDirection edgeDirection = edgePatternExpr.getEdgeDescriptor().getEdgeDirection();
        putCollection(startLabel, edgeLabel, endLabel, clauseCollection, edgeDirection);
    }

    public void putCollection(ElementLabel startLabel, ElementLabel edgeLabel, ElementLabel endLabel,
            ClauseCollection clauseCollection, EdgeDescriptor.EdgeDirection edgeDirection) {
        if (!collectionMap.containsKey(startLabel)) {
            collectionMap.put(startLabel, new ArrayList<>());
        }
        collectionMap.get(startLabel).add(new Entry(edgeLabel, endLabel, clauseCollection, edgeDirection));
    }

    public Map<ElementLabel, StateContainer> getInputMap() {
        return inputMap;
    }

    public Map<ElementLabel, StateContainer> getOutputMap() {
        return outputMap;
    }

    public Iterator<Pair<ElementLabel, List<Entry>>> entryIterator() {
        return collectionMap.entrySet().stream().map(e -> new Pair<>(e.getKey(), e.getValue())).iterator();
    }

    @Override
    public Iterator<ClauseCollection> iterator() {
        return collectionMap.values().stream().flatMap(e -> e.stream().map(c -> c.clauseCollection)).iterator();
    }

    @Override
    public int hashCode() {
        return Objects.hash(collectionMap, inputMap, outputMap);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof CollectionTable)) {
            return false;
        }
        CollectionTable that = (CollectionTable) object;
        return Objects.equals(this.collectionMap, that.collectionMap) && Objects.equals(this.inputMap, that.inputMap)
                && Objects.equals(this.outputMap, that.outputMap);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("collection-table: \n");
        Iterator<Pair<ElementLabel, List<Entry>>> entryIterator = this.entryIterator();
        while (entryIterator.hasNext()) {
            Pair<ElementLabel, List<Entry>> mapEntry = entryIterator.next();
            for (Entry entry : mapEntry.second) {
                sb.append("\t").append(mapEntry.getFirst()).append(" to ");
                sb.append(entry.getDestinationLabel()).append(" [ ");
                sb.append(entry.getEdgeLabel()).append(" ]\n");
            }
        }
        return sb.toString();
    }
}
