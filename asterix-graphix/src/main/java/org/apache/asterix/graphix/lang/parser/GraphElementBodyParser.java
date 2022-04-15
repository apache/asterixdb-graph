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
package org.apache.asterix.graphix.lang.parser;

import java.io.StringReader;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.statement.GraphElementDecl;
import org.apache.asterix.graphix.metadata.entity.schema.Element;
import org.apache.hyracks.api.exceptions.IWarningCollector;

public final class GraphElementBodyParser {
    // Just a wrapper for the parseGraphElementBody method.
    public static GraphElementDecl parse(Element element, GraphixParserFactory parserFactory,
            IWarningCollector warningCollector) throws CompilationException {
        GraphElementDecl graphElementDecl = null;
        for (String definition : element.getDefinitionBodies()) {
            // Parse our the definition.
            GraphixParser parser = (GraphixParser) parserFactory.createParser(new StringReader(definition));
            GraphElementDecl parsedElementDecl;
            try {
                parsedElementDecl = parser.parseGraphElementBody(element.getIdentifier());

            } catch (CompilationException e) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR,
                        "Bad definition for a graph element: " + e.getMessage());
            }

            // Accumulate the element bodies.
            if (graphElementDecl == null) {
                graphElementDecl = parsedElementDecl;

            } else {
                graphElementDecl.getBodies().add(parsedElementDecl.getBodies().get(0));
            }

            // Gather any warnings.
            if (warningCollector != null) {
                parser.getWarnings(warningCollector);
            }
        }
        return graphElementDecl;
    }
}
