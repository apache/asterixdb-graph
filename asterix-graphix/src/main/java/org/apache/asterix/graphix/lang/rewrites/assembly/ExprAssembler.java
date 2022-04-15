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
package org.apache.asterix.graphix.lang.rewrites.assembly;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import org.apache.asterix.common.exceptions.CompilationException;

/**
 * A generic assembler for building a list of T instances, where incoming instances take the last T instance as input.
 */
public class ExprAssembler<T> implements Iterable<T> {
    private final Deque<T> assemblyQueue = new ArrayDeque<>();

    public void bind(IExprAssembly<T> assembly) throws CompilationException {
        if (assemblyQueue.isEmpty()) {
            assemblyQueue.addLast(assembly.apply(null));

        } else {
            T currentLastAssembly = assemblyQueue.getLast();
            assemblyQueue.add(assembly.apply(currentLastAssembly));

        }
    }

    public T getLast() {
        return assemblyQueue.getLast();
    }

    @Override
    public Iterator<T> iterator() {
        return assemblyQueue.iterator();
    }
}
