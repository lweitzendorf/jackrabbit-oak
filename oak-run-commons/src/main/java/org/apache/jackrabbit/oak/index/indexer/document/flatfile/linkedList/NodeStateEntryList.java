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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.linkedList;

import java.util.Iterator;

import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.jetbrains.annotations.NotNull;

public interface NodeStateEntryList {

    /**
     * Add an item at the tail of the list.
     */
    void add(@NotNull NodeStateEntry item);

    /**
     * Remove the first item from the list.
     *
     * @return the removed item
     */
    NodeStateEntry remove();

    long estimatedMemoryUsage();

    int size();

    /**
     * Get an iterator to iterate over the whole list
     */
    Iterator<NodeStateEntry> iterator();

    boolean isEmpty();

    void close();

}
