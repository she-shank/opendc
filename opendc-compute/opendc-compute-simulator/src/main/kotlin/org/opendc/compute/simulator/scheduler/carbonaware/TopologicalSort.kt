/*
 * Copyright (c) 2025 AtLarge Research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.opendc.compute.simulator.scheduler.carbonaware

/**
 * Utility object for performing topological sort on task dependencies.
 *
 * Uses Kahn's algorithm to produce a valid ordering where all parent tasks
 * appear before their children.
 */
public object TopologicalSort {
    /**
     * Performs topological sort using Kahn's algorithm.
     *
     * @param taskCount Number of tasks to sort
     * @param parents Array where element at index [i] contains the indices of all parent tasks of task [i]
     * @return Array containing task indices in topological order (parents before children)
     * @throws IllegalArgumentException if a circular dependency is detected
     */
    public fun sort(
        taskCount: Int,
        parents: Array<IntArray>,
    ): IntArray {
        // Build in-degree array and children adjacency list
        val inDegree = IntArray(taskCount)
        val children = Array(taskCount) { mutableListOf<Int>() }

        for (i in 0 until taskCount) {
            for (parentIdx in parents[i]) {
                children[parentIdx].add(i)
                inDegree[i]++
            }
        }

        // Kahn's algorithm
        val queue = ArrayDeque<Int>()
        val result = IntArray(taskCount)
        var count = 0

        // Add all tasks with no dependencies to the queue
        for (i in 0 until taskCount) {
            if (inDegree[i] == 0) {
                queue.addLast(i)
            }
        }

        // Process tasks in topological order
        while (queue.isNotEmpty()) {
            val task = queue.removeFirst()
            result[count++] = task

            // Reduce in-degree for all children
            for (child in children[task]) {
                if (--inDegree[child] == 0) {
                    queue.addLast(child)
                }
            }
        }

        // Check for circular dependencies
        require(count == taskCount) {
            "Circular dependency detected in workflow: only $count of $taskCount tasks could be ordered"
        }

        return result
    }
}
