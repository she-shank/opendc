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

package org.opendc.compute.simulator.scheduler

import org.opendc.compute.simulator.scheduler.filters.HostFilter
import org.opendc.compute.simulator.scheduler.weights.HostWeigher
import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask
import org.opendc.simulator.compute.power.CarbonModel
import org.opendc.simulator.compute.power.CarbonReceiver
import java.time.InstantSource
import java.util.SplittableRandom
import java.util.random.RandomGenerator
import kotlin.math.max
import kotlin.math.min
import kotlin.math.roundToInt

/**
 * Hybrid Priority-Based Carbon-Aware Workflow Scheduler.
 *
 * Combines carbon-aware scheduling with workflow dependency awareness:
 * - Delays deferrable tasks during high carbon periods
 * - Tracks task relationships to avoid delaying critical tasks
 * - Uses slack thresholds to ensure deadline safety
 * - Prioritizes tasks with many dependents
 *
 * Strategy:
 * 1. Track task metadata (when first seen, relationship indicators)
 * 2. Calculate slack buffer based on task priority
 * 3. Apply conservative delays for tasks with many pending tasks in queue
 * 4. Use carbon forecast to make delay decisions
 *
 * @param filters List of host filters to apply
 * @param weighers List of host weighers for scoring hosts
 * @param clock Clock for tracking time
 * @param carbonDelayThreshold Percentile threshold for "high carbon" (default: 0.2 = 20th percentile)
 * @param maxDelayHours Maximum hours to delay a task for carbon benefit (default: 4 hours)
 * @param forecastHorizon Number of hours to look ahead in carbon forecast (default: 24 hours)
 * @param subsetSize Number of hosts to consider after weighing
 * @param random Random number generator
 * @param slackThresholdMultiplier Multiplier for slack safety margin (default: 2.0 = 2x safety)
 * @param prioritizeCriticalPath Whether to be conservative with early tasks (default: true)
 */
public class CarbonAwareWorkflowScheduler(
    private val filters: List<HostFilter>,
    private val weighers: List<HostWeigher>,
    private val clock: InstantSource,
    private val carbonDelayThreshold: Double = 0.2,
    private val maxDelayHours: Int = 4,
    private val forecastHorizon: Int = 24,
    private val subsetSize: Int = 1,
    private val random: RandomGenerator = SplittableRandom(0),
    private val slackThresholdMultiplier: Double = 2.0,
    private val prioritizeCriticalPath: Boolean = true,
) : ComputeScheduler, CarbonReceiver {
    private val hosts = mutableListOf<HostView>()
    private var carbonModel: CarbonModel? = null
    private var lowCarbon: Boolean = false
    private var currentCarbonIntensity: Double = 0.0 // Track current carbon intensity

    private var tasksDelayed = 0
    private var tasksScheduled = 0
    private var criticalPathSkips = 0
    private var slackSkips = 0

    // Task metadata tracking
    private data class TaskMetadata(
        val firstSeen: Long,
        var queuePosition: Int = 0,
    )

    private val taskMetadata = mutableMapOf<Int, TaskMetadata>()

    init {
        require(subsetSize >= 1) { "Subset size must be one or greater" }
        require(slackThresholdMultiplier >= 1.0) { "Slack threshold multiplier must be >= 1.0" }
    }

    override fun addHost(host: HostView) {
        hosts.add(host)
    }

    override fun removeHost(host: HostView) {
        hosts.remove(host)
    }

    override fun updateHost(hostView: HostView) {
        // No-op
    }

    override fun setHostEmpty(hostView: HostView) {
        // No-op
    }

    override fun select(iter: MutableIterator<SchedulingRequest>): SchedulingResult {
        var result: SchedulingResult? = null
        var queuePos = 0

        for (req in iter) {
            if (req.isCancelled) {
                iter.remove()
                continue
            }

            val task = req.task as? ServiceTask ?: continue

            // Track this task
            updateTaskMetadata(task, queuePos)
            queuePos++

            // Workflow-aware delay decision
            if (shouldDelayWorkflowAware(task)) {
                tasksDelayed++
                continue
            }

            // Schedule this task
            val filteredHosts = hosts.filter { host -> filters.all { filter -> filter.test(host, task) } }

            val subset =
                if (weighers.isNotEmpty()) {
                    val filterResults = weighers.map { it.getWeights(filteredHosts, task) }
                    val weights = DoubleArray(filteredHosts.size)

                    for (fr in filterResults) {
                        val min = fr.min
                        val range = (fr.max - min)

                        if (range == 0.0) {
                            continue
                        }

                        val multiplier = fr.multiplier
                        val factor = multiplier / range

                        for ((i, weight) in fr.weights.withIndex()) {
                            weights[i] += factor * (weight - min)
                        }
                    }

                    weights.indices
                        .asSequence()
                        .sortedByDescending { weights[it] }
                        .map { filteredHosts[it] }
                        .take(subsetSize)
                        .toList()
                } else {
                    filteredHosts
                }

            val maxSize = min(subsetSize, subset.size)
            if (maxSize == 0) {
                result = SchedulingResult(SchedulingResultType.FAILURE, null, req)
                break
            } else {
                iter.remove()
                tasksScheduled++
                result = SchedulingResult(SchedulingResultType.SUCCESS, subset[random.nextInt(maxSize)], req)
                break
            }
        }

        if (result == null) return SchedulingResult(SchedulingResultType.EMPTY)

        return result
    }

    /**
     * Update metadata for task tracking.
     */
    private fun updateTaskMetadata(
        task: ServiceTask,
        queuePosition: Int,
    ) {
        if (!taskMetadata.containsKey(task.id)) {
            taskMetadata.put(
                task.id,
                TaskMetadata(
                    firstSeen = clock.millis(),
                    queuePosition = queuePosition,
                ),
            )
        } else {
            taskMetadata.get(task.id)!!.queuePosition = queuePosition
        }
    }

    /**
     * Workflow-aware delay decision logic with adaptive forecast horizon.
     *
     * Considers:
     * - Task deferrability
     * - Carbon intensity regime (adaptive to task deadline)
     * - Deadline slack with safety margin
     * - Queue position (early tasks likely more critical)
     * - Total tasks in system (busy periods need less delay)
     *
     * Enhanced: Uses adaptive forecast horizon based on task deadline.
     * If deadline is sooner than configured horizon, uses deadline instead.
     */
    private fun shouldDelayWorkflowAware(task: ServiceTask): Boolean {
        // Basic checks
        if (!task.deferrable) return false

        val currentTime = clock.millis()
        val slack = task.deadline - currentTime - task.duration

        // Calculate adaptive forecast horizon based on task deadline
        val timeUntilDeadlineMs = task.deadline - currentTime
        val timeUntilDeadlineHours = (timeUntilDeadlineMs / 3600000.0)

        // Use minimum of configured horizon and time until deadline
        // Also enforce minimum of 1 hour for stable percentile calculation
        val adaptiveHorizonHours =
            min(
                forecastHorizon.toDouble(),
                max(1.0, timeUntilDeadlineHours),
            ).toInt()

        // Get carbon forecast for adaptive horizon
        val forecast = carbonModel?.getForecast(adaptiveHorizonHours)
        if (forecast == null || forecast.isEmpty()) {
            // No forecast available, use global lowCarbon flag as fallback
            if (lowCarbon) return false
        } else {
            // Calculate task-specific carbon regime using adaptive horizon
            val localForecastSize = forecast.size
            val quantileIndex = (localForecastSize * carbonDelayThreshold).roundToInt()
            val carbonThreshold = forecast.sorted()[quantileIndex]

            // If carbon is already low relative to adaptive forecast, don't delay
            if (currentCarbonIntensity < carbonThreshold) {
                return false
            }
        }

        // Calculate minimum required slack with safety margin
        val baseSlackNeeded = maxDelayHours * 3600000L
        var minSlackNeeded = (baseSlackNeeded * slackThresholdMultiplier).toLong()

        // Workflow-aware adjustments
        if (prioritizeCriticalPath) {
            val metadata = taskMetadata.get(task.id)

            if (metadata != null) {
                // Early tasks in queue are likely on or near critical path
                // Require more slack for delaying them
                if (metadata.queuePosition < 5) {
                    minSlackNeeded = (minSlackNeeded * 1.5).toLong()

                    // If very early and not enough slack, skip delay
                    if (slack < minSlackNeeded) {
                        criticalPathSkips++
                        return false
                    }
                } // If many tasks waiting (busy period), be more conservative
                // This prevents cascading delays
                val queueDepth = taskMetadata.size
                if (queueDepth > 20) {
                    minSlackNeeded = (minSlackNeeded * 1.2).toLong()
                }
            }
        }

        // Final slack check
        if (slack < minSlackNeeded) {
            slackSkips++
            return false
        }

        // Safe to delay
        return true
    }

    override fun removeTask(
        task: ServiceTask,
        host: HostView?,
    ) {
        // Clean up metadata for completed tasks to prevent memory bloat
        taskMetadata.remove(task.id)
    }

    override fun setCarbonModel(carbonModel: CarbonModel?) {
        this.carbonModel = carbonModel
    }

    override fun removeCarbonModel(carbonModel: CarbonModel?) {
        if (this.carbonModel == carbonModel) {
            this.carbonModel = null
        }
    }

    override fun updateCarbonIntensity(newCarbonIntensity: Double) {
        // Store current carbon intensity for adaptive horizon calculations
        currentCarbonIntensity = newCarbonIntensity

        // Use forecast to determine if we're in low carbon regime (for fallback)
        val forecast = carbonModel?.getForecast(forecastHorizon) ?: return

        if (forecast.isEmpty()) {
            lowCarbon = false
            return
        }

        val localForecastSize = forecast.size
        val quantileIndex = (localForecastSize * carbonDelayThreshold).roundToInt()
        val carbonThreshold = forecast.sorted()[quantileIndex]

        lowCarbon = newCarbonIntensity < carbonThreshold
    }
}
