/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.updater.strategy;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A strategy that limits the number of instances selected by the subclass using variable steps.
 *
 * @param <T> Instance type.
 */
public class VariableBatchStrategy<T extends Comparable<T>> implements UpdateStrategy<T> {
  private final Ordering<T> ordering;
  protected final ImmutableList<Integer> maxActiveGroups;
  private final boolean rollingForward;
  private Optional<Integer> instanceModCount;

  private static final Logger LOG = LoggerFactory.getLogger(VariableBatchStrategy.class);

  /**
   * Creates a variable active-limited strategy that applies an upper bound to all results.
   *
   * @param maxActiveGroups  List of Maximum group sizes. Each group size represents a step.
   * {@link #getNextGroup(Set, Set)}.
   */
  public VariableBatchStrategy(Ordering<T> ordering,
      List<Integer> maxActiveGroups,
      boolean rollingForward) {

    this.ordering = Objects.requireNonNull(ordering);
    this.rollingForward = rollingForward;

    maxActiveGroups.forEach(x -> Preconditions.checkArgument(x > 0));

    this.maxActiveGroups = ImmutableList.copyOf(maxActiveGroups);
  }

  private int determineStep(int idle) {

    // Calculate which step we are in by finding out how many instances we have left to update.
    int scheduled = instanceModCount.get() - idle;

    int step = 0;
    int sum = 0;

    LOG.info("Update progress {} changed, {} idle, and {} total to be changed.",
        scheduled,
        idle,
        instanceModCount.get());

    if (rollingForward) {
      while (sum < scheduled && step < maxActiveGroups.size()) {
        sum += maxActiveGroups.get(step);

        ++step;
      }
    } else {

      // Starting with the first step as we're now comparing to idle instead of scheduled
      // to work backwards from the last step that was executed in the batch steps.
      sum = maxActiveGroups.get(step);

      // TODO(rdelvalle): Consider if it's necessary to handle fractional steps.
      // (i.e. halfway between step X and X+1)
      while (sum < idle) {
        ++step;

        if (step == maxActiveGroups.size()) {
          break;
        }

        sum += maxActiveGroups.get(step);
      }
    }

    // Cap at last step in case final instance count is greater than the sum of all steps.
    return Math.min(step, maxActiveGroups.size() - 1);
  }

  @Override
  public final Set<T> getNextGroup(Set<T> idle, Set<T> active) {

    // Get the size for the idle set on the first run only. This is representative of the number
    // of overall instance modifications this update will trigger.
    if (!instanceModCount.isPresent()) {
      instanceModCount = Optional.of(idle.size());
    }

    return ordering.sortedCopy(doGetNextGroup(idle, active)).stream()
            .limit(Math.max(0, maxActiveGroups.get(determineStep(idle.size())) - active.size()))
            .collect(Collectors.toSet());
  }

  /**
   * Return a list of instances to be updated.
   * Returns an empty list if the current active group has not completed.
   * If the result is larger than the current group size in {@link #maxActiveGroups},
   * it will be truncated.
   *
   * @param idle Idle instances, candidate for being updated.
   * @param active Instances currently being updated.
   * @return A subset of {@code idle}, instances to start updating.
   */
  Set<T> doGetNextGroup(Set<T> idle, Set<T> active) {
    if (active.isEmpty()) {
      return idle;
    } else {
      return ImmutableSet.of();
    }
  }
}
