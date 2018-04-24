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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import java.util.stream.Collectors;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore.Mutable;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.Storage.Util;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;

/**
 * A strategy that limits the number of instances selected by the subclass using variable steps.
 *
 * @param <T> Instance type.
 */
public class VariableBatchStrategy<T extends Comparable<T>> implements UpdateStrategy<T> {
  private final Ordering<T> ordering;
  protected final ImmutableList<Integer> maxActive;
  private final boolean rollingForward;

  private final int totalInstanceCount;

  /**
   * Creates an variable active-limited strategy that applies an upper bound to all results.
   *
   * @param maxActive  List of Maximum number of values to return from. Each represents a step.
   * {@link #getNextGroup(Set, Set)}.
   */
  public VariableBatchStrategy(Ordering<T> ordering, List<Integer> maxActive, boolean rollingForward) {
    this.ordering = Objects.requireNonNull(ordering);
    this.rollingForward = rollingForward;

    maxActive.forEach(x -> Preconditions.checkArgument(x > 0));

    int acc = 0;
    for (int step : maxActive) {
      Preconditions.checkArgument(step > 0);
      acc += step;
    }
    this.totalInstanceCount = acc;
    this.maxActive = ImmutableList.copyOf(maxActive);
  }


  private final int determineStep(int idle){

    // Calculate which step where in by finding out how many instances we have left to update
    int scheduled = totalInstanceCount - idle;

    int step = 0;
    int sum = 0;

    if (rollingForward) {
      System.out.printf("Currently there are %d scheduled, %d idle, and %d total.\n", scheduled, idle, totalInstanceCount);

      while (sum < scheduled) {
        sum += maxActive.get(step);
        ++step;
      }
    } else {
      System.out.printf("Currently there are %d scheduled, %d idle, and %d total.\n", scheduled, idle, totalInstanceCount);

      // Starting with the first step as we're now comparing to idle instead of scheduled
      // to work backwards from the last step that was executed in the batch steps.
      sum = maxActive.get(step);

      // TODO(rdelvalle): Consider if it's necessary to handle fractional steps.
      while (sum < idle) {
        ++step;
        sum += maxActive.get(step);
      }
    }

    return Math.min(step, maxActive.size()-1);
  }

  @Override
  public final Set<T> getNextGroup(Set<T> idle, Set<T> active) {

    return ordering.sortedCopy(doGetNextGroup(idle, active)).stream()
            .limit(Math.max(0, maxActive.get(determineStep(idle.size())) - active.size()))
            .collect(Collectors.toSet());
  }

  /**
   * Return a list of instances to be updated. If the result is larger than {@link #maxActive},
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
