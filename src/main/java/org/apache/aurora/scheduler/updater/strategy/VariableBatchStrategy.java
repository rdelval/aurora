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
  private final Storage storage;
  private final IJobUpdateKey key;
  protected final ImmutableList<Integer> maxActive;
  private final boolean rollingForward;

  private final int totalInstanceCount;

  /**
   * Creates an variable active-limited strategy that applies an upper bound to all results.
   *
   * @param maxActive  List of Maximum number of values to return from. Each represents a step.
   * {@link #getNextGroup(Set, Set)}.
   */
  public VariableBatchStrategy(Ordering<T> ordering, List<Integer> maxActive, boolean rollingForward, Storage storage, IJobUpdateKey key) {
    this.ordering = Objects.requireNonNull(ordering);
    this.storage = Objects.requireNonNull(storage);
    this.key = Objects.requireNonNull(key);
    this.rollingForward = rollingForward;

    maxActive.forEach(x -> Preconditions.checkArgument(x > 0));


    int acc = 0;
    for (int step : maxActive) {
      Preconditions.checkArgument(step > 0);
      acc += step;
    }

    this.maxActive = ImmutableList.copyOf(maxActive);
    this.totalInstanceCount = acc;

  }

  @Override
  public final Set<T> getNextGroup(Set<T> idle, Set<T> active) {


    // Calculate which step where in by finding out how many instances we have left to update
    int scheduled = totalInstanceCount - idle.size();
    int pending = totalInstanceCount - scheduled;

    int step = 0;

    System.out.printf("Currently there are %d pending, %d scheduled,, %d active, %d idle, and %d total.\n", pending, scheduled, active.size(), idle.size(), totalInstanceCount);

    if (pending == 0) {
      step = maxActive.size()-1;
    } else {
      int sum = 0;
      while (sum < scheduled) {
        sum += maxActive.get(step);

        ++step;
      }
    }

    return ordering.sortedCopy(doGetNextGroup(idle, active)).stream()
            .limit(Math.max(0, maxActive.get(step) - active.size()))
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


    int total = idle.size() + active.size();

    System.out.printf("Currently there are %d active, %d idle, and %d total.\n", active.size(), idle.size(), total);

    if (active.isEmpty()) {

/*      if (this.rollingForward) {
        ++curStep;
      } else {
        --curStep;
      } */

      return idle;
    } else {
      return ImmutableSet.of();
    }
  }
}
