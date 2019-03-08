package org.apache.aurora.scheduler.updater.strategy;

public interface AutoPauseEnabledStrategy {

  /**
   * Returns whether the strategy has auto paused enabled.
   *
   * @return True if strategy has auto pause enabled, false if disabled.
   */

  boolean autoPause();
}
