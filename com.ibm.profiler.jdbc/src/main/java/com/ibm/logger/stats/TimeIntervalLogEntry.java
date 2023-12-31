/*
 * Copyright 2017 Steve McDuff
 * 
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
package com.ibm.logger.stats;

import java.util.concurrent.ConcurrentHashMap;

import com.ibm.commerce.cache.CacheUtilities;
import com.ibm.commerce.cache.IOperationMetric;
import com.ibm.commerce.cache.OperationStatistics;
import com.ibm.commerce.cache.TimeIntervalStatisticsAggregator;

public class TimeIntervalLogEntry extends AbstractLogEntry {




	
	/**
	 * Capture statistics for the last minute of execution of the operation in
	 * intervals of 5 seconds. Keep 14 intervals in memory for query purposes.
	 */
	private TimeIntervalStatisticsAggregator statistics;

	private long intervalDuration;

	private boolean skipLastInterval = true;
	
	private String intervalName = "1m";
	
	private static final ConcurrentHashMap<Long, String> NAME_CACHE = new ConcurrentHashMap<Long, String>();

	/**
	 * Constructor that builds a log entry.
	 * 
	 * @param id
	 *            the initial id for the object to build
	 * @param intervalDurationInNano
	 *            the interval duration in nanoseconds.
	 */
	public TimeIntervalLogEntry(String id, long intervalDurationInNano) {
		super(id);
		intervalDuration = intervalDurationInNano;
		initializeName();
		initializeStatistics();
	}



	private void initializeName() {
		intervalName = NAME_CACHE.get(intervalDuration);
		if( intervalName  == null ){
			intervalName = CacheUtilities.getDurationShortText(intervalDuration);
			NAME_CACHE.put(intervalDuration, intervalName);
		}
	}



	private void initializeStatistics() {
		// less than 1 second

		int timeFragmentCount;

		long timeFragmentDuration;

		if (intervalDuration <= 1000000000) {
			// capture the full interval in a single fragment, use 2 additional
			// fragments as buffer.
			timeFragmentCount = 3;
			timeFragmentDuration = intervalDuration;
		} else {
			// split the time interval in 10 fragments, use 2 addition fragments
			// for buffer.
			timeFragmentCount = 12;
			timeFragmentDuration = intervalDuration / 10;
		}

		statistics = new TimeIntervalStatisticsAggregator(timeFragmentDuration,
				timeFragmentCount);
	}

	@Override
    public void addValue(IOperationMetric metric) {
		long stopTime = metric.getStopTime();
		OperationStatistics intervalStatistics = statistics
				.getInterval(stopTime);
		intervalStatistics.logStatistic(metric);
	}

	/**
	 * @param processed
	 *            the time it took to process the request
	 * */
	@Override
    public void addValue(long processed, int responseSize,
			boolean cacheEnabled, boolean cacheHit, boolean failed) {
		statistics.getCurrentInterval().logStatistic(processed,
				responseSize, cacheEnabled, cacheHit, !failed);
	}

	/**
	 * Clear out active logger entry values
	 * */
	@Override
	public void clear() {
		// nothing to do.
	}

	@Override
	public OperationStatistics getStatistics() {
		return statistics.getStatistics(intervalDuration, skipLastInterval);
	}

	public OperationStatistics getStatistics(boolean skipLastInterval) {
		return statistics.getStatistics(intervalDuration, skipLastInterval);
	}
	
	@Override
    public long getIntervalDuration() {
		return intervalDuration;
	}

	public boolean isSkipLastInterval() {
		return skipLastInterval;
	}

	public void setSkipLastInterval(boolean skipLastInterval) {
		this.skipLastInterval = skipLastInterval;
	}
	
	/**
	 * 
	 * @return the internal statistics tracker.
	 */
	protected TimeIntervalStatisticsAggregator getInternalStatistics() {
		return statistics;
	}

	@Override
	public String getIntervalName() {
		return intervalName;
	}

}
