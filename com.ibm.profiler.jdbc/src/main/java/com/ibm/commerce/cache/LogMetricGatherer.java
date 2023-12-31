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
package com.ibm.commerce.cache;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ibm.logger.PerformanceLogger;

/**
 * Gather performance metrics within a java logger output.
 */
public class LogMetricGatherer extends AbstractLogMetricGatherer {

	/**
	 * default log level
	 */
	private static final Level DEFAULT_LOG_LEVEL = Level.FINE;

	/**
	 * default entry log level
	 */
	private static final Level DEFAULT_ENTRY_LOG_LEVEL = Level.FINER;

	/**
	 * logger to use
	 */
	private Logger logger;

	/**
	 * log level to use
	 */
	private Level level = DEFAULT_LOG_LEVEL;

	/**
	 * log level to use
	 */
	private Level entryLevel = DEFAULT_ENTRY_LOG_LEVEL;

	/**
	 * Constructor
	 * 
	 * @param setLogger
	 *            the logger to use
	 */
	public LogMetricGatherer(Logger setLogger) {
		logger = setLogger;
	}

	/**
	 * Empty constructor
	 */
	public LogMetricGatherer() {

	}

	/**
	 * get the log level.
	 * 
	 * @return the log level.
	 */
	public Level getLevel() {
		return level;
	}

	/**
	 * set the log level.
	 * 
	 * @param level
	 *            the log level.
	 */
	public void setLevel(Level level) {
		this.level = level;
	}

	/**
	 * Get the entry metric log level
	 * 
	 * @return The entry metric log level
	 */
	public Level getEntryLevel() {
		return entryLevel;
	}

	/**
	 * Set the new entry log level.
	 * 
	 * @param entryLevel
	 *            The new entry log level.
	 */
	public void setEntryLevel(Level entryLevel) {
		this.entryLevel = entryLevel;
	}

	/**
	 * get the logger.
	 * 
	 * @return the logger.
	 */
	public Logger getLogger() {
		return logger;
	}

	/**
	 * set the logger.
	 * 
	 * @param logger
	 *            the logger.
	 */
	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ibm.commerce.cache.MetricGatherer#gatherMetric(com.ibm.commerce.cache
	 * .OperationMetric)
	 */
	@Override
	public void gatherMetric(OperationMetric metric) {
		logMetricToLogger(metric, logger, level);
	}
	
    public void gatherMetric(OperationMetric metric, boolean printProperties)
    {
        logMetricToLogger(metric, logger, level, null, printProperties);
    }

	/**
	 * gather entry log
	 * 
	 * @param metric
	 *            metric to log
	 */
	@Override
    public void gatherMetricEntryLog(OperationMetric metric) {
		if (logger.isLoggable(entryLevel)) {
			logEntryLogMetricToLogger(metric, logger, entryLevel);
		}
	}

	/**
	 * log information into logger
	 * 
	 * @param level
	 *            level needed to log
	 * @param message
	 *            message to log
	 * @param properties
	 *            properties to log
	 */
	public void gatherInformationLog(Level level, String message,
			Map<String, String> properties) {
		if (logger.isLoggable(level)) {
		    
		    String logMessage = formatInformationLog(message, properties);
		    
			if( logMessage != null ) {
				this.logger.logp(level, "", "", logMessage);
			}
		}
	}

	/**
	 * Log a metric to a logger
	 * 
	 * @param metric
	 *            the metric to log
	 * @param currentLogger
	 *            the logger to use
	 * @param logLevel
	 *            the log level to use, if null, the default level of FINE will
	 *            be used.
	 */
	public static void logMetricToLogger(OperationMetric metric,
			Logger currentLogger, Level logLevel) {
		logMetricToLogger(metric, currentLogger, logLevel, null);
	}

	   /**
     * Log a metric to a logger
     * 
     * @param metric
     *            the metric to log
     * @param currentLogger
     *            the logger to use
     * @param logLevel
     *            the log level to use, if null, the default level of FINE will
     *            be used.
     * @param returnValue
     *            The return value to print in the logs.
     */
    public static void logMetricToLogger(OperationMetric metric,
            Logger currentLogger, Level logLevel, Object returnValue) {
        logMetricToLogger(metric, currentLogger, logLevel, returnValue, false);
    }
	
	/**
	 * Log a metric to a logger
	 * 
	 * @param metric
	 *            the metric to log
	 * @param currentLogger
	 *            the logger to use
	 * @param logLevel
	 *            the log level to use, if null, the default level of FINE will
	 *            be used.
	 * @param printProperties 
	 *            print properties if set to true.
	 * @param returnValue
	 *            The return value to print in the logs.
	 */
	public static void logMetricToLogger(OperationMetric metric,
			Logger currentLogger, Level logLevel, Object returnValue, boolean printProperties) {
		if (currentLogger == null) {
			return;
		}
		if (metric == null) {
			return;
		}
		if (logLevel == null) {
			logLevel = DEFAULT_LOG_LEVEL;
		}

		if (currentLogger.isLoggable(logLevel)) {
			String logMessage = formatMetricLog(metric, returnValue, printProperties);

			if( logMessage != null ) {
			    currentLogger.logp(logLevel, "", "", logMessage);
			}
		}

		PerformanceLogger.increase(metric);
	}

	/**
	 * log entry log to logger
	 * 
	 * @param metric
	 *            the metric to log
	 * @param currentLogger
	 *            current logger
	 * @param logLevel
	 *            the level to log
	 */
	public static void logEntryLogMetricToLogger(OperationMetric metric,
			Logger currentLogger, Level logLevel) {
		if (currentLogger == null) {
			return;
		}
		if (metric == null) {
			return;
		}
		if (logLevel == null) {
			logLevel = DEFAULT_LOG_LEVEL;
		}

		// pass true to write for Entrylog, pass false to write for Exitlog
		currentLogger.logp(logLevel, "", "",
				OperationMetric.writeEntryExitLog(metric, true));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ibm.commerce.cache.MetricGatherer#start()
	 */
	@Override
	public void start() {
		// nothing to do here.
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ibm.commerce.cache.MetricGatherer#stop()
	 */
	@Override
	public void stop() {
		// nothing to do here.
	}

	/**
	 * Can the performance metric be logged.
	 * 
	 * @return true if the performance metric can be logged.
	 * @deprecated Use the {@link com.ibm.commerce.cache.MetricGatherer#isEnabled()} method instead.
	 */
    public boolean isLoggable() {
		return isEnabled();
	}

    @Override
    public boolean isEnabled()
    {
        if (logger == null) {
            return false;
        }
        return logger.isLoggable(level)
                || PerformanceLogger.isEnabled();
    }
    
    @Override
    public boolean isEnabled(String marker)
    {
        return isEnabled() && logger.isLoggable(Level.FINEST);
    }

}
