package xyz.kovacs.jduppur;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.sun.management.OperatingSystemMXBean;

import xyz.kovacs.jduppur.Crawler.CrawlerLogger;

public class jDupPur {

	public static final DecimalFormat PERCENT = new DecimalFormat("00.0000%");

	private static final Logger LOG = LogManager.getLogger(jDupPur.class);

	public static void main(String[] args) throws Exception {
		Cli.parse(args);
		Cli.printOptions();

		final Thread footprintLoggerThread = new Thread(
				new FootprintLogger(Cli.getInterval(), Crawler.initLogger(Cli.getInterval())));
		footprintLoggerThread.setDaemon(true);
		footprintLoggerThread.start();

		if (Cli.createList()) {
			if (!(Cli.checkDuplicates() || Cli.writeOutput())) {
				throw new IllegalArgumentException(
						"When creating a list, it either must be used to check for duplicates ('d') or to write an output ('o')");
			}

			final Set<String> fileList = Crawler.list(Cli.getInput());
			LOG.info("{} files listed recursively in {}", fileList.size(), Cli.getInput());
			final long start = System.nanoTime();
			final Map<String, List<String>> index = Crawler.index(fileList, Cli.getDigest(), Cli.getParallel());
			final long end = System.nanoTime();
			LOG.info("{} files indexed in {}", index.size(), humanReadableTime(end - start));
			if (LOG.isTraceEnabled()) {
				final SortedMap<String, List<String>> sorted = new TreeMap<>(index);
				for (final Entry<String, List<String>> entry : sorted.entrySet()) {
					LOG.trace("{} : {}", entry.getKey(), entry.getValue());
				}
			}

			if (Cli.checkDuplicates()) {
				final List<Entry<String, List<String>>> duplicates = index.entrySet()
						.parallelStream()
						.filter(e -> e.getValue().size() > 1)
						.collect(Collectors.toList());
				for (final Entry<String, List<String>> candidate : duplicates) {
					final List<String> candidateList = candidate.getValue().stream().sorted().collect(Collectors.toList());
					final Iterator<String> candidateListOuterIterator = candidateList.iterator();
					while (candidateListOuterIterator.hasNext()) {
						final String outerCandidate = candidateListOuterIterator.next();
						final Iterator<String> candidateListInnerIterator = candidateList.iterator();
						while (candidateListInnerIterator.hasNext()) {
							final String innerCandidate = candidateListInnerIterator.next();
							if (innerCandidate.equals(outerCandidate)) {
								continue;
							}
							if (FileUtils.contentEquals(new File(outerCandidate), new File(innerCandidate))) {
								LOG.warn(
										"Duplicate found on hash {}:" + System.getProperty("line.separator")
												+ "\tfileA: {}" + System.getProperty("line.separator") + "\tfileB: {}",
										candidate.getKey(), outerCandidate, innerCandidate);
								candidateListInnerIterator.remove();
							} else {
								LOG.fatal(
										"You got a hash collision, how awesome is that? 😁 Hash: {}"
												+ System.getProperty("line.separator") + "\tfileA: {}"
												+ System.getProperty("line.separator") + "\tfileB: {}",
										candidate.getKey(), outerCandidate, innerCandidate);
								candidateListInnerIterator.remove();
							}
						}
					}
				}
			}
			
			if (Cli.writeOutput()) {
				final List<String> output = new ArrayList<>(index.size());
				for (final Entry<String, List<String>> entry : index.entrySet()) {
					for (final String fileName : entry.getValue()) {
						output.add(entry.getKey() + " *" + fileName.replaceAll("\\\\", "/"));
					}
				}
				Collections.sort(output);
				FileUtils.writeLines(new File(Cli.getOutput()), output);
			}
		}

		footprintLoggerThread.interrupt();
	}

	public static String humanReadableTime(final long durationInNanos) {
		final long milliseconds = TimeUnit.MILLISECONDS.convert(durationInNanos, TimeUnit.NANOSECONDS);
		final StringBuilder builder = new StringBuilder();

		// add hours
		if (milliseconds >= 60L * 60L * 1000L) {
			builder.append(String.format("%02d", (milliseconds / 1000L / 60L / 60L)));
		} else {
			builder.append("00");
		}
		// add minutes
		if (milliseconds >= 60L * 1000L) {
			builder.append(":").append(String.format("%02d", (milliseconds / 1000L / 60L) % 60L));
		} else {
			builder.append(":00");
		}
		// add seconds
		if (milliseconds >= 1000L) {
			builder.append(":").append(String.format("%02d", (milliseconds / 1000L) % 60L));
		} else {
			builder.append(":00");
		}
		// add milliseconds
		builder.append(".").append(String.format("%03d", milliseconds % 1000L));

		return builder.toString();
	}

	private static final class FootprintLogger implements Runnable {

		private static final Logger LOG = LogManager.getLogger(FootprintLogger.class);

		private final long interval;
		private final CrawlerLogger crawlerLogger;

		private FootprintLogger(final long interval, final CrawlerLogger crawlerLogger) {
			this.interval = interval * 1000L;
			this.crawlerLogger = crawlerLogger;
		}

		@Override
		public void run() {
			final long start = System.nanoTime();

			if (!LOG.isDebugEnabled()) {
				return;
			}
			final OperatingSystemMXBean osBean;
			if (ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean) {
				osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
			} else {
				LOG.debug("Could not get OperatingSystemMXBean");
				return;
			}

			try {
				// in the first 10 seconds the numbers are way off... let's wait a bit
				Thread.sleep(10000L);
			} catch (InterruptedException e) {
				LOG.debug(FootprintLogger.class.getSimpleName() + " interrupted -> ending thread");
				return;
			}

			while (true) {
				try {
					Thread.sleep(interval);
					LOG.debug(
							"Current estimations for footprint are: RAM: {}, CPU%: {} (max on 1 thread ≈ {}), CPUΔt: {}, runtime: {}, I/O throughput: {}/s",
							FileUtils.byteCountToDisplaySize(
									Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()),
							PERCENT.format(osBean.getProcessCpuLoad()),
							PERCENT.format(1.0 / osBean.getAvailableProcessors()),
							jDupPur.humanReadableTime(osBean.getProcessCpuTime()),
							jDupPur.humanReadableTime(System.nanoTime() - start),
							FileUtils.byteCountToDisplaySize(
									(long) ((double) crawlerLogger.getProcessedBytes() / (double) TimeUnit.SECONDS
											.convert(System.nanoTime() - crawlerLogger.getStartedProcessingAt(),
													TimeUnit.NANOSECONDS))));
				} catch (InterruptedException e) {
					LOG.debug(FootprintLogger.class.getSimpleName() + " interrupted -> ending thread");
					return;
				}
			}
		}
	}
}
