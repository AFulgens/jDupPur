package xyz.kovacs.jduppur;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.sun.management.OperatingSystemMXBean;

import xyz.kovacs.jduppur.Crawler.CrawlerLogger;

public class jDupPur {

	public static final DecimalFormat PERCENT = new DecimalFormat("00.0000%");

	private static final Logger LOG = LogManager.getLogger(jDupPur.class);

	public static void main(final String[] args) throws Exception {
		Cli.parse(args);
		Cli.printOptions();

		final Thread footprintLoggerThread = new Thread(
				new FootprintLogger(Cli.getInterval(), Crawler.initLogger(Cli.getInterval())));
		footprintLoggerThread.setDaemon(true);
		footprintLoggerThread.start();

		if (Cli.createIndex()) {
			final Map<String, List<String>> index = createIndex();

			if (Cli.checkDuplicates()) {
				checkForDuplicates(index);
			}

			if (Cli.writeOutput()) {
				writeIndex(index);
			}
		} else if (Cli.check()) {
			final Map<String, List<String>> reIndex = reIndex();

			if (Cli.checkDuplicates()) {
				checkForDuplicates(reIndex);
			}
		} else if (Cli.createPurgatory()) {
			final Pair<Map<String, Pair<String, String>>, Map<String, Pair<String, String>>> purgatory = createPurgatory();
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

	private static Map<String, List<String>> createIndex() {
		if (!(Cli.checkDuplicates() || Cli.writeOutput())) {
			throw new IllegalArgumentException(
					"When creating a list, it either must be used to check for duplicates ('d') or to write an output ('o')");
		}

		final Set<String> fileList = Crawler.list(Cli.getInput(), Cli.getExcludes());
		LOG.info("{} files listed recursively in {}", fileList.size(), Cli.getInput());
		final long start = System.nanoTime();
		final Map<String, List<String>> index = Crawler.index(fileList, Cli.getDigest(), Cli.getParallel(),
				Cli.getExcludes());
		final long end = System.nanoTime();
		LOG.info("{} files indexed into {} hashes in {}", index.values().stream().flatMap(List::stream).count(),
				index.size(), humanReadableTime(end - start));
		if (LOG.isTraceEnabled()) {
			final SortedMap<String, List<String>> sorted = new TreeMap<>(index);
			for (final Entry<String, List<String>> entry : sorted.entrySet()) {
				LOG.trace("{} : {}", entry.getKey(), entry.getValue());
			}
		}
		return index;
	}

	private static Pair<Map<String, Pair<String, String>>, Map<String, Pair<String, String>>> createPurgatory() {
		Map<String, Pair<String, String>> directories = new HashMap<>();
		Map<String, Pair<String, String>> files = new HashMap<>();

		// TODO: do a generic implementation of checkForDuplicates(index, index) -> use
		// that to create a purgatory list and for checking duplicates in the same index

		return Pair.of(directories, files);
	}

	private static void checkForDuplicates(final Map<String, List<String>> index) throws IOException {
		final List<Pair<String, String>> duplicates = new ArrayList<>();
		final List<Entry<String, List<String>>> candidates = index.entrySet()
				.parallelStream()
				.filter(e -> e.getValue().size() > 1)
				.collect(Collectors.toList());
		for (final Entry<String, List<String>> candidate : candidates) {
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
						duplicates.add(Pair.of(outerCandidate, innerCandidate));
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
		List<Pair<String, String>> directories = new ArrayList<>();
		if (Cli.consolidateDirectories()) {
			List<String> primaries = duplicates.stream()
					.map(Pair::getLeft)
					.map(jDupPur::properAbsolutePath)
					.collect(Collectors.toList());
			List<String> secondaries = duplicates.stream()
					.map(Pair::getRight)
					.map(jDupPur::properAbsolutePath)
					.collect(Collectors.toList());
			primary: for (final String primary : primaries) {
				for (final Pair<String, String> directory : directories) {
					if (StringUtils.startsWithIgnoreCase(primary, directory.getLeft())) {
						continue primary;
					}
				}

				secondary: for (final String secondary : secondaries) {
					for (final Pair<String, String> directory : directories) {
						if (StringUtils.startsWithIgnoreCase(secondary, directory.getRight())) {
							continue secondary;
						}
					}

					if (StringUtils.substringAfterLast(primary, "/")
							.equals(StringUtils.substringAfterLast(secondary, "/"))) {
						final String primaryDirectory = StringUtils.substringBeforeLast(primary, "/");
						final Collection<String> primaryFiles = FileUtils
								.listFiles(new File(primaryDirectory), null, false)
								.stream()
								.map(File::getAbsolutePath)
								.map(jDupPur::properAbsolutePath)
								.collect(Collectors.toList());
						if (primaries.containsAll(primaryFiles)) {
							final String secondaryDirectory = StringUtils.substringBeforeLast(secondary, "/");
							final Collection<String> secondaryFiles = FileUtils
									.listFiles(new File(secondaryDirectory), null, false)
									.stream()
									.map(File::getAbsolutePath)
									.map(jDupPur::properAbsolutePath)
									.collect(Collectors.toList());
							if (secondaries.containsAll(secondaryFiles)) {
								directories.add(Pair.of(primaryDirectory, secondaryDirectory));
							}
						}
					}
				}
			}

			directories.stream().forEach(p -> {
				LOG.warn("Duplicate directories found:" + System.getProperty("line.separator") + "\tdir A: {}"
						+ System.getProperty("line.separator") + "\tdir B: {}", p.getLeft(), p.getRight());
			});
		}
		List<String> directoriesToSkip = directories.stream().map(Pair::getRight).collect(Collectors.toList());
		duplicates.stream().filter(p -> {
			for (final String directory : directoriesToSkip) {
				if (p.getRight().startsWith(directory)) {
					return false;
				}
			}
			return true;
		}).forEach(p -> {
			LOG.warn("Duplicate files found:" + System.getProperty("line.separator") + "\tfile A: {}"
					+ System.getProperty("line.separator") + "\tfile B: {}", p.getLeft(), p.getRight());
		});
	}

	private static void writeIndex(final Map<String, List<String>> index) throws IOException {
		final List<String> output = new ArrayList<>(index.size());
		LOG.info("Writing index into {}", Cli.getOutput());
		if (Cli.sort() == 0) {
			for (final Entry<String, List<String>> entry : index.entrySet()) {
				for (final String fileName : entry.getValue()) {
					output.add(entry.getKey() + " *" + properAbsolutePath(fileName));
				}
			}
		} else if (Cli.sort() == 1) {
			for (final Entry<String, List<String>> entry : index.entrySet()) {
				for (final String fileName : entry.getValue()) {
					output.add(entry.getKey() + " *" + properAbsolutePath(fileName));
				}
			}
			Collections.sort(output);
		} else if (Cli.sort() == 2) {
			final List<String> sorted = new ArrayList<>(index.size());
			for (final Entry<String, List<String>> entry : index.entrySet()) {
				for (final String fileName : entry.getValue()) {
					sorted.add(properAbsolutePath(fileName) + " *" + entry.getKey());
				}
			}
			Collections.sort(sorted);
			output.addAll(sorted.stream()
					.map(e -> StringUtils.splitByWholeSeparator(e, " *", 2))
					.map(e -> e[1] + " *" + e[0])
					.collect(Collectors.toList()));
		}
		FileUtils.writeLines(new File(Cli.getOutput()), StandardCharsets.UTF_8.name(), output);
		LOG.info("Index written into {}", Cli.getOutput());
	}

	private static Map<String, List<String>> reIndex() throws IOException {
		final List<String> input = FileUtils.readLines(new File(Cli.getInput()), StandardCharsets.UTF_8);
		LOG.info("{} files listed in index {}", input.size(), Cli.getInput());

		final long start = System.nanoTime();
		Map<String, List<String>> reIndex = Crawler.reIndex(input, Cli.getDigest(), Cli.getParallel(),
				Cli.getExcludes());
		final long end = System.nanoTime();
		LOG.info("{} files re-indexed in {}", input.size(), humanReadableTime(end - start));

		return reIndex;
	}

	public static <T> Stream<T> conditionallyParallel(final Stream<T> stream, final boolean makeParallel) {
		if (makeParallel) {
			return stream.parallel();
		} else {
			return stream;
		}
	}

	public static String properAbsolutePath(final String inproperAbsolutePath) {
		return RegExUtils.replaceAll(RegExUtils.replaceAll(inproperAbsolutePath, "\\\\", "/"), "/+", "/");
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
