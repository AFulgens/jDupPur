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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.SetUtils;
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
				writeIndex(index, Cli.getOutput());
			}
		} else if (Cli.check()) {
			final Map<String, List<String>> reIndex = reIndex();

			if (Cli.checkDuplicates()) {
				checkForDuplicates(reIndex);
			}
		} else if (Cli.createPurgatory()) {
			final Set<String> toPurge = createPurgatory();

			if (Cli.writeOutput()) {
				writePurgatory(toPurge);
			}
		} else if (Cli.updateIndex()) {
			if (Cli.writeOutput()) {
				updateIndex(Cli.getOutput());
			} else {
				updateIndex(Cli.getInput());
			}
		}

		footprintLoggerThread.interrupt();
	}

	public static <T> Stream<T> conditionallyParallel(final Stream<T> stream, final boolean makeParallel) {
		return makeParallel ? stream.parallel() : stream; // NOTE: we are not forcing sequential in case !makeParallel
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

	public static String properAbsolutePath(final String inproperAbsolutePath) {
		return RegExUtils.replaceAll(RegExUtils.replaceAll(inproperAbsolutePath, "\\\\", "/"), "/+", "/");
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

	private static void updateIndex(final String outputFileName) throws IOException {
		writeIndex(conditionallyParallel(readIndex(Cli.getInput()).entrySet().stream(), Cli.getParallel())
				.filter(e -> new File(e.getValue().get(0)).exists())
				.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())), outputFileName);
	}

	private static Set<String> createPurgatory() throws IOException {
		final String[] indexes = Cli.getInput().split("\\*");
		final Pair<List<Pair<String, String>>, List<Pair<String, String>>> purgatory = diffIndexes(
				readIndex(indexes[0]), readIndex(indexes[1]));

		if (LOG.isDebugEnabled()) {
			purgatory.getLeft().stream().forEach(p -> {
				LOG.debug("Duplicate directories found:" + System.getProperty("line.separator") + "\tdir A: {}"
						+ System.getProperty("line.separator") + "\tdir B: {}", p.getLeft(), p.getRight());
			});
			purgatory.getRight().stream().forEach(p -> {
				LOG.debug("Duplicate files found:" + System.getProperty("line.separator") + "\tfile A: {}"
						+ System.getProperty("line.separator") + "\tfile B: {}", p.getLeft(), p.getRight());
			});
		}

		final Set<String> toPurge = SetUtils.union(
				purgatory.getLeft()
						.stream()
						.map(d -> d.getRight())
						.map(d -> "(directory) " + d)
						.collect(Collectors.toSet()),
				purgatory.getRight()
						.stream()
						.map(f -> f.getRight())
						.map(f -> "(file) " + f)
						.collect(Collectors.toSet()));

		if (LOG.isInfoEnabled()) {
			toPurge.stream().forEach(p -> {
				LOG.info("To purge: {}", p);
			});
		}

		return toPurge;
	}

	private static void checkForDuplicates(final Map<String, List<String>> index) throws IOException {
		final Pair<List<Pair<String, String>>, List<Pair<String, String>>> diff = diffIndexes(index, index);

		diff.getLeft().stream().forEach(p -> {
			LOG.warn("Duplicate directories found:" + System.getProperty("line.separator") + "\tdir A: {}"
					+ System.getProperty("line.separator") + "\tdir B: {}", p.getLeft(), p.getRight());
		});
		diff.getRight().stream().forEach(p -> {
			LOG.warn("Duplicate files found:" + System.getProperty("line.separator") + "\tfile A: {}"
					+ System.getProperty("line.separator") + "\tfile B: {}", p.getLeft(), p.getRight());
		});
	}

	private static void writeIndex(final Map<String, List<String>> index, final String outputFileName)
			throws IOException {
		final List<String> output = new ArrayList<>(index.size());
		LOG.info("Writing index into {}", outputFileName);
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
		FileUtils.writeLines(new File(outputFileName), StandardCharsets.UTF_8.name(), output);
		LOG.info("Index written into {}", outputFileName);
	}

	private static void writePurgatory(final Set<String> toPurge) throws IOException {
		LOG.info("Writing purge list into {}", Cli.getOutput());

		FileUtils.writeLines(new File(Cli.getOutput()), StandardCharsets.UTF_8.name(), toPurge);

		LOG.info("Index written into {}", Cli.getOutput());
	}

	private static Map<String, List<String>> readIndex(final String indexFileName) throws IOException {
		final List<String> input = FileUtils.readLines(new File(indexFileName), StandardCharsets.UTF_8);
		LOG.info("{} files listed in index {}", input.size(), indexFileName);

		final Map<String, List<String>> index = new HashMap<>(input.size(), 1.0f);
		for (final String line : input) {
			final String[] entry = StringUtils.splitByWholeSeparator(line, " *", 2);
			index.computeIfAbsent(entry[0], k -> new ArrayList<>(1)).add(entry[1]);
		}

		return index;
	}

	private static Pair<List<Pair<String, String>>, List<Pair<String, String>>> diffIndexes(
			final Map<String, List<String>> primaryIndex, final Map<String, List<String>> purgatoryIndex)
			throws IOException {

		final List<Pair<String, String>> files = new ArrayList<>();

		LOG.debug("Starting to proces {} entries in primary index", primaryIndex.size());
		int counter = 0;

		for (final Entry<String, List<String>> primaryEntry : primaryIndex.entrySet()) {
			++counter;
			LOG.trace("At entry {} out of {}", counter, primaryIndex.size());
			if (counter % 1000 == 0) {
				LOG.debug("At entry {} out of {}", counter, primaryIndex.size());
			}
			String consideredFile = primaryEntry.getValue().get(0);
			seeker: if (!Cli.getExcludes().isEmpty()
					&& !(Cli.getExcludes().size() == 1 && Cli.getExcludes().iterator().next().pattern().equals(":"))) {
				for (final Pattern exclusionPattern : Cli.getExcludes()) {
					for (final String candidateFile : primaryEntry.getValue()) {
						if (!exclusionPattern.matcher(candidateFile).matches()) {
							consideredFile = candidateFile;
							break seeker;
						}
					}
				}
				continue;
			}

			for (final String candidateDuplicate : purgatoryIndex.getOrDefault(primaryEntry.getKey(),
					Collections.emptyList())) {
				if (consideredFile.equals(candidateDuplicate)) {
					continue;
				}

				if (FileUtils.contentEquals(new File(consideredFile), new File(candidateDuplicate))) {
					files.add(Pair.of(consideredFile, candidateDuplicate));
				} else {
					LOG.fatal(
							"You got a hash collision, how awesome is that? 😁 Hash: {}"
									+ System.getProperty("line.separator") + "\tfileA: {}"
									+ System.getProperty("line.separator") + "\tfileB: {}",
							primaryEntry.getKey(), consideredFile, candidateDuplicate);
				}
			}
		}

		List<Pair<String, String>> directories = new ArrayList<>();
		if (Cli.consolidateDirectories()) {
			counter = 0;
			LOG.debug("Consolidating directories for {} file pairs", files.size());
			List<String> primaries = files.stream()
					.map(Pair::getLeft)
					.map(jDupPur::properAbsolutePath)
					.collect(Collectors.toList());
			List<String> secondaries = files.stream()
					.map(Pair::getRight)
					.map(jDupPur::properAbsolutePath)
					.collect(Collectors.toList());
			primary: for (final String primary : primaries) {
				++counter;
				LOG.trace("Consolidating pair number {} out of {}", counter, files.size());
				if (counter % 1000 == 0) {
					LOG.debug("Consolidating pair number {} out of {}", counter, files.size());
				}
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
		}
		LOG.debug("{} directories found during consolidation", directories.size());
		List<String> directoriesToSkip = directories.stream().map(Pair::getRight).collect(Collectors.toList());
		List<Pair<String, String>> filesNotInDuplicateDirectories = files.stream().filter(p -> {
			for (final String directory : directoriesToSkip) {
				if (p.getRight().startsWith(directory)) {
					return false;
				}
			}
			return true;
		}).collect(Collectors.toList());
		LOG.debug("{} files left after directory consolidation", filesNotInDuplicateDirectories.size());

		return Pair.of(directories, filesNotInDuplicateDirectories);
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
