package xyz.kovacs.jduppur;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.SetUtils;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class Crawler {

	private static final Logger LOG = LogManager.getLogger(Crawler.class);

	private static int recursionLevel = 0;

	private static CrawlerLogger crawlerLogger = new CrawlerLogger(Long.MAX_VALUE);
	private static Thread crawlerLoggerThread;

	public synchronized static CrawlerLogger initLogger(final long interval) {
		crawlerLogger = new CrawlerLogger(interval);
		crawlerLoggerThread = new Thread(crawlerLogger);
		crawlerLoggerThread.setDaemon(true);
		crawlerLoggerThread.start();
		return crawlerLogger;
	}

	public synchronized static Set<String> list(final String root, final Set<Pattern> excludes) {
		recursionLevel++;

		final File rootFile = new File(root);
		if (!rootFile.isAbsolute()) {
			throw new IllegalArgumentException("Path must be absolute, it was: " + root);
		}

		LOG.trace("[level: {}] Listing {}", recursionLevel, root);
		final File[] files = rootFile.listFiles();
		LOG.trace("[level: {}] {} files in {}", recursionLevel, ArrayUtils.getLength(files), root);
		if (ArrayUtils.getLength(files) == 0) {
			return Collections.emptySet();
		}
		final Set<String> directResults = Arrays.stream(files)
				.filter(FilePredicate.EXISTS)
				.filter(FilePredicate.READABLE)
				.filter(FilePredicate.NOT_SYMLINK)
				.filter(FilePredicate.IS_FILE)
				.map(File::getAbsolutePath)
				.map(jDupPur::properAbsolutePath)
				.filter(p -> {
					for (final Pattern pattern : excludes) {
						if (pattern.matcher(p).matches()) {
							return false;
						}
					}
					return true;
				})
				.collect(Collectors.toSet());
		crawlerLogger.addFiles(directResults);
		LOG.trace("[level: {}] {} real files in {}", recursionLevel, directResults.size(), root);
		final Set<String> indirectResults = Arrays.stream(files)
				.filter(FilePredicate.EXISTS)
				.filter(FilePredicate.READABLE)
				.filter(FilePredicate.NOT_SYMLINK)
				.filter(FilePredicate.IS_DIRECTORY)
				.map(File::getAbsolutePath)
				.map(jDupPur::properAbsolutePath)
				.flatMap(d -> list(d, excludes).stream())
				.collect(Collectors.toSet());
		LOG.trace("[level: {}] {} files recursively in {}", recursionLevel, indirectResults.size(), root);

		--recursionLevel;
		return SetUtils.union(directResults, indirectResults);
	}

	public synchronized static Map<String, List<String>> index(final Set<String> fileList,
			final Function<InputStream, String> digestFunction, final boolean parallel, final Set<Pattern> excludes) {

		crawlerLogger.turnOffListing();

		// we expect some collisions, and if possible, don't want to have expands on
		// this Map
		final ConcurrentMap<String, List<String>> result = new ConcurrentHashMap<>((int) (fileList.size() * 1.5));

		jDupPur.conditionallyParallel(fileList.stream(), parallel)
				.map(File::new)
				.filter(FilePredicate.EXISTS)
				.filter(FilePredicate.READABLE)
				.filter(FilePredicate.NOT_SYMLINK)
				.filter(FilePredicate.IS_FILE)
				.filter(p -> {
					for (final Pattern pattern : excludes) {
						if (pattern.matcher(jDupPur.properAbsolutePath(p.getAbsolutePath())).matches()) {
							return false;
						}
					}
					return true;
				})
				.forEach(f -> {
					try (final InputStream is = FileUtils.openInputStream(f)) {
						result.computeIfAbsent(digestFunction.apply(is), k -> new ArrayList<>())
								.add(jDupPur.properAbsolutePath(f.getAbsolutePath()));
						crawlerLogger.processed(jDupPur.properAbsolutePath(f.getAbsolutePath()));
					} catch (FileNotFoundException fnfe) {
						LOG.error("{}: file does not exist", jDupPur.properAbsolutePath(f.getAbsolutePath()));
					} catch (IOException e) {
						LOG.error("{}: cannot read file", jDupPur.properAbsolutePath(f.getAbsolutePath()));
					}
				});

		crawlerLogger.stop();
		crawlerLoggerThread.interrupt();

		return result;
	}

	public synchronized static Map<String, List<String>> reIndex(final List<String> indexAsList,
			final Function<InputStream, String> digestFunction, final boolean parallel, final Set<Pattern> excludes) {

		MultiValuedMap<String, String> index = new ArrayListValuedHashMap<>();
		index: for (final String indexEntry : indexAsList) {
			final String[] entry = StringUtils.splitByWholeSeparator(indexEntry, " *", 2);
			for (final Pattern pattern : excludes) {
				if (pattern.matcher(entry[1]).matches()) {
					continue index;
				}
			}
			index.put(entry[0], entry[1]);
		}
		crawlerLogger.addFiles(index.values());

		crawlerLogger.turnOffListing();

		AtomicLong ok = new AtomicLong(0L);
		AtomicLong fail = new AtomicLong(0L);
		Map<String, List<String>> reIndex = new ConcurrentHashMap<>();

		jDupPur.conditionallyParallel(index.entries().stream(), parallel)
				.map(e -> Pair.of(new File(e.getValue()), e.getKey()))
				.filter(p -> FilePredicate.EXISTS.test(p.getLeft()))
				.filter(p -> FilePredicate.READABLE.test(p.getLeft()))
				.filter(p -> FilePredicate.NOT_SYMLINK.test(p.getLeft()))
				.filter(p -> FilePredicate.IS_FILE.test(p.getLeft()))
				.map(p -> {
					try (final InputStream is = FileUtils.openInputStream(p.getLeft())) {
						final String digest = digestFunction.apply(is);
						crawlerLogger.processed(jDupPur.properAbsolutePath(p.getLeft().getAbsolutePath()));
						return Pair.of(p.getLeft(), Pair.of(digest, p.getRight()));
					} catch (FileNotFoundException fnfe) {
						LOG.error("{}: File does not exist", jDupPur.properAbsolutePath(p.getLeft().getAbsolutePath()));
						return null;
					} catch (IOException e) {
						LOG.error("{}: Could not read file", jDupPur.properAbsolutePath(p.getLeft().getAbsolutePath()));
						return null;
					}
				})
				.forEach(p -> {
					if (p.getRight().getLeft().equalsIgnoreCase(p.getRight().getRight())) {
						LOG.info("{}: OK", jDupPur.properAbsolutePath(p.getLeft().getAbsolutePath()));
						ok.incrementAndGet();
						reIndex.computeIfAbsent(p.getRight().getLeft(), k -> new ArrayList<>())
								.add(jDupPur.properAbsolutePath(p.getLeft().getAbsolutePath()));
					} else {
						LOG.error("{}: FAIL", jDupPur.properAbsolutePath(p.getLeft().getAbsolutePath()));
						fail.incrementAndGet();
					}
				});

		crawlerLogger.stop();
		crawlerLoggerThread.interrupt();

		LOG.info("{} files were OK", ok.get());
		LOG.log(fail.get() > 0 ? Level.ERROR : Level.INFO, "{} files were FAIL", fail.get());

		return reIndex;
	}

	public static final class CrawlerLogger implements Runnable {

		private static final Logger LOG = LogManager.getLogger(CrawlerLogger.class);

		private final AtomicBoolean stopped = new AtomicBoolean(false);
		private final AtomicBoolean listing = new AtomicBoolean(true);
		private final AtomicLong fileCount = new AtomicLong(0);
		private final AtomicLong processedFileCount = new AtomicLong(0);
		private final AtomicLong cumulativeSize = new AtomicLong(0L);
		private final AtomicLong processedCumulativeSize = new AtomicLong(0L);
		private final long interval;
		private long startedProcessingAt = System.nanoTime();

		private CrawlerLogger(final long interval) {
			this.interval = interval * 1000L;
		}

		public void addFiles(final Collection<String> fileNames) {
			if (listing.get()) {
				long count = fileNames.stream()
						.map(File::new)
						.filter(FilePredicate.EXISTS)
						.filter(FilePredicate.READABLE)
						.filter(FilePredicate.NOT_SYMLINK)
						.filter(FilePredicate.IS_FILE)
						.count();
				long size = fileNames.stream()
						.map(File::new)
						.filter(FilePredicate.EXISTS)
						.filter(FilePredicate.READABLE)
						.filter(FilePredicate.NOT_SYMLINK)
						.filter(FilePredicate.IS_FILE)
						.mapToLong(FileUtils::sizeOf)
						.sum();
				fileCount.addAndGet(count);
				cumulativeSize.addAndGet(size);
				LOG.debug("{} files (with cumulative size {}) added to the logger", count,
						FileUtils.byteCountToDisplaySize(size));
			} else {
				throw new IllegalStateException("Files can only be added to the logger in listing state");
			}
		}

		public void turnOffListing() {
			LOG.info("Listing ended, counted {} files (with total size {})", fileCount.get(),
					FileUtils.byteCountToDisplaySize(cumulativeSize.get()));
			listing.getAndSet(false);
		}

		public void processed(String fileName) {
			if (!listing.get()) {
				long size = FileUtils.sizeOf(new File(fileName));
				processedFileCount.getAndIncrement();
				processedCumulativeSize.addAndGet(size);
				LOG.trace("File {} (with size {}) marked as processed", fileName,
						FileUtils.byteCountToDisplaySize(size));
			} else {
				throw new IllegalStateException("Files can only be marked as processed in non-listing state");
			}
		}

		public void stop() {
			stopped.getAndSet(true);
		}

		public long getProcessedBytes() {
			return processedCumulativeSize.get();
		}

		public long getStartedProcessingAt() {
			return startedProcessingAt;
		}

		@Override
		public void run() {
			boolean indexingOngoing = false;
			long start = System.nanoTime();
			while (!stopped.get()) {
				try {
					Thread.sleep(interval);
					if (listing.get()) {
						LOG.info("Still listing, currently @ {} files (with total size {}), elapsed time: {}",
								fileCount.get(), FileUtils.byteCountToDisplaySize(cumulativeSize.get()),
								jDupPur.humanReadableTime(System.nanoTime() - start));
					} else {
						if (!indexingOngoing) {
							indexingOngoing = true;
							start = System.nanoTime();
							startedProcessingAt = start;
						}
						if (LOG.isInfoEnabled()) {
							long processedCount = processedFileCount.get();
							long processedSize = processedCumulativeSize.get();

							long current = System.nanoTime();
							long elapsedTime = current - start;
							long totalSize = cumulativeSize.get();
							long estimatedTime = (long) ((double) elapsedTime / (double) processedSize
									* (double) totalSize) - elapsedTime;

							LOG.info(
									"[{}/{}] Still indexing, currently @ {} files out of {} (with processed size {} out of {}), elapsed time: {}, estimated time left: {}",
									jDupPur.PERCENT.format((double) processedCount / (double) fileCount.get()),
									jDupPur.PERCENT.format((double) processedSize / (double) cumulativeSize.get()),
									processedCount, fileCount.get(), FileUtils.byteCountToDisplaySize(processedSize),
									FileUtils.byteCountToDisplaySize(cumulativeSize.get()),
									jDupPur.humanReadableTime(current - start),
									jDupPur.humanReadableTime(estimatedTime));
						}
					}
				} catch (InterruptedException e) {
					LOG.trace("CrawlerLogger interruped -> ending thread");
					return;
				}
			}
			LOG.info("Indexing finished, indexed {} files (with total size of {})", processedFileCount.get(),
					FileUtils.byteCountToDisplaySize(processedCumulativeSize.get()));
		}
	}
}
