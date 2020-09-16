package xyz.kovacs.jduppur;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

public final class Cli {

	private static final Logger LOG = LogManager.getLogger(Cli.class);

	private static final String HELP = "h";

	private static final String QUIET = "q"; // INFO -> WARN
	private static final String VERBOSE = "v"; // INFO -> DEBUG
	private static final String VERY_VERBOSE = "vv"; // INFO -> ALL
	private static final String LOGGER_INTERVAL = "l";

	private static final String EXCLUDE = "e";

	private static final String HASH_FUNCTION = "@";

	private static final String CREATE_INDEX = "r";
	private static final String PARALLEL_INDEXING = "p";
	private static final String SORT_INDEX = "s";
	private static final String UPDATE_INDEX = "a";

	private static final String CREATE_PURGE_LIST = "u";

	private static final String CHECK = "c";
	private static final String CHECK_DUPLICATES = "d";
	private static final String CONSOLIDATE_DIRECTORIES = "n";

	private static final String WRITE_OUTPUT = "o";

	private static final Options OPTIONS = new Options();
	private static final Map<String, Pair<String, String>> OPTION_MAP = Map.ofEntries(
			Map.entry(HELP, Pair.of("help", "print this message")),

			Map.entry(QUIET, Pair.of("quiet", "do not print info messages, only warnings and errors")),
			Map.entry(VERBOSE, Pair.of("verbose", "print debug messages (overrides -q,--quiet)")),
			Map.entry(VERY_VERBOSE,
					Pair.of("very-verbose", "print all log messages (overrides -v,--verbose and -q,--quiet)")),
			Map.entry(LOGGER_INTERVAL, Pair.of("logger-interval",
					"interval in seconds of logging during crawling, indexing, and purging (must be at least 1, default: Long.MAX_VALUE)")),

			Map.entry(EXCLUDE, Pair.of("exclude", "exclude paths, which match any of these regexes (separator: ' * ')")),
			
			Map.entry(HASH_FUNCTION,
					Pair.of("hash-function", "overrides the hash function to be used (default: SHA-512)")),

			Map.entry(CREATE_INDEX, Pair.of("create-index",
					"create purgatory index (recursively of the given path), must be coupled with -d,--check-duplicates or -o,--write-index; mutually exclusive with -c,--check")),
			Map.entry(PARALLEL_INDEXING, Pair.of("parallel-indexing",
					"if this flag is set, the indexing will be done in parallel (not recommended for HDDs) (default: not parallel)")),
			Map.entry(WRITE_OUTPUT, Pair.of("write-output",
					"writing the list created with -r,--create-index or -u,--create-purge-list into the file given with this option (file must not exist beforehand)")),
			Map.entry(SORT_INDEX,
					Pair.of("sort-index",
							"sort index before persisting (0: don't, 1: based on hash, 2: based on path; default: 1)")),
			Map.entry(UPDATE_INDEX, Pair.of("update-index", "updates an index (i.e. removes files, which do not exist); in place, if not combined with -o,--write-index")),
			
			Map.entry(CREATE_PURGE_LIST, Pair.of("create-purge-list",
					"create a list of purgable items, where the first path gives a primary index (files to keep) and the second path gives a purgatory index (files to delete, if duplicate); mutually exclusive with -r,--create-index and -c,--check")),

			Map.entry(CHECK, Pair.of("check", "read hashes from the given file and check them")),
			Map.entry(CHECK_DUPLICATES, Pair.of("check-duplicates",
					"if this flag is set, checking for duplicates (either on the fly via -r,--create-index or cold via -c,--check) will be performed and output onto WARN will be printed (no action on the filesystem)")),
			Map.entry(CONSOLIDATE_DIRECTORIES, Pair.of("consolidate-directories",
					"consolidate directories in reports via -d,--check-duplicates and for -u,--create-purge-list")));

	static {
		for (final Entry<String, Pair<String, String>> option : OPTION_MAP.entrySet()) {
			OPTIONS.addOption(Option.builder(option.getKey())
					.longOpt(option.getValue().getLeft())
					.desc(option.getValue().getRight())
					.numberOfArgs(1)
					.build());
		}
		OPTIONS.getOption(HELP).setArgs(0);
		OPTIONS.getOption(VERBOSE).setArgs(0);
		OPTIONS.getOption(VERY_VERBOSE).setArgs(0);
		OPTIONS.getOption(QUIET).setArgs(0);
		OPTIONS.getOption(PARALLEL_INDEXING).setArgs(0);
		OPTIONS.getOption(CHECK_DUPLICATES).setArgs(0);
		OPTIONS.getOption(CONSOLIDATE_DIRECTORIES).setArgs(0);
		OPTIONS.getOption(CREATE_PURGE_LIST).setArgs(2);
	}

	private static CommandLine cli;

	public synchronized static void parse(final String[] args) throws ParseException {
		final List<String> usedArgs = new ArrayList<>(Arrays.asList(args));

		cli = new DefaultParser().parse(OPTIONS, usedArgs.toArray(new String[0]));

		if (cli.hasOption(HELP)) {
			new HelpFormatter().printHelp("jDupPur", null, OPTIONS, null, true);
			System.exit(0);
		}

		if (cli.hasOption(QUIET)) {
			Configurator.setLevel("xyz.kovacs.jduppur", Level.WARN);
		}
		if (cli.hasOption(VERBOSE)) {
			Configurator.setLevel("xyz.kovacs.jduppur", Level.DEBUG);
		}
		if (cli.hasOption(VERY_VERBOSE)) {
			Configurator.setLevel("xyz.kovacs.jduppur", Level.ALL);
		}

		if (!cli.hasOption(SORT_INDEX)) {
			usedArgs.add("-" + SORT_INDEX);
			usedArgs.add("" + 1);
			cli = new DefaultParser().parse(OPTIONS, usedArgs.toArray(new String[0]));
		}

		if (!cli.hasOption(EXCLUDE)) {
			usedArgs.add("-" + EXCLUDE);
			usedArgs.add(":");
		}

		if (!cli.hasOption(HASH_FUNCTION)) {
			usedArgs.add("-" + HASH_FUNCTION);
			usedArgs.add("SHA-512");
			cli = new DefaultParser().parse(OPTIONS, usedArgs.toArray(new String[0]));
		}

		if (!cli.hasOption(LOGGER_INTERVAL)) {
			usedArgs.add("-" + LOGGER_INTERVAL);
			usedArgs.add("" + Long.MAX_VALUE);
			cli = new DefaultParser().parse(OPTIONS, usedArgs.toArray(new String[0]));
		}

		internalCheck();
	}

	public static void printOptions() {
		Arrays.stream(cli.getOptions())
				.filter(o -> o.getArgs() > 0)
				.map(o -> o.getLongOpt() + " = " + o.getValue())
				.forEach(LOG::debug);
		if (!cli.hasOption(PARALLEL_INDEXING)) {
			LOG.debug("parallel-indexing = not set (i.e. sequential indexing)");
		} else {
			LOG.debug("parallel-indexing = set (i.e. parallel indexing)");
		}
		if (!cli.hasOption(CHECK_DUPLICATES)) {
			LOG.debug("check-duplicates = not set (i.e. not printing duplicates onto WARN)");
		} else {
			LOG.debug("check-duplicates = set (i.e. printing duplicates onto WARN)");
		}
		if (!cli.hasOption(CONSOLIDATE_DIRECTORIES)) {
			LOG.debug("consolidate-directories = not set (i.e. not consolidating directories)");
		} else {
			LOG.debug("consolidate-directories = set (i.e. consolidating directories)");
		}
		if (cli.hasOption(VERY_VERBOSE)) {
			LOG.debug("log-level = very-verbose (print all messages)");
		} else if (cli.hasOption(VERBOSE)) {
			LOG.debug("log-level = verbose (print debug messages and all above)");
		} else if (cli.hasOption(QUIET)) {
			LOG.debug("log-level = quiet (print warning messages and all above)");
		} else {
			LOG.debug("log-level = normal (print info messages and all above)");
		}
	}

	public static String getInput() {
		if (cli.hasOption(CREATE_INDEX)) {
			return cli.getOptionValue(CREATE_INDEX);
		} else if (cli.hasOption(CHECK)) {
			return cli.getOptionValue(CHECK);
		} else if (cli.hasOption(CREATE_PURGE_LIST)) {
			return Arrays.stream(cli.getOptionValues(CREATE_PURGE_LIST)).collect(Collectors.joining("*"));
		} else if (cli.hasOption(UPDATE_INDEX)) {
			return cli.getOptionValue(UPDATE_INDEX);
		}
		throw new IllegalStateException("No input found with current configuration");
	}

	public static Set<Pattern> getExcludes() {
		final String regexes = cli.getOptionValue(EXCLUDE);
		return Arrays.stream(StringUtils.splitByWholeSeparator(regexes, " * "))
				.distinct()
				.map(r -> Pattern.compile(r))
				.collect(Collectors.toSet());
	}

	public static Function<InputStream, String> getDigest() {
		return is -> {
			try {
				return (String) DigestUtils.class
						.getDeclaredMethod(asHexMethod(cli.getOptionValue(HASH_FUNCTION)), InputStream.class)
						.invoke(null, is);
			} catch (final IllegalAccessException | IllegalArgumentException | InvocationTargetException
					| NoSuchMethodException | SecurityException e) {
				LOG.debug(e);
				return StringUtils.EMPTY; // safe to ignore, we already tested it in #internalCheck()
			}
		};
	}

	public static long getInterval() {
		return Long.parseLong(cli.getOptionValue(LOGGER_INTERVAL));
	}

	public static boolean getParallel() {
		return cli.hasOption(PARALLEL_INDEXING);
	}

	public static boolean createIndex() {
		return cli.hasOption(CREATE_INDEX);
	}
	
	public static boolean createPurgatory() {
		return cli.hasOption(CREATE_PURGE_LIST);
	}
	
	public static boolean updateIndex() {
		return cli.hasOption(UPDATE_INDEX);
	}

	public static boolean check() {
		return cli.hasOption(CHECK);
	}

	public static boolean checkDuplicates() {
		return cli.hasOption(CHECK_DUPLICATES);
	}

	public static boolean consolidateDirectories() {
		return cli.hasOption(CONSOLIDATE_DIRECTORIES);
	}

	public static int sort() {
		return Integer.parseInt(cli.getOptionValue(SORT_INDEX));
	}

	public static boolean writeOutput() {
		return cli.hasOption(WRITE_OUTPUT);
	}

	public static String getOutput() {
		return cli.getOptionValue(WRITE_OUTPUT);
	}

	private static void internalCheck() throws ParseException {
		if (cli.hasOption(CREATE_INDEX)) {
			if (cli.hasOption(CHECK)) {
				throw new ParseException("Creating an index and checking are mutually exclusive");
			}
			if (cli.hasOption(CREATE_PURGE_LIST)) {
				throw new ParseException("Creating an index and a purge list are mutually exclusive");
			}
			if (cli.hasOption(UPDATE_INDEX)) {
				throw new ParseException("Creating an index and updating it are mutually exclusive");
			}
			if (!new File(cli.getOptionValue(CREATE_INDEX)).isAbsolute()) {
				throw new ParseException("Argument for purgatory index creation must be an absolute path, it was: "
						+ cli.getOptionValue(CREATE_INDEX));
			}
		}

		if (cli.hasOption(SORT_INDEX)) {
			try {
				final int sort = Integer.parseInt(cli.getOptionValue(SORT_INDEX));
				if (sort < 0 || sort > 2) {
					throw new ParseException("Invalid sort option: " + cli.getOptionValue(SORT_INDEX));
				}
			} catch (final NumberFormatException nfe) {
				throw new ParseException(
						"Sort option must be an integer, but it was " + cli.getOptionValue(SORT_INDEX));
			}
		}

		if (cli.hasOption(CREATE_PURGE_LIST)) {
			if (cli.hasOption(CREATE_INDEX)) {
				throw new ParseException("Creating an index and a purge list are mutually exclusive");
			}
			if (cli.hasOption(CHECK)) {
				throw new ParseException("Checking and creating a purge list are mutually exclusive");
			}
			if (cli.hasOption(UPDATE_INDEX)) {
				throw new ParseException("Creating a purge list and updating an index are mutually exclusive");
			}
			String[] indexes = cli.getOptionValues(CREATE_PURGE_LIST);
			if (!new File(indexes[0]).isAbsolute() || !new File(indexes[1]).isAbsolute()) {
				throw new ParseException("Both arguments for purge list creation must be an absolute path, it was: "
						+ cli.getOptionValue(CREATE_PURGE_LIST));
			}
		}

		if (cli.hasOption(CHECK)) {
			if (cli.hasOption(CREATE_INDEX)) {
				throw new ParseException("Creating an index and checking are mutually exclusive");
			}
			if (cli.hasOption(CREATE_PURGE_LIST)) {
				throw new ParseException("Checking and creating a purge list are mutually exclusive");
			}
			if (cli.hasOption(UPDATE_INDEX)) {
				throw new ParseException("Checking an index and updating an index are mutually exclusive");
			}
		}

		if (cli.hasOption(UPDATE_INDEX)) {
			if (cli.hasOption(CREATE_INDEX)) {
				throw new ParseException("Creating an index and updating it are mutually exclusive");
			}
			if (cli.hasOption(CREATE_PURGE_LIST)) {
				throw new ParseException("Creating a purge list and updating an index are mutually exclusive");
			}
			if (cli.hasOption(CHECK)) {
				throw new ParseException("Checking an index and updating an index are mutually exclusive");
			}
		}

		if (cli.hasOption(HASH_FUNCTION)) {
			final String digestName = cli.getOptionValue(HASH_FUNCTION);

			try {
				DigestUtils.isAvailable(digestName);
			} catch (final IllegalArgumentException iae) {
				throw new ParseException("Unrecognised hash function: " + digestName);
			}

			try {
				final Method methodCandidate = DigestUtils.class.getDeclaredMethod(asHexMethod(digestName),
						InputStream.class);
				if (!(methodCandidate.invoke(null,
						new ByteArrayInputStream("test".getBytes(StandardCharsets.UTF_8))) instanceof String)) {
					throw new NoSuchMethodException(
							"The method " + asHexMethod(digestName) + " is not returning a String");
				}
			} catch (final NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				LOG.debug(e);
				throw new ParseException("Utility method " + asHexMethod(digestName) + " not found in "
						+ DigestUtils.class.getSimpleName());
			}
		}

		if (cli.hasOption(LOGGER_INTERVAL)) {
			try {
				long interval = Long.parseLong(cli.getOptionValue(LOGGER_INTERVAL));
				if (interval <= 0) {
					throw new ParseException(
							"Logger interval must be at least 1, but it was " + cli.getOptionValue(LOGGER_INTERVAL));
				}
			} catch (final NumberFormatException nfe) {
				throw new ParseException(
						"Logger interval must be an integer, but it was " + cli.getOptionValue(LOGGER_INTERVAL));
			}
		}

		if (cli.hasOption(WRITE_OUTPUT)) {
			if (new File(cli.getOptionValue(WRITE_OUTPUT)).exists()) {
				throw new ParseException("File for output alread exists: " + cli.getOptionValue(WRITE_OUTPUT));
			}
			try {
				new File(cli.getOptionValue(WRITE_OUTPUT)).createNewFile();
			} catch (final IOException ie) {
				throw new ParseException("I/O error occured during file creation: " + cli.getOptionValue(WRITE_OUTPUT));
			} catch (final SecurityException se) {
				throw new ParseException(
						"Security exception during file creation: " + cli.getOptionValue(WRITE_OUTPUT));
			}
		}
	}

	private static String asHexMethod(String digestName) {
		return StringUtils.remove(digestName.toLowerCase(Locale.ENGLISH), "-") + "Hex";
	}
}
