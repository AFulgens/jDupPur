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
import java.util.function.Function;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Cli {

	private static final Logger LOG = LogManager.getLogger(Cli.class);

	private static final String HELP = "h";
	private static final String CREATE_LIST = "c";
	private static final String HASH_FUNCTION = "s";
	private static final String LOGGER_INTERVAL = "l";
	private static final String PARALLEL_INDEXING = "p";
	private static final String CHECK_DUPLICATES = "d";
	private static final String WRITE_OUTPUT = "o";

	private static final Options OPTIONS = new Options();
	private static final Map<String, Pair<String, String>> OPTION_MAP = Map.ofEntries(
			Map.entry(HELP, Pair.of("help", "print this message")),
			Map.entry(CREATE_LIST,
					Pair.of("create-list",
							"create purgatory list (recursively of the given path), must be coupled with 'd' or 'o'")),
			Map.entry(HASH_FUNCTION,
					Pair.of("hash-function", "overrides the hash function to be used (default: SHA-512")),
			Map.entry(LOGGER_INTERVAL, Pair.of("logger-interval",
					"interval in seconds of logging during crawling, indexing, and purging (default: Long.MAX_VALUE)")),
			Map.entry(PARALLEL_INDEXING, Pair.of("parallel-indexing",
					"if this flag is set, the indexing will be done in parallel (not recommended for HDDs) (default: not parallel)")),
			Map.entry(CHECK_DUPLICATES, Pair.of("check-duplicates",
					"if this flag is set, checking for duplicates (either on the fly via 'c' or cold via 'i') will be performed and output onto WARN will be printed (no action on the filesystem)")),
			Map.entry(WRITE_OUTPUT, Pair.of("write-output",
					"writing the list created with 'c' into the file given with this option (file must not exist beforehand)")));
	static {
		for (final Entry<String, Pair<String, String>> option : OPTION_MAP.entrySet()) {
			OPTIONS.addOption(Option.builder(option.getKey())
					.longOpt(option.getValue().getLeft())
					.desc(option.getValue().getRight())
					.numberOfArgs(1)
					.build());
		}
		OPTIONS.getOption(HELP).setArgs(0);
		OPTIONS.getOption(PARALLEL_INDEXING).setArgs(0);
		OPTIONS.getOption(CHECK_DUPLICATES).setArgs(0);
	}

	private static CommandLine cli;

	public synchronized static void parse(final String[] args) throws ParseException {
		final List<String> usedArgs = new ArrayList<>(Arrays.asList(args));

		cli = new DefaultParser().parse(OPTIONS, usedArgs.toArray(new String[0]));

		if (cli.hasOption(HELP)) {
			new HelpFormatter().printHelp("jDupPur", null, OPTIONS, null, true);
			System.exit(0);
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

		check();
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
	}

	public static String getInput() {
		return cli.getOptionValue(CREATE_LIST); // TODO: cli.getOptionValue(PURGE_BASE)
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
				return StringUtils.EMPTY; // safe to ignore, we already tested it in #check()
			}
		};
	}

	public static long getInterval() {
		return Long.parseLong(cli.getOptionValue(LOGGER_INTERVAL));
	}

	public static boolean getParallel() {
		return cli.hasOption(PARALLEL_INDEXING);
	}

	public static boolean createList() {
		return cli.hasOption(CREATE_LIST);
	}

	public static boolean checkDuplicates() {
		return cli.hasOption(CHECK_DUPLICATES);
	}

	public static boolean writeOutput() {
		return cli.hasOption(WRITE_OUTPUT);
	}

	public static String getOutput() {
		return cli.getOptionValue(WRITE_OUTPUT);
	}

	private static void check() throws ParseException {
		if (cli.hasOption(CREATE_LIST)) {
			if (!new File(cli.getOptionValue(CREATE_LIST)).isAbsolute()) {
				throw new ParseException("Argument for purgatory list creation must be an absolute path, it was: "
						+ cli.getOptionValue(CREATE_LIST));
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
				Long.parseLong(cli.getOptionValue(LOGGER_INTERVAL));
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
				throw new ParseException("Security exception during file creation: " + cli.getOptionValue(WRITE_OUTPUT));
			}
		}
	}

	private static String asHexMethod(String digestName) {
		return StringUtils.remove(digestName.toLowerCase(Locale.ENGLISH), "-") + "Hex";
	}
}
