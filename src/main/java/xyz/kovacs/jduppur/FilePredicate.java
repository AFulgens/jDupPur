package xyz.kovacs.jduppur;

import java.io.File;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public enum FilePredicate implements Predicate<File> {

	EXISTS {
		@Override
		public boolean test(File f) {
			return test(f, File::exists, "does not exist");
		}
	},
	READABLE {
		@Override
		public boolean test(File f) {
			return test(f, File::canRead, "is not readable");
		}
	},
	NOT_SYMLINK {
		@Override
		public boolean test(File f) {
			return test(f, file -> !FileUtils.isSymlink(file), "is a symlink");
		}
	},
	IS_FILE {
		@Override
		public boolean test(File f) {
			return test(f, File::isFile, "is not a file");
		}
	},
	IS_DIRECTORY {
		@Override
		public boolean test(File f) {
			return test(f, File::isDirectory, "is not a directory");
		}
	};
	
	private static final Logger LOG = LogManager.getLogger(FilePredicate.class);
	
	private static boolean test(final File f, final Function<File, Boolean> tester, final String message) {
		if (tester.apply(f)) {
			return true;
		}
		LOG.trace("{}: {}", jDupPur.properAbsolutePath(f.getAbsolutePath()), message);
		return false;
	}

	@Override
	public abstract boolean test(File t);
}
