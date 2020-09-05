jDupPur
=======

jDupPur is a mini-project for easing the task of purging duplicate data.

Workflow
--------

1. Generate purgatory file on the given folder
    - I.e. it will generate a file, containing the hashes of all files (recursively) within the given folder
1. Purge the purgatory based on the purgatory file and a primary folder
    - I.e. it will iterate over all files in the primary and delete all files from the purgatory, which are the same (at least that's the idea, currently it only creates a candidate list, but doesn't do any file operations)

Examples
-------

### Identifying candidates in backup

I noticed that I tend to do cleanups (e.g. of backups) in the following way:

- I create an "everything goes into this BLOB" directory, whereto I put everything, unsorted
- I start to cleanly organize stuff into some selected categories, directories, etc.
- I have a problem by identifying duplicates, i.e. files which I have sorted, but there are other copies still lingering under different paths in the "BLOB" directory.

Thus:

#### First run (internal SSD, containing only "clean stuff"):
`--logger-interval 5 --very-verbose --create-index C:\Users\foo\Documents\books\ --parallel-indexing --write-output C:\Users\foo\Documents\books.sha512`

Output: a file containing a SHA-512 for all files (recursively) found in `C:\Users\foo\Documents\books\`

#### Second run (external HDD, containing the "BLOB" directory):
`--logger-interval 5 --very-verbose --create-index X:\many-backups-not-sorted\ --write-output X:\backups.sha512`

Output: a file containing a SHA-512 for all files (recursively) found in `X:\many-backups-not-sorted\`

#### Third run:
`--logger-internal 5 --very-verbose --consolidate-directories --create-purge-list C:\Users\foo\Documents\books.sha512 X:\backups.sha512 --write-output X:\to-purge.txt`

Output: a file, listing candidate directories and files, which can be deleted in `many-backups-not-sorted`

### Identifying duplicates in existing data

#### First run:
`--logger-interval 5 --very-verbose --create-index C:\Users\foo\Documents\ --parallel-indexing --write-output C:\Users\foo\docs.sha512`

Output: a file containing a SHA-512 for all files (recursively) found in `C:\Users\foo\Documents\`

#### Second run:
`--logger-interval 5 --very-verbose --consolidate-directories --check-duplicates C:\Users\foo\docs.sha512`

Output: a file, listing candidate directories and files, which are duplicated within `C:\Users\foo\Documents\`. Please note that some duplication are totally legit (e.g. if you have websites exported, then a lot of `.js` files or even images will be duplicated, because that's how those dumps work).


Command line parameters
-----------------------

```
usage: jDupPur [-@ <arg>] [-c <arg>] [-d] [-e <arg>] [-h] [-l <arg>] [-n]
       [-o <arg>] [-p] [-q] [-r <arg>] [-s <arg>] [-u <arg>] [-v] [-vv]
 -@,--hash-function <arg>       overrides the hash function to be used
                                (default: SHA-512)
 -c,--check <arg>               read hashes from the given file and check
                                them
 -d,--check-duplicates          if this flag is set, checking for
                                duplicates (either on the fly via
                                -r,--create-index or cold via -c,--check)
                                will be performed and output onto WARN
                                will be printed (no action on the
                                filesystem)
 -e,--exclude <arg>             exclude paths, which match any of these
                                regexes (separator: ' * ')
 -h,--help                      print this message
 -l,--logger-interval <arg>     interval in seconds of logging during
                                crawling, indexing, and purging (must be
                                at least 1, default: Long.MAX_VALUE)
 -n,--consolidate-directories   consolidate directories in reports via
                                -d,--check-duplicates and for
                                -u,--create-purge-list
 -o,--write-output <arg>        writing the list created with
                                -r,--create-index or
                                -u,--create-purge-list into the file given
                                with this option (file must not exist
                                beforehand)
 -p,--parallel-indexing         if this flag is set, the indexing will be
                                done in parallel (not recommended for
                                HDDs) (default: not parallel)
 -q,--quiet                     do not print info messages, only warnings
                                and errors
 -r,--create-index <arg>        create purgatory index (recursively of the
                                given path), must be coupled with
                                -d,--check-duplicates or -o,--write-index;
                                mutually exclusive with -c,--check
 -s,--sort-index <arg>          sort index before persisting (0: don't, 1:
                                based on hash, 2: based on path; default:
                                1)
 -u,--create-purge-list <arg>   create a list of purgable items, where the
                                first path gives a primary index (files to
                                keep) and the second path gives a
                                purgatory index (files to delete, if
                                duplicate); mutually exclusive with
                                -r,--create-index and -c,--check
 -v,--verbose                   print debug messages (overrides
                                -q,--quiet)
 -vv,--very-verbose             print all log messages (overrides
                                -v,--verbose and -q,--quiet)
```

Rationale
---------

### Is this safe?

No, use it at your own risk.

### Is this compatible with X?

Theoritically yes. It's "compatible" with the various `...sum` utilities of *nix and with Total Commander in the sense that the output format is the same as with those tools. This tool, however, supports stricly absolute paths, thus if you create a checksum file with some other tool, which contains relative paths, `jDupPur` can't use that.

Another difference is that for `jDupPur` a file not existing is no biggie and will land only on the DEBUG-log, not on WARN/ERROR (in constrast to failing checksums). Files with matching hashes ("OK") land on the INFO-log.

### Is this performant?

Performant enough for my purposes.

I did not put too much effort into fine-tuning the code itself (memory consumption, CPU cycles), because based on my experience, the bottleneck is anyways I/O on the disk (even for SSDs), although with stream multi-threading is so easy that I opted for it. Plus some light-weight profiling with jVisualVM seem to confirm my assumptions.

On my SSD (Samsung SSD 850 PRO) the current state of software resulted in parallel 100% I/O at around 550MB/s instead of the single-threaded performance of around 200MB/s. See, of course, [caveats for parallel streams](https://gist.github.com/AFulgens/ba1fec3235cfda1269550fb8e9793db3). Here the trade-off is that reading many smaller files on one thread and some bigger files on other threads seem to balance corrently with parallelism. On an HDD this won't work, that's why there is a switch 😎 My numbers on an HDD (Toshiba Performance X300 via USB3) were around 120MB/s for a single threaded read (of bigger files, dropping to 40-50 MB/s for many small files), while the parallel indexing couldn't go above 60MB/s. On a NAS (RAID6 of 8 WD Red PROs) TODO.

On a sustained read (e.g. check of 200GB), on an SSD with parallelism I get ≈30% reduced runtime against a serials check (e.g. compared to Total Commander). On smaller chunks (9 GB of variously sized files) you can get a doubling of speed or even a bit more compared to a single thread.

### Why Java, maven, and eclipse?

'cause I'm lazy and wanted to do the project as fast as possible, instead of bumping into language- or tooling induced problems. Java is my main language, thus using Java, even if it's not the best hammer for this particular nail.

### Isn't this a bit overengineered?

Probably. I had a little bit of fun here and there.

### Why do you litter `final` for everything possible, why is everything possible with streams/lambdas?

That's an inside joke. Basically just to make extension/debugging as hard as possible.
