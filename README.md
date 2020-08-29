jDupPur
=======

jDupPur is a mini-project for purging duplicate data.

Workflow
--------

1. Generate purgatory file on the given folder
    - I.e. it will generate a file, containing the hashes of all files (recursively) within the given folder
1. Purge the purgatory based on the purgatory file and a primary folder
    - I.e. it will iterate over all files in the primary and delete all files from the purgatory, which are the same

Example
-------

TODO

Command line parameters
-----------------------

TODO

Rationale
---------

### Is this safe?

No, use it at your own risk.

### Is this performant?

Performant enough for my purposes.

I did not put too much effort into fine-tuning the code itself (memory consumption, CPU cycles), because based on my experience, the bottleneck is anyways I/O on the disk (even for SSDs), although with stream multi-threading is so easy that I opted for it.

On my SSD (Samsung SSD 850 PRO) this resulted in 100% I/O at around 550MB/s instead of the single-threaded performance of around 200MB/s. See, of course, [caveats for parallel streams](https://gist.github.com/AFulgens/ba1fec3235cfda1269550fb8e9793db3). Here the trade-off is that reading many smaller files on one thread and some bigger files on other threads seem to balance corrently with parallelism. On an HDD this won't work, that's why there is a switch 😎 My numbers on an HDD (Toshiba Performance X300 via USB3) were TODO

### Why Java, maven, and eclipse?

'cause I'm lazy and wanted to do the project as fast as possible, instead of bumping into language- or tooling induced problems. Java is my main language, thus using Java, even if it's not the best hammer for this particular nail.

### Isn't this a bit overengineered?

Probably. I had a little bit of fun here and there.

### Why do you litter `final` for everything possible?

That's an inside joke.
