<pre>
======================================================================
                 __________  ____  ____  _________
                / ____/ __ \/ __ \/ __ \/ ____/   |
               / / __/ /_/ / / / / /_/ / /   / /| |
              / /_/ / ____/ /_/ / _, _/ /___/ ___ |
              \____/_/    \____/_/ |_|\____/_/  |_|
                  The Greenplum Query Optimizer
              Copyright (c) 2015, Pivotal Software, Inc.
            Licensed under the Apache License, Version 2.0
======================================================================
</pre>

Welcome to GPORCA, the Greenplum Next Generation Query Optimizer!

To understand the objectives and architecture of GPORCA please refer to the following articles:
* [Orca: A Modular Query Optimizer Architecture for Big Data](https://content.pivotal.io/white-papers/orca-a-modular-query-optimizer-architecture-for-big-data).
* [Profiling Query Compilation Time with GPORCA](http://engineering.pivotal.io/post/orca-profiling/)
* [Improving Constraints In ORCA](http://engineering.pivotal.io/post/making-orca-smarter/)

Want to [Contribute](#contribute)?

# First Time Setup

## Build and install GPORCA

ORCA is built automatically with GPDB as long as `--disable-orca` is not used.
<a name="test"></a>

## Test GPORCA

The GPORCA unit tests use the `gporca_prove` utility as the driver to run the
tests. It is a wrapper around the standard perl `prove` program which is also
used to run the TAP tests in GPDB.

To test GPORCA, first go into the `gporca` directory and build GPORCA and the
test executable, `gporca_test`:

```
make gporca_test
```

To run all GPORCA tests, use the `gporca_prove` command from the gporca directory
after build finishes.

```
./gporca_prove --schedule=tap-schedule.txt
```

The test are also run automatically as part of the `unittest-check` make target,
along with all the other GPDB unit tests.

Much like `make`, `prove` has a -j option that allows running multiple tests in
parallel to save time. Using it is recommended for faster testing.

```
./gporca_prove --schedule=tap-schedule.txt -j8
```

By default, `gporca_prove` does not print the output of failed tests. To print the
output of failed tests, use the `--verbose` flag like so (this is
useful for debugging failed tests):

```
./gporca_prove --schedule=tap-schedule.txt -j8 --verbose
```

To run only the previously failed tests, you can use `prove`'s option to save the
list of tests. First run the tests with:
```
./gporca_prove --schedule=tap-schedule.txt -j8 --state=save
```

You can then re-run the failed tests with:
```
./gporca_prove --schedule=tap-schedule.txt -j8 --state=failed
```

To run a specific individual test:

```
./gporca_prove CAggTest
```

To run a specific minidump, for example for `data/dxl/minidump/TVFRandom.mdp`:
```
./gporca_prove data/dxl/minidump/TVFRandom.mdp
```

Note that some tests use assertions that are only enabled for DEBUG builds, so
DEBUG-mode tests tend to be more rigorous.

<a name="addtest"></a>
## Adding tests

Most of the regression tests come in the form of a "minidump" file.
A minidump is an XML file that contains all the input needed to plan a query,
including information about all tables, datatypes, and functions used, as well
as statistics. It also contains the resulting plan.

A new minidump can be created by running a query on a live GPDB server:

1. Run these in a psql session:

```
set client_min_messages='log';
set optimizer=on;
set optimizer_enumerate_plans=on;
set optimizer_minidump=always;
set optimizer_enable_constant_expression_evaluation=off;
```

2. Run the query in the same psql session. It will create a minidump file
   under the "minidumps" directory, in the master's data directory:

```
$ ls -l $MASTER_DATA_DIRECTORY/minidumps/
total 12
-rw------- 1 heikki heikki 10818 Jun 10 22:02 Minidump_20160610_220222_4_14.mdp
```

3. Run xmllint on the minidump to format it better, and copy it under the
   data/dxl/minidump directory:

```
xmllint --format $MASTER_DATA_DIRECTORY/minidumps/Minidump_20160610_220222_4_14.mdp > data/dxl/minidump/MyTest.mdp
```

4. Add it to the test suite, in server/src/unittest/gpopt/minidump/CICGTest.cpp

```
--- a/server/src/unittest/gpopt/minidump/CICGTest.cpp
+++ b/server/src/unittest/gpopt/minidump/CICGTest.cpp
@@ -217,6 +217,7 @@ const CHAR *rgszFileNames[] =
                "data/dxl/minidump/EffectsOfJoinFilter.mdp",
                "data/dxl/minidump/Join-IDF.mdp",
                "data/dxl/minidump/CoerceToDomain.mdp",
+               "data/dxl/minidump/Mytest.mdp",
                "data/dxl/minidump/LeftOuter2InnerUnionAllAntiSemiJoin.mdp",
 #ifndef GPOS_DEBUG
                // TODO:  - Jul 14 2015; disabling it for debug build to reduce testing time
```

Alternatively, it could also be added to the `tap-schedule.txt` file as follows:
```
--- a/src/backend/gporca/tap-schedule.txt
+++ b/src/backend/gporca/tap-schedule.txt
@@ -448,6 +448,7 @@ data/dxl/minidump/PartTbl-SubqueryOuterRef.mdp
 data/dxl/minidump/PartTbl-CSQ-PartKey.mdp
 data/dxl/minidump/PartTbl-CSQ-NonPartKey.mdp
 data/dxl/minidump/PartTbl-AggWithExistentialSubquery.mdp
+data/dxl/minidump/Mytest.mdp
 
 #CPartTbl6Test
 data/dxl/minidump/PartTbl-PredicateWithCast.mdp
```

<a name="updatetest"></a>
## Update tests

In some situations, a failing test does not necessarily imply that the fix is
wrong. Occasionally, existing tests need to be updated. There is now a script
that allows for users to quickly and easily update existing mdps. This script
takes in a logfile that it will use to update the mdps. This logfile can be
obtained from running `gporca_prove` as shown below.

Existing minidumps can be updated by running the following:


1. Run `./gporca_prove -j8 --state=save`.

2. If there are failing tests, run
```
./gporca_prove --state=failed -D > /tmp/failures.out
```

3. The output file can then be used with the `fix_mdps.py` script.
```
./scripts/fix_mdps.py /tmp/failures.out
```
Note: This will overwrite existing mdp files. This is best used after
committing existing changes, so you can more easily see the diff.
Alternatively, you can use `scripts/fix_mdps.py --dryRun` to not change
mdp files

4. Ensure that all changes are valid and as expected.

# Advanced Setup

## Explicitly Specifying Xerces For Build

If you want to build with a custom version of Xerces, is recommended to use the
`--prefix` option to the Xerces-C configure script to install Xerces in a
location other than the default under `/usr/local/`, because you may have other
software that depends on the platform's version of Xerces-C. Installing in a
non-default prefix allows you to have GP-Xerces installed side-by-side with
unpatched Xerces without incompatibilities.

<a name="contribute"></a>
# How to Contribute

We accept contributions via [Github Pull requests](https://help.github.com/articles/using-pull-requests) only.


ORCA has a [style guide](StyleGuilde.md), try to follow the existing style in your contribution to be consistent.

[clang-format]: https://clang.llvm.org/docs/ClangFormat.html
A set of [clang-format]-based rules are enforced in CI. Your editor or IDE may automatically support it. When in doubt, check formatting locally before submitting your PR:

```
CLANG_FORMAT=clang-format src/tools/fmt chk
```

For more information, head over to the [formatting README](README.format.md).

We follow GPDB's comprehensive contribution policy. Please refer to it [here](https://github.com/greenplum-db/gpdb#contributing) for details.

