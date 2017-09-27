#! /bin/sh
#-------------------------------------------------------------------------
#
# Gen_fmgrtab.sh
#    shell script to generate fmgroids.h and fmgrtab.c from pg_proc.h
#
# NOTE: if you change this, you need to fix Gen_fmgrtab.pl too!
#
<<<<<<< HEAD
# Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
=======
# Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
>>>>>>> 49f001d81e
# Portions Copyright (c) 1994, Regents of the University of California
#
#
# IDENTIFICATION
<<<<<<< HEAD
#    $PostgreSQL: pgsql/src/backend/utils/Gen_fmgrtab.sh,v 1.41 2009/01/01 17:23:48 momjian Exp $
=======
#    $PostgreSQL: pgsql/src/backend/utils/Gen_fmgrtab.sh,v 1.40 2008/06/23 17:54:29 tgl Exp $
>>>>>>> 49f001d81e
#
#-------------------------------------------------------------------------

CMDNAME=`basename $0`

if [ x"$AWK" = x"" ]; then
	AWK=awk
fi

cleanup(){
    [ x"$noclean" != x"t" ] && rm -f "$SORTEDFILE" "$$-$OIDSFILE" "$$-$TABLEFILE"
}

noclean=

#
# Process command line switches.
#
while [ $# -gt 0 ]
do
    case $1 in
        --noclean)
            noclean=t
            ;;
        --help)
            echo "$CMDNAME generates fmgroids.h and fmgrtab.c from pg_proc.h."
            echo
            echo "Usage:"
            echo "  $CMDNAME inputfile"
            echo
            echo "The environment variable AWK determines which Awk program"
            echo "to use. The default is \`awk'."
            echo
            echo "Report bugs to <bugs@greenplum.org>."
            exit 0
            ;;
        -*)
            echo "$CMDNAME: invalid option: $1"
            exit 1
            ;;
        *)
            INFILE=$1
            ;;
    esac
    shift
done


if [ x"$INFILE" = x ] ; then
    echo "$CMDNAME: no input file"
    exit 1
fi

SORTEDFILE="$$-fmgr.data"
OIDSFILE=fmgroids.h
TABLEFILE=fmgrtab.c


trap 'echo "Caught signal." ; cleanup ; exit 1' 1 2 15

#
# Collect the column numbers of the pg_proc columns we need.  Because we will
# be looking at data that includes the OID as the first column, add one to
# each column number.
#
proname=`egrep '^#define Anum_pg_proc_proname[ 	]' $INFILE | $AWK '{print $3+1}'`
prolang=`egrep '^#define Anum_pg_proc_prolang[ 	]' $INFILE | $AWK '{print $3+1}'`
proisstrict=`egrep '^#define Anum_pg_proc_proisstrict[ 	]' $INFILE | $AWK '{print $3+1}'`
proretset=`egrep '^#define Anum_pg_proc_proretset[ 	]' $INFILE | $AWK '{print $3+1}'`
pronargs=`egrep '^#define Anum_pg_proc_pronargs[ 	]' $INFILE | $AWK '{print $3+1}'`
prosrc=`egrep '^#define Anum_pg_proc_prosrc[ 	]' $INFILE | $AWK '{print $3+1}'`

#
# Generate the file containing raw pg_proc data.  We do three things here:
# 1. Strip off the DATA macro call, leaving procedure OID as $1
# and all the pg_proc field values as $2, $3, etc on each line.
# 2. Fold quoted fields to simple "xxx".  We need this because such fields
# may contain whitespace, which would confuse awk's counting of fields.
# Fortunately, this script doesn't need to look at any fields that might
# need quoting, so this simple hack is sufficient.
# 3. Select out just the rows for internal-language procedures.
#
# Note assumption here that INTERNALlanguageId == 12.
#
egrep '^DATA' $INFILE | \
sed 	-e 's/^[^O]*OID[^=]*=[ 	]*//' \
	-e 's/(//' \
	-e 's/"[^"]*"/"xxx"/g' | \
$AWK "\$$prolang == \"12\" { print }" | \
sort -n > $SORTEDFILE

if [ $? -ne 0 ]; then
    cleanup
    echo "$CMDNAME failed"
    exit 1
fi


cpp_define=`echo $OIDSFILE | tr abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ | sed -e 's/[^ABCDEFGHIJKLMNOPQRSTUVWXYZ]/_/g'`

#
# Generate fmgroids.h
#
cat > "$$-$OIDSFILE" <<FuNkYfMgRsTuFf
/*-------------------------------------------------------------------------
 *
 * $OIDSFILE
 *    Macros that define the OIDs of built-in functions.
 *
 * These macros can be used to avoid a catalog lookup when a specific
 * fmgr-callable function needs to be referenced.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * NOTES
 *	******************************
 *	*** DO NOT EDIT THIS FILE! ***
 *	******************************
 *
 *	It has been GENERATED by $CMDNAME
 *	from $INFILE
 *
 *-------------------------------------------------------------------------
 */
#ifndef $cpp_define
#define $cpp_define

/*
 *	Constant macros for the OIDs of entries in pg_proc.
 *
 *	NOTE: macros are named after the prosrc value, ie the actual C name
 *	of the implementing function, not the proname which may be overloaded.
 *	For example, we want to be able to assign different macro names to both
 *	char_text() and name_text() even though these both appear with proname
 *	'text'.  If the same C function appears in more than one pg_proc entry,
 *	its equivalent macro will be defined with the lowest OID among those
 *	entries.
 */
FuNkYfMgRsTuFf

tr 'abcdefghijklmnopqrstuvwxyz' 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' < $SORTEDFILE | \
$AWK "{ if (seenit[\$$prosrc]++ == 0)
	printf \"#define F_%s %s\\n\", \$$prosrc, \$1; }" >> "$$-$OIDSFILE"

if [ $? -ne 0 ]; then
    cleanup
    echo "$CMDNAME failed"
    exit 1
fi

cat >> "$$-$OIDSFILE" <<FuNkYfMgRsTuFf

#endif /* $cpp_define */
FuNkYfMgRsTuFf

#
# Generate fmgr's built-in-function table.
#
# Print out the function declarations, then the table that refers to them.
#
cat > "$$-$TABLEFILE" <<FuNkYfMgRtAbStUfF
/*-------------------------------------------------------------------------
 *
 * $TABLEFILE
 *    The function manager's table of internal functions.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * NOTES
 *
 *	******************************
 *	*** DO NOT EDIT THIS FILE! ***
 *	******************************
 *
 *	It has been GENERATED by $CMDNAME
 *	from $INFILE
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/fmgrtab.h"

FuNkYfMgRtAbStUfF

$AWK "{ if (seenit[\$$prosrc]++ == 0)
	print \"extern Datum\", \$$prosrc, \"(PG_FUNCTION_ARGS);\"; }" $SORTEDFILE >> "$$-$TABLEFILE"

if [ $? -ne 0 ]; then
    cleanup
    echo "$CMDNAME failed"
    exit 1
fi


cat >> "$$-$TABLEFILE" <<FuNkYfMgRtAbStUfF

const FmgrBuiltin fmgr_builtins[] = {
FuNkYfMgRtAbStUfF

# Note: using awk arrays to translate from pg_proc values to fmgrtab values
# may seem tedious, but avoid the temptation to write a quick x?y:z
# conditional expression instead.  Not all awks have conditional expressions.

$AWK "BEGIN {
    Bool[\"t\"] = \"true\";
    Bool[\"f\"] = \"false\";
}
{ printf (\"  { %d, \\\"%s\\\", %d, %s, %s, %s },\\n\"),
	\$1, \$$prosrc, \$$pronargs, Bool[\$$proisstrict], Bool[\$$proretset], \$$prosrc ;
}" $SORTEDFILE >> "$$-$TABLEFILE"

if [ $? -ne 0 ]; then
    cleanup
    echo "$CMDNAME failed"
    exit 1
fi

cat >> "$$-$TABLEFILE" <<FuNkYfMgRtAbStUfF
  /* dummy entry is easier than getting rid of comma after last real one */
  /* (not that there has ever been anything wrong with *having* a
     comma after the last field in an array initializer) */
  { 0, NULL, 0, false, false, NULL }
};

/* Note fmgr_nbuiltins excludes the dummy entry */
const int fmgr_nbuiltins = (sizeof(fmgr_builtins) / sizeof(FmgrBuiltin)) - 1;
FuNkYfMgRtAbStUfF

# We use the temporary files to avoid problems with concurrent runs
# (which can happen during parallel make).
mv "$$-$OIDSFILE" $OIDSFILE
mv "$$-$TABLEFILE" $TABLEFILE

cleanup
exit 0
