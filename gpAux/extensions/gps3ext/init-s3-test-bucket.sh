#!/bin/bash
#
# Populate a test S3 server and bucket, for testing purposes.
#
# Set AWS_ACCESSID and AWS_SECRET environment variables before calling
# (unless you're using a fake server that doesn't require valid credentials
#
# This can be used with real S3. Or you can test locally, with a fake S3
# server, by setting the "endpoint" in upload.sh to point to a local server.
# I have used the tool from https://github.com/jubos/fake-s3 successfully.
#

cat >/tmp/emptyfile <<EOF
EOF

cat >/tmp/smallfile1 <<EOF
1	ssf
2	abc
3	deefe3
4	e90jg
5	br t34
6	3r3r
7	ccfee4
8	hbafe
EOF

cat >/tmp/smallfile2 <<EOF
101	foo
102	bar
EOF


# This file contains mostly numbers in the second column, except
# for a few rows. For testing errors.
rm -f /tmp/mostly_numbers
for ((i = 1; i < 1000; i++))
do
    echo -e "$i\t$i" >> /tmp/mostly_numbers
done
echo -e "1000\tnot-a-number" >> /tmp/mostly_numbers
for ((i = 1001; i <= 2000; i++))
do
    echo -e "$i\t$i" >> /tmp/mostly_numbers
done

cat >/tmp/mostly_numbers <<EOF
101	foo
102	bar
EOF


rm -f /tmp/1_mil_file
perl -e 'for my $i (1..1000000) { print "$i\tfoo$i\n" };' > /tmp/1_mil_file

# Create 5 small files, and one completely empty one, in bucket "smallbucket".

bucketname="gps3ext-test"

bash upload.sh /tmp/smallfile1 $bucketname "small-1"
bash upload.sh /tmp/smallfile2 $bucketname "small-2"
bash upload.sh /tmp/smallfile1 $bucketname "small-3"
bash upload.sh /tmp/smallfile2 $bucketname "small-4"
bash upload.sh /tmp/smallfile1 $bucketname "small-tiny"
bash upload.sh /tmp/emptyfile  $bucketname "small-empty"

# Upload the large file
bash upload.sh /tmp/1_mil_file $bucketname "largefile"

# Upload the mostly_numbers file
bash upload.sh /tmp/mostly_numbers $bucketname "mostly_numbers"




# Create Huge bucket with one 1 GB file
perl -e 'for my $i (1..10000000) { print "$i\tfooxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx$i\n" };' > /tmp/1gb_file

perl -e 'for my $i (1..1000000) { print "$i\tfooxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx$i\n" };' > /tmp/100mb_file

bash upload.sh /tmp/1gb_file hugebucket "1gb_file"
