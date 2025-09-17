#!/bin/sh

export ENV_INPUT_PARQUET_FILENAME=./sample.parquet

geninput(){
	wikixgz=~/Downloads/enwiki-20250801-pages-articles-multistream-index.txt.gz

	test -f "${wikixgz}" || exec sh -c '
		echo sample index file missing.
		echo you should prepare something like that somehow.
		exit 1
	'

	which rs-splited2parquet | fgrep -q rs-splited2parquet || exec sh -c '
		echo rs-splited2parquet missing.
		echo you can install it using cargo install.
		exit 1
	'

	cat "${wikixgz}" |
		zcat |
		dd if=/dev/stdin of=/dev/stdout bs=1048576 status=progress |
		ENV_COLUMN_SIZE=3 ENV_DELIM=: rs-splited2parquet |
		cat > "${ENV_INPUT_PARQUET_FILENAME}"
}

test -f "${ENV_INPUT_PARQUET_FILENAME}" || geninput

./rs-parquet-row-group-stats | jq -c
