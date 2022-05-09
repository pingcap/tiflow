#!/bin/bash

set -eu

IFS=""

# eg. drop table xxx; drop table if exists xxx; drop table if exists xxx,xxx,xxx;
#   drop table xxx, xxx, xxx ;
function rewrite() {
	orgsql=$1
	index=$2

	# do not rewrite `drop table if exists`
	if [[ $(echo $orgsql | grep -i "if exists") != "" ]]; then
		echo $orgsql
		return
	fi

	table=
	# split `drop table` and extract table name part
	IFS=' ' read -ra words <<<"$orgsql"
	rename="rename table"
	wl=${#words[@]}
	for ((i = 2; i < wl; i++)); do
		table=${words[i]}
		# remove all chars after `;`
		table=${table%;*}
		# deal the `xxx,xxx,xxx` or `xxx,` or `xxx,xxx` or `,`
		if [[ $(echo $table | grep -i ",") != "" ]]; then
			IFS=',' read -ra tbs <<<"$table"
			for tb in ${tbs[@]}; do
				rename="$rename $tb to $tb""_""$index,"
			done
		else
			rename="$rename $table to $table""_""$index,"
		fi
		# meet the end, just break
		table=${words[i]}
		if [[ $(echo $table | grep -i ";") != "" ]]; then
			break
		fi
	done
	# remove last `,`
	len=$((${#rename} - 1))
	rename=${rename:0:$len}
	rename="$rename;"

	echo $rename
}

# convert will add `create database` and `use xxx` when meet `drop table`
function convert() {
	infile=$1
	outfile=$2
	rm -f $outfile
	# extract file without path
	file=${infile##*/}
	# extract filename
	filename=${file%.*}
	i=1
	((i = i + 1))
	if [[ $(echo $file | grep -i "\.test") != "" ]]; then
		# deal the test file
		preline=
		while read line; do
			#echo "line:$line"
			#(1) we recognize the `drop table` and add `create database`
			#(2) if we meet `--error` before `drop table`, it's test, keep the `drop table`
			if [[ $(echo $line | grep -i "^drop table") != "" && $(echo $preline | grep -i "error") == "" ]]; then
				rename=$(rewrite $line $i)
				echo "$rename" >>$outfile
				((i = i + 1))
			else
				echo "$line" >>$outfile
			fi
			preline=$line
		done <$infile
	else
		#deal the result file
		preline="fake cdc firstline"
		line=
		while read ll; do
			line=$ll
			#(1) we recognize the `drop table` and add `create database`
			#(2) if we meet `Error` after `drop table`, it's test and need keep the `drop table`
			#(3) do not rewrite multi-rows `drop table`
			if [[ $(echo $preline | grep -i "^drop table") != "" && $(echo $line | grep -i "^Error") == "" && $(echo $preline | grep -i ";") != "" ]]; then
				rename=$(rewrite $preline $i)
				echo "$rename" >>$outfile
				((i = i + 1))
			elif [[ $preline != "fake cdc firstline" ]]; then
				echo "$preline" >>$outfile
			fi
			preline=$line
		done <$infile
		# deal last line
		if [[ $(echo $line | grep -i "^drop table") != "" ]]; then
			rename=$(rewrite $preline $i)
			echo "$rename" >>$outfile
		else
			echo "$line" >>$outfile
		fi
	fi
	echo "update $infile success!"
}

# valid check if the new file and old file only diff at what we add
function valid() {
	infile=$1
	outfile=$2
	rm -f diff.log
	diff $infile $outfile >>diff.log
	if [[ $(grep -v "create database\|use" diff.log | grep -v "[0-9]*,*" diff.log) != "" ]]; then
		echo "fail convert file:$infile, need check"
		return
	fi

	echo ""
	rm -f diff.log
}

# deal will read the test and result file to make convert
function deal() {
	testCase=$1
	testFile=$2
	resultFile=$3
	# new file
	testFileNew=$testFile".new"
	resultFileNew=$resultFile".new"
	# convert test and result file
	convert $testFile $testFileNew
	convert $resultFile $resultFileNew
	# check old file and new file
	if [[ $(valid $testFile $testFileNew) != "" ]]; then
		echo "file:$testFile convert fail. check the diff.log for more detail"
		exit 0
	fi
	if [[ $(valid $resultFile $resultFileNew) != "" ]]; then
		echo "file:$resultFile convert fail. check the diff.log for more detail"
		exit 0
	fi
	echo "successful convert for test case:$testCase"
	# rename the original file
	mv -i $testFile $testFile".back"
	mv -i $testFileNew $testFile
	mv -i $resultFile $resultFile".back"
	mv -i $resultFileNew $resultFile
}

testCases=$1
echo "test_cases:$testCases"

if [[ $# == 2 ]]; then
	cmd=$2
	echo "cmd:$cmd"
	if [[ $cmd = "recover" ]]; then
		rm -rf r
		mv r_bak r
		rm -rf t
		mv t_bak t
		exit 0
	fi
fi

mv r r_bak
mv t t_bak
mkdir r
mkdir t

# split input by blank
IFS=' ' read -ra allCases <<<"$testCases"
echo "allCases:$allCases"
for tc in ${allCases[*]}; do
	cp -f r_bak/$tc.result r/
	cp -f t_bak/$tc.test t/
	testFile="./t/$tc.test"
	resultFile="./r/$tc.result"
	echo "file:$testFile,$resultFile"
	deal $tc $testFile $resultFile
done
