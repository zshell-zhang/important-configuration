#!/bin/sh
# used to wipe all the java compiled .class files in target directory

echo "\n***************** wipe .class files in $1 ***********************\n"
cd "$1"

fun()
{
	for name in `ls`
	do
		if [ -d "$name" ]; then # if the file is directory then process this directory
			cd "$name"
			fun
			cd ..
		fi
	done
	for item in `ls | grep .class$`
	do
		echo "the file $item has been removed"
	done
	rm -f *.class # process the current directory
}

fun
echo "\n********** all the .class files in $1 has been removed **********\n"
