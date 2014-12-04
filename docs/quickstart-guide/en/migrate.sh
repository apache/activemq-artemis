#!/bin/sh
for i in `find . -name "*.xml"`; do
	newFile=`echo $i | sed 's/xml/md/'`
	echo creating $newFile
	pandoc -f docbook -t markdown $i -o $newFile
done