#!/bin/sh
# https://tinyapps.org/blog/201701240700_convert_asciidoc_to_markdown.html
asciidoc -b docbook restDocu.adoc
#pandoc -f docbook -t markdown_strict restDocu.xml -o restDocu.md
#sed -i "s/\*\([^\*]\+\)\*\([^\*]\)/'\1'\2/g" restDocu.md

asciidoc -b docbook restDocuV2.adoc
#pandoc -f docbook -t markdown_strict restDocuV2.xml -o restDocuV2.md
#sed -i "s/\*\([^\*]\+\)\*\([^\*]\)/'\1'\2/g" restDocuV2.md
echo "In beiden xml-Dateien noch "
echo "    linenumbering=\"options=\"nowrap\"\""
echo "und "
echo "    language=\"options=\"nowrap\"\""
echo "durch"
echo "    linenumbering=\"unnumbered\" options=\"unwrap\"\""
echo "und "
echo "    language=\"json\" options=\"nowrap\""
echo "ersetzen! "
echo "....................."
echo "Dann "
echo "    pandoc -f docbook -t markdown_strict restDocu.xml -o restDocu.md"
echo "    sed -i \"s/\*\([^\*]\+\)\*\([^\*]\)/'\1'\2/g\" restDocu.md"
echo "und "
echo "    pandoc -f docbook -t markdown_strict restDocuV2.xml -o restDocuV2.md"
echo "    sed -i \"s/\*\([^\*]\+\)\*\([^\*]\)/'\1'\2/g\" restDocuV2.md"
echo "ausführen."

