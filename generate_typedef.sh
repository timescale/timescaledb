#!/bin/bash
gobjdump -W src/*.o |egrep -A3 DW_TAG_typedef |perl -e 'while (<>) { chomp; @flds = split;next unless (1 < @flds);\
     next if $flds[0]  ne "DW_AT_name" && $flds[1] ne "DW_AT_name";\
     next if $flds[-1] =~ /^DW_FORM_str/;\
     print $flds[-1],"\n"; }' |sort |uniq > typedef.list.local
wget -q -O - "http://www.pgbuildfarm.org/cgi-bin/typedefs.pl?branch=HEAD" |\
 cat - typedef.list.local | sort | uniq > typedef.list
rm typedef.list.local
