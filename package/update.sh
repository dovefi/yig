#!/bin/bash
ftp -n<<!
open 10.0.47.182
user smaug Zgydxcc2018
binary
hash
cd /pub/Untested_Packages/yig
mput ../*.x86_64.rpm
close
bye
!
