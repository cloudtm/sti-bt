#!/bin/bash

FILES=$@;
MAX_NR_NODES=0;

for file in ${FILES}; do
  #it is equals to the number of lines with data
  awk 'BEGIN {
nrNodes = 0;
throughput = 0;
numberOfTx = 0;
numberOfAbort = 0;
totalCommitDuration = 0;
reqPerSeqIdx = -1;
writeCountIdx = -1;
readCountIdx = -1;
failuresIdx = -1;
commitDurationIdx = -1;
}
{
if (match($0, "SLAVE") != 0) {
size = split($0, res, ",");
for (n = 1; n < size; ++n) {
if (res[n] == "REQ_PER_SEC") {reqPerSeqIdx = n; continue;}
if (res[n] == "WRITE_COUNT") {writeCountIdx = n; continue;}
if (res[n] == "READ_COUNT") {readCountIdx = n; continue;}
if (res[n] == "FAILURES") {failuresIdx = n; continue;}
if (res[n] == "AVG_ALL_COMMIT_DURATION") {commitDurationIdx = n; continue;}
}
next;
}
nrNodes++;
split($0, res2, ","); 
throughput += res2[reqPerSeqIdx];
numberOfTx += res2[writeCountIdx];
numberOfTx += res2[readCountIdx];
numberOfAbort += res2[failuresIdx];
totalCommitDuration += res2[commitDurationIdx];
} 
END {totalCommitDuration /= nrNodes; print nrNodes"\t"throughput"\t"(numberOfAbort*100/(numberOfAbort+numberOfTx))"\t"(totalCommitDuration/1000)}' ${file}
done   

