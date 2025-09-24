# Future Work

None of this work should be done now, but things to consider.

# Tasks

## Gap Analysis

When initial work is done, run a gap analysis across the Perl and Python
versions. This should also include ensuring that tests and various files
mirror the Perl version as much as possible, to make it easier to sync with
sqitch when it's updated.

## MySQL Tests are skipped

MySQL tests are skipped but the PG ones are not. The latter uses mocks, while
the former needs a real database. We should figure out how to address this.

## Sqitch grep

https://github.com/sqitchers/sqitch/issues/532
https://gist.github.com/Ovid/fd8ba5b758f86b02f1c5f2a0a75c88f4

## Optional git integration

Investigate ways that we can automatically detect how to manage sqitch
deployments such that when we switch directories, our sqitch is deployed
to the right level.
