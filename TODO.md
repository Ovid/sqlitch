# Future Work

None of this work should be done now, but things to consider.

# Tasks

## Docs

README references missing docs

## Python best practices

Does it follow them?

## Tutorials

Grab some tutorials from sqitch.org and run them to make sure they work.

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

# Issues

Never do all subtasks at once. One small error at the top and it cascades
across them. (https://github.com/kirodotdev/Kiro/issues/2980)

Watch the output carefully. If the code gives a poor error message (such as
showing an unterminated string at the end of the file), Kiro can get confused
(such as trying to `tail` the file to see the unterminated string, when the
error is hundreds of lines above it).

Sometimes Kiro just *stops*. It's not always clear why, so you tell it to
"continue". Sometimes it stays stopped and you have to start a new session.

Other times it stops and instead of "continue", I could paste in an error
message from the console and it wakes up.

No matter how clear your steering documents, Kiro will often forget them for a
long task. As a result, you need to keep an eye out. For example, my steering
docs explain that the full test suite must pass, with no warnings, before a
task is finished. Kiro does not care. I suspect this is due to context size
limiations in its only available model: Claude Sonnet 4. While it's been
bumped to 1 million tokens
(https://every.to/vibe-check/vibe-check-claude-sonnet-4-now-has-a-1-million-token-context-window),
not AI tool has access to that lovely window.

As a result of the above, anything you tell it to do at the start of a new
task, it tends to do. Things you tell it to do at the end of a task tend to be
forgotten. It would be nice if Kiro recognized it was finishing a task and
reread its steering files. (https://github.com/kirodotdev/Kiro/issues/3003)

Read your tasks *carefully*. I found that at one point, after creating a bunch
of excellent, independent tasks, that near the end of a long task list, it
seemed to get "lazy" and would bundle multiple related changes inside of a
single task and I'd have to ask it to separate them into separate tasks or
subtasks.

When it offers to "trust" a command, it often offers a choice between that and
shorter verions of the command, such as:

    python -m pytest tests/integration/test_bundle_integration.py -v
    python *

Obviously, I don't want `python *`, but I'm kinda OK with `python -m pytest *`
(still can be destructive...). But I have to add the latter version manually.

Sometimes after a task is complete and I merge it, the window still says it's
working on the task. I click to start a new task and the "completed" task is
marked as "Task execution aborted".

Also, having fun with Kiro just *stopping* because "Restarting the
terminal because the connection to the shell process was lost... " I wound up
restarting Kiro and it crashed. Restarted a second time and it worked, but
still kept getting the terminal error. Finally rebooted my Mac and it worked
:/

Sometimes it will have a command that says "Waiting on your input" and you can
click "run" to run it, but it doesn't run. You have to scroll up to the top of
the command and click the "Run" icon there and it works.
