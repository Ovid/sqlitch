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

## flake8

Temporarily suppressed cyclomatic complexity errors. These will need to be
addressed:

	sqlitch/commands/add.py:97:5: C901 'AddCommand._parse_args' is too complex (31)
	sqlitch/commands/add.py:455:1: C901 'add_command' is too complex (14)
	sqlitch/commands/bundle.py:53:5: C901 'BundleCommand._parse_args' is too complex (14)
	sqlitch/commands/bundle.py:496:5: C901 'BundleCommand._find_change_index' is too complex (13)
	sqlitch/commands/checkout.py:129:5: C901 'CheckoutCommand._parse_args' is too complex (27)
	sqlitch/commands/deploy.py:69:5: C901 'DeployCommand._parse_args' is too complex (22)
	sqlitch/commands/deploy.py:341:5: C901 'DeployCommand._deploy_changes' is too complex (16)
	sqlitch/commands/deploy.py:454:5: C901 'DeployCommand._deploy_changes_with_feedback' is too complex (11)
	sqlitch/commands/init.py:69:5: C901 'InitCommand._parse_args' is too complex (29)
	sqlitch/commands/log.py:95:5: C901 'LogCommand._parse_args' is too complex (61)
	sqlitch/commands/log.py:407:1: C901 'log_command' is too complex (12)
	sqlitch/commands/rebase.py:73:5: C901 'RebaseCommand._parse_args' is too complex (47)
	sqlitch/commands/rebase.py:501:1: C901 'rebase_command' is too complex (18)
	sqlitch/commands/revert.py:80:5: C901 'RevertCommand._parse_args' is too complex (24)
	sqlitch/commands/revert.py:349:5: C901 'RevertCommand._revert_changes' is too complex (15)
	sqlitch/commands/show.py:29:5: C901 'ShowCommand.execute' is too complex (14)
	sqlitch/commands/show.py:142:5: C901 'ShowCommand._show_change_or_script' is too complex (14)
	sqlitch/commands/status.py:83:5: C901 'StatusCommand._parse_args' is too complex (14)
	sqlitch/commands/tag.py:50:5: C901 'TagCommand._parse_args' is too complex (12)
	sqlitch/commands/tag.py:106:5: C901 'TagCommand._add_tag' is too complex (13)
	sqlitch/commands/verify.py:106:5: C901 'VerifyCommand._parse_args' is too complex (23)
	sqlitch/commands/verify.py:688:1: C901 'verify_command' is too complex (11)
	sqlitch/core/config.py:369:5: C901 'Config.get_target' is too complex (12)
	sqlitch/core/plan.py:97:5: C901 'Plan._parse_content' is too complex (11)
	sqlitch/core/plan.py:191:5: C901 'Plan._parse_change' is too complex (13)
	sqlitch/core/sqitch.py:62:5: C901 'Sqitch._get_user_name' is too complex (11)
	sqlitch/core/target.py:76:5: C901 'Target.from_config' is too complex (18)
	sqlitch/engines/base.py:535:5: C901 'Engine.search_events' is too complex (16)
	sqlitch/engines/base.py:1240:5: C901 'EngineRegistry.revert' is too complex (13)
	sqlitch/engines/base.py:1303:5: C901 'EngineRegistry.deploy' is too complex (15)
	sqlitch/engines/firebird.py:272:5: C901 'FirebirdEngine._create_connection' is too complex (11)
	sqlitch/engines/firebird.py:598:5: C901 'FirebirdEngine.search_events' is too complex (16)
	sqlitch/engines/mysql.py:328:5: C901 'MySQLEngine._parse_connection_string' is too complex (20)
	sqlitch/engines/mysql.py:529:5: C901 'MySQLEngine._split_sql_statements' is too complex (11)
	sqlitch/engines/snowflake.py:323:5: C901 'SnowflakeEngine._create_connection' is too complex (12)
	sqlitch/engines/vertica.py:330:5: C901 'VerticaEngine._create_connection' is too complex (11)
	sqlitch/i18n/date_time.py:46:5: C901 'LocaleAwareDateTimeFormatter.format_datetime' is too complex (14)
	sqlitch/i18n/extract_messages.py:38:5: C901 'MessageExtractor.visit_Call' is too complex (11)
	sqlitch/utils/feedback.py:256:1: C901 'format_error_with_suggestions' is too complex (11)
	sqlitch/utils/formatter.py:281:5: C901 'ItemFormatter._format_date' is too complex (12)
	sqlitch/utils/progress.py:394:1: C901 'confirm_action' is too complex (12)

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

Quite often Kiro will run something in the terminal which runs in a pager, or
piped to less or something. When that happens, Kiro cheerfully blocks and you
have to manually exit the pager to allow Kiro to continue. However, you don't
want to hit 'escape' because that might terminate the pager *before* it emits
the output that Kiro is looking for. Oops.
