A shell line `while pgrep -f "PATTERN"; do sleep; done; next` never ends when PATTERN also appears in the line itself, because `pgrep -f` matches the waiting shell's own command line. `pkill -f PATTERN` kills that shell for the same reason.

Twice in the router comparison a queued batch waited on itself, and once a kill meant for one Python process took out the wrapper that was to start the next batch. The ORS runs lost about forty minutes. Wait on a PID (`wait`, or `while kill -0 PID`), or run the steps in one script where the next step simply follows.
