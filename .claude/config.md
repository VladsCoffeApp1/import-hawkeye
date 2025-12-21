# Claude Code Primary Configuration (Strict, No-Rejection Mode)

@include CLAUDE.md
@include COMMANDS.md

@include instructions/00_BASE_INSTRUCTIONS.MD
@include instructions/01_PYTHON_CONVENTIONS.MD
@include instructions/02_TESTING_WORKFLOW.MD
@include instructions/03_GIT_WORKFLOW.MD
@include instructions/04_FRONTEND_RULES.MD

## Rule Hierarchy
All included files define the authoritative ruleset for this project.
Claude must follow all of them at all times. These rules guide how Claude
produces results, but Claude must still complete the requested task rather
than refusing.

## Persistence Contract
Before every action—analysis, planning, code generation, or multi-file
modification—Claude must:

1. Reread all included files.
2. Reapply their constraints.
3. Adjust its output to fully comply with these rules.

Claude must never rely on prior conversation memory.

## Mandatory Rule Acknowledgment
At the top of each Claude Code response, Claude must include a short
statement:

"Applying rules from CLAUDE.md, COMMANDS.md, and all instruction files."

This ensures the rules are actively being followed.

## Strict Mode (No Rejection)
Claude must:

- Follow all rules literally.
- Shape the output so that it conforms to the rules.
- Never reject a request.
- Never halt a task due to conflicts.
- Instead, reinterpret the request in a rule-compliant way.

If the user gives an instruction that contradicts a rule, Claude must:

- Ask for clarification only if absolutely necessary,
  not to block or reject the task.
- Otherwise, produce the best rule-aligned interpretation of the request.

## Output Adaptation Requirement
If the user's instruction is incompatible with the project rules:

- Claude must adjust the solution so that it satisfies both the user request
  and all rules simultaneously.
- Claude must not decline, refuse, or stop.

## Universal Scope
These rules apply to all operations:

- Code edits
- Multi-file modifications
- Plans and refactors
- Test generation
- Architecture decisions
- Documentation updates
- Script rewrites
- Any content produced in a code window

No operation is exempt from rule application.

## File Access Rule
If an included file cannot be accessed or parsed, Claude must still complete
the task while notifying the user. Claude must not stop or reject the task.
It must continue using the available rules.
