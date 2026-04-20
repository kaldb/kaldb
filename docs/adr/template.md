# ADR 0000: Short Title

> Copy this file to `docs/adr/NNNN-short-title.md` and replace the placeholders.
> If the proposal needs deep implementation detail, keep this ADR focused on the decision and link to a separate design doc.

## Status

Current state: `Draft` | `Accepted` | `Rejected` | `Superseded`

Discussion thread: `<link or n/a>`

Issue: `<link or n/a>`

PR: `<link or n/a>`

Supersedes: `<ADR id or n/a>`

Superseded by: `<ADR id or n/a>`

## Motivation

_Describe the problem this ADR is trying to solve. Explain why the change is needed now, what constraints matter, and what would happen if the system stayed as-is._

## Questions

_List the key questions this ADR needs to answer. If there are no open questions, say "None." If there are questions with proposed answers, capture both the question and the answer clearly._

Suggested format:

- Question: `<question>`
  Answer: `<answer, decision, or status>`

## Public Interfaces

_List any externally visible, operational, or compatibility-sensitive changes introduced by this proposal. If there are none, say "None."_

Consider whether this ADR changes any of the following:

- APIs, RPCs, request or response shapes
- Config files, flags, environment variables, or defaults
- Metadata formats, schemas, or persisted state
- Metrics, logs, alerts, or dashboards
- CLI tools or operator workflows
- Upgrade behavior or rollback behavior
- User-visible or query-visible behavior

## Proposed Changes

_Describe the proposed design in enough detail for reviewers to evaluate it. Use subsections when the change is non-trivial._

Suggested subsections:

- Summary
- Detailed design
- Rollout or phases
- Open questions

## Compatibility, Deprecation, and Migration Plan

_Describe the effect on existing users, operators, stored metadata, and running clusters._

Address the following when relevant:

- What breaks, changes, or remains compatible?
- Are any old behaviors being deprecated?
- Is migration required?
- Can the change be rolled back safely?
- When, if ever, will old behavior be removed?

## Test Plan

_Describe how the change will be validated. Focus on how we will know the system works and how we will know nothing important regressed._

Consider:

- Unit tests
- Integration tests
- End-to-end or system tests
- Upgrade or rollback tests
- Failure-mode testing

## Documentation Plan

_List the documentation that should be added or updated if this ADR is implemented._

Consider:

- `README.md`
- `docs/topics/`
- Runbooks
- Config reference
- API docs

## Rejected Alternatives

_List the main alternatives considered and why they were not chosen._

## Consequences

_Summarize the expected outcomes and tradeoffs of this decision. Include both benefits and costs._
