# Requests for Comments

The procedure is loosely based on the procedure used for [Rust
RFCs](https://github.com/rust-lang/rfcs). 

Requests for comments is intended to be a consistent approach to
discussing and proposing changes to TimescaleDB and allow feedback on
the change before it is implemented.

Note that many smaller fixes can be handled as a simple issue using
the normal GitHub workflow but the RFCs are intended to support more
substantial changes that might require a wider audience and broader
discussion.

Changes that require an RFC:
- Semantic or syntactic changes that are not a bugfix.
- Adding or removing features.

As a guideline, any code change that is not a bugfix but would require
updating the documentation should be an RFC.

Changes that do not require an RFC:
- Documentation updates
- Refactorings that do not change semantics nor require documentation
  updates.
- Addition, removal, or update of internal functions. These are mostly
  focused on developers and for that reason an RFC is not necessary.

## Submitting an RFC

1. Copy `0000-template.md` into `0000-new-feature.md`. Do not assign
   an RFC number and just use `0000` for the number.
2. Fill in the details in the RFC. Please be careful about giving
   enough context so that the motivation for the RFC can be
   understood. In particular, example usage will help a lot as well as
   a description of the current situation to understand what it
   intends to solve.
3. Create a pull request. This will be discussed and you should be
   prepared to handle feedback on the proposal.
4. Once all comments have been handled and discussed, create an issue
   with the contents of the proposal.
5. Use the issue number of the created issue to rename your RFC, e.g.,
   `1234-new-feature.md` in the pull request and fill in the issue
   number in the RFC.
6. Update the pull request message and the commit to include `RFC for
   #1234` so that it is easy to find the discussions about the feature
   once starting to work with the issue.

## Implementing an RFC

An RFC does not have to picked up directly, and depending on
priorities it might be picked up at a later time. Since each RFC has
an issue, it will be easy to find the associated issue and prioritize
and work with it using normal GitHub workflows.

If you decide to work on an RFC, you should assign yourself to
it. Since this is occationally forgotten, it might be prudent to add a
comment to the issue and check if anybody is already working on it, if
you're not sure.



