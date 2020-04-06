## CONTRIBUTING

Please raise GitHub issues for any questions so others can benefit from the discussion.

This is an Open Source project for many reasons, with some central goals:

- quality reference code (the code should be clean and easy to read; where it makes sense, it should remain possible to clone it locally and use it in tweaked form)
- optimal resilience and performance (getting performance right can add huge value for some systems, i.e., making it prettier but disrupting the performance would be bad)
- this code underpins non-trivial production systems (having good tests is not optional for reasons far deeper than looking at code coverage stats)

We'll do our best to be accommodating to PRs and issues, but please appreciate that [we emphasize decisiveness for the greater good of the project and its users](https://www.hanselman.com/blog/AlwaysBeClosingPullRequests.aspx); _new features [start with -100 points](https://blogs.msdn.microsoft.com/oldnewthing/20090928-00/?p=16573)_.

Within those constraints, contributions of all kinds are welcome:

- raising [Issues](https://github.com/jet/propulsion/issues) is always welcome (but we'll aim to be decisive in the interests of keeping the list navigable).
- bugfixes with good test coverage are always welcome; in general we'll seek to move them to NuGet prerelease and then NuGet release packages with relatively short timelines (there's unfortunately not presently a MyGet feed for CI packages rigged).
- improvements / tweaks, _subject to filing a GitHub issue outlining the work first to see if it fits a general enough need to warrant adding code to the implementation and to make sure work is not wasted or duplicated_
