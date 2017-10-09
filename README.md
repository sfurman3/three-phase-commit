# three-phase-commit
- To run the test cases just execute the `./build` script and run the grading script.
  + All of the test cases that I wrote begin with an underscore (`_`).
  + Note: I did all of my testing with my own test script (called `test`). It
    should work identically.

- 3pc works for the most part, except for the total failure stuff. That stuff
  definitely doesn't work :)
  + One quirk is that processes that don't have a song in their local playlist
    will forward the request to other servers. If they get a response, they'll
    write that song to their DT log as "commit add song url" and return the
    value.

# extra credit question
- Happy Columbus Day.
