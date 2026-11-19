# omp relay screens (hk 5413)

Captured from a private herdr server (its own HOME and socket, never a fleet
session) running omp 18.14.2 with no model signed in. The setup wizard was
skipped with Escape, and the composer was then typed into and submitted. Host
and user paths are replaced with `<HOME>` and `<PATH>`.

| File | What it shows |
|---|---|
| `idle-empty-visible.txt` | `agent read --source visible`, idle, empty composer (the bottom row is a bare `╰─`) |
| `typed-unsent-visible.txt` | the marker typed with no Enter; the composer row is `╰─ <marker>` |
| `typed-unsent-unwrapped.txt` | `--source recent-unwrapped` of the same screen; the composer row is still the last row |
| `typed-unsent-ansi.txt` | `pane read --format ansi` of the same screen; the composer text draws in its own colour |
| `submitted-visible.txt` | the marker submitted: echoed above the composer, which is empty; an error row, and the status stays idle |
| `submitted-unwrapped.txt` | `--source recent-unwrapped` of the submitted screen |

Read the composer as the last non-blank row. The relay's transcript is the rows
above it, so a typed row never reads as an echo.
