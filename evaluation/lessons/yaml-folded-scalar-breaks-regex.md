A YAML folded scalar (`>`) joins its lines with a space, so a regex split across two lines gains a space in the middle.

The OSM tag filter read "tertiary| secondary", which matches no highway value, so builds before commit `566d78c` (2026-10-02) had no secondary road Edges and nothing complained. v0.3.3 and later have them. The filter is now one line in `config/sources.yaml`. Check a filter by counting what it returns for each value it names.
