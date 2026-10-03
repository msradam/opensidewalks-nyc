A YAML folded scalar (`>`) joins its lines with a space, so a regex split across two lines gains a space in the middle.

The OSM tag filter read "tertiary| secondary", which matches no highway value, so every build lacked secondary roads and nothing complained. Check a filter by counting what it returns for each value it names. Evidence: research_notes/release/si_neutral/secondary_filter.json.
