Splitting a graph file by the borough of its edges leaves out every node that no edge references.

In v0.3.1-nyc.1 that was every unattached curb ramp, about a third of them, and it made the Staten Island split look as if all its 8,996 curb nodes were attached. Give an unreferenced node to the borough in its own tag, and check that the splits add up to the whole file.
