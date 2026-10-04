Paging a Socrata dataset with $limit and $offset and no $order can return some rows twice and skip others, while the row count still looks right.

A Staten Island pull of 23,326 ramp rows held only 16,664 distinct ramps: 6,662 came twice and 6,662 were missing. Nothing failed, the build just attached fewer ramps (6,619 against the release's 8,996), and an earlier audit's Staten Island ramp numbers were computed on that download. Order by `:id`, count distinct `:id` values, and compare with `count(*)`. Evidence: research_notes/release/ramp_gap/gap5.json, refetch/refetch.json.
