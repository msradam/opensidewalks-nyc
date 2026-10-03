python-osw-validation pins geopandas==0.14.4, so installing it next to the pipeline silently downgrades geopandas and Stage 1 fails.

Run the validator with uv run --no-project --isolated --with python-osw-validation. Also: uv run without --no-project inside the repo writes a uv.lock. Evidence: research_notes/evidence/repro_si/build_attempt1.log.
