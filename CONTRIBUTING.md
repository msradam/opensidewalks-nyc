# Contributing to OpenSidewalks NYC

OpenSidewalks NYC is an independent, experimental dataset. The two most useful things a person can do for it are to tell the project where the data is wrong and to improve OpenStreetMap, which is where most of the data comes from. Code and documentation changes are welcome too. Read the [Disclaimer](README.md#disclaimer) first: nothing here has been checked on the ground.

## Report a data error

Open a [data error issue](https://github.com/msradam/opensidewalks-nyc/issues/new?template=data-error.yml). The template asks for three things: where (a street corner, an address or coordinates), what is wrong, and how you know (you were there, you looked at imagery, you compared it with another map). A GitHub account is needed, and what you write there is public.

Before you report, it helps to know where a value comes from, because that decides who can fix it:

- Sidewalks, crossings, steps, plazas and streets come from OpenStreetMap. A sidewalk nobody has mapped yet, or a crossing drawn in the wrong place, is a gap or an error in OpenStreetMap, and the fix belongs there (see the next section). An issue here is still welcome, because it tells the project where the map is thin.
- Curb ramps and their slopes come from the NYC DOT ramp survey, captured mostly in 2018. A ramp that has been rebuilt since is still shown as it was. The project cannot change the survey, but it can record the disagreement.
- Incline and width are estimates from LiDAR and the city's sidewalk polygons. Tell the project where they look wrong, with the place and what you saw.
- Everything else (a field that is missing, a count that looks off, a route that should not exist) is this project's own doing, and a report fixes it fastest.

## Help in OpenStreetMap

Most of the network is OpenStreetMap, so the best way to improve the dataset is to improve the map, following OpenStreetMap's own conventions. Do not tag anything for this project. Map what is on the ground, the way the OpenStreetMap wiki's [Sidewalks](https://wiki.openstreetmap.org/wiki/Sidewalks) and [crossing](https://wiki.openstreetmap.org/wiki/Tag:highway%3Dcrossing) pages describe it, and the next build will read it. A build reads one dated extract, pinned in `config/sources.yaml`, so an edit appears in the dataset when the project moves to a newer extract, which happens with a release, not on a schedule.

What the pipeline reads:

- Sidewalks drawn as their own ways (`highway=footway` + `footway=sidewalk`) become Sidewalk Edges, and crossings (`footway=crossing`) become Crossing Edges. Much of the Bronx, Queens and Staten Island has streets with no sidewalk mapped in any form.
- `sidewalk=*` and `sidewalk:left`, `sidewalk:right` and `sidewalk:both` tags on a street are carried on the street edge. A street tagged as having a sidewalk is walked by this project's wheelchair profile; one tagged `no` or `separate` is not.
- `barrier=kerb` and `kerb=*` nodes, with `tactile_paving`, become curb nodes, and `highway=elevator` nodes are carried so a route can change level there.
- `crossing:markings`, `surface`, `width`, `foot`, `bridge`, `tunnel` and `layer` are read on ways. `wheelchair`, `smoothness` and `incline` are not read yet.

The [OpenStreetMap US](https://osmus.org/) community has a Slack with New York City and pedestrian channels where mappers coordinate. Groups such as BetaNYC run mapping events that put community observations into OpenStreetMap.

## Run the pipeline or the notebook

The notebook [`notebooks/how-it-works.ipynb`](notebooks/how-it-works.ipynb) follows a few blocks through every stage and runs in under a minute without a build. It reads the latest release's FlatGeobuf over the network, or a local copy through the `OSW_NYC_FGB` environment variable. A rendered copy is at [How it is built](https://msradam.github.io/opensidewalks-nyc/how-it-works.html).

A full build re-downloads every source and takes about an hour, 35 GB of memory and 30 GB of disk:

```bash
uv venv --python 3.11 && source .venv/bin/activate
uv pip install -e .
python -m pipeline build
python scripts/snap_endpoints.py --input output/nyc-osw.geojson
```

For a quick build, uncomment the `study_area` block in `config/build.yaml` and run `python -m pipeline clean` first. [METHODOLOGY.md](METHODOLOGY.md) describes each stage, [SCHEMA.md](SCHEMA.md) every field, and [scripts/README.md](scripts/README.md) the release files.

## Change the code or the documents

Open an issue or a pull request. Keep the change small and say what it fixes. Each pipeline change gets a test that fails without it, in the style of the files in `tests/` (plain functions, run with `python tests/<file>.py`), and a measurement on the whole graph before and after when it changes the data. Run the tests you touched before you open the pull request.

Commit messages follow the usual Git form: an imperative subject line under 50 characters and a body that says why. Prose in documents and comments is plain: short sentences, no dashes used as punctuation, and no claim without a number or a file behind it. The project does not add tool or model co-authorship lines to commits; `NOTICE` says how language models were used.

The data is ODbL-1.0 because it is derived from OpenStreetMap, and the code is Apache-2.0. A contribution is accepted under the same terms.
