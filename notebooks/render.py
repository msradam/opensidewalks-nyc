"""Run how-it-works.ipynb and write it to demo/how-it-works.html for the website.

usage: python notebooks/render.py
"""
import re
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
NB = HERE / "how-it-works.ipynb"
HTML = HERE.parent / "demo" / "how-it-works.html"

# One per figure, in order. nbconvert has no field for alt text.
ALT = [
    "Map of OpenSidewalks Edges by type and Curb Ramp Nodes around West 181st Street.",
    "Map of surveyed curb ramp positions and the Edge endpoints they snap to, most within about a metre.",
    "Map of planimetric sidewalk polygons with each sidewalk Edge coloured by the width it takes from them.",
    "The terrain model of the area, and a scatter plot of recomputed against published incline lying close to the diagonal.",
    "Map of pedestrian Edges coloured by incline, steepest on the slopes toward the parks.",
    "Brooklyn Bridge promenade heights: the published deck rises to about 45 m while the terrain model below is near 0 m over the water.",
    "Map of one trip: the walking route takes stairs and the wheelchair route goes round.",
]

subprocess.run(["jupyter", "nbconvert", "--to", "notebook", "--execute", "--inplace", str(NB)], check=True)
subprocess.run(["jupyter", "nbconvert", "--to", "html", "--output-dir", str(HTML.parent),
                "--output", HTML.stem, str(NB)], check=True)

html = HTML.read_text()
placeholder = 'alt="No description has been provided for this image"'
assert html.count(placeholder) == len(ALT), f"{html.count(placeholder)} figures, {len(ALT)} descriptions"
for text in ALT:
    html = html.replace(placeholder, f'alt="{text}"', 1)
# nbconvert's default prompt and token colours fail WCAG AA contrast.
html = html.replace("</head>", """<style>
.jp-InputPrompt, .jp-OutputPrompt { color: #595959 !important; opacity: 1 !important; }
.highlight .c1 { color: #2f5f5f !important; }
.highlight .mi, .highlight .mf { color: #116611 !important; }
.highlight .ow { color: #7a1fc2 !important; }
.highlight .o { color: #4d4d4d !important; }
a { text-decoration: underline !important; }
</style></head>""", 1)
# pandas tables have no header scope; screen readers need it to read cells.
def scoped(table):
    head, _, body = table.group(0).partition("</thead>")
    th = re.compile(r"<th(?=[ >])")
    return th.sub('<th scope="col"', head) + "</thead>" + th.sub('<th scope="row"', body)
html = re.sub(r'<table border="1" class="dataframe">.*?</table>', scoped, html, flags=re.S)
html = html.replace("<title>how-it-works</title>", "<title>How opensidewalks-nyc is pieced together</title>")
HTML.write_text(html)
print("wrote", HTML)
