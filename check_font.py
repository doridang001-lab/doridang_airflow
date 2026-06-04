import pathlib, re

for n in ["slide13", "slide14"]:
    txt = pathlib.Path(fr"C:\airflow\pptx_unpacked2\ppt\slides\{n}.xml").read_text(encoding="utf-8")
    fonts = set(re.findall(r'typeface="([^"]+)"', txt))
    print(f"{n} fonts: {fonts}")
    has_neverland = "210 네버랜드" in txt
    print(f"  '210 네버랜드' in xml: {has_neverland}")
