"""Repack pptx_unpacked2 into the final improved PPTX."""
import pathlib, zipfile

SRC = pathlib.Path(r"C:\airflow\pptx_unpacked2")
OUT = pathlib.Path(r"C:\Users\민준\OneDrive - 주식회사 도리당\data\report\PowerPoint\전사 DXAX 교육_260528_개선.pptx")

with zipfile.ZipFile(OUT, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for f in SRC.rglob("*"):
        if f.is_file():
            arc = f.relative_to(SRC).as_posix()
            zf.write(f, arc)

print(f"Packed {OUT}")
print(f"Slides inside:")
with zipfile.ZipFile(OUT) as zf:
    slides = sorted(n for n in zf.namelist() if n.startswith("ppt/slides/slide") and n.endswith(".xml"))
    for s in slides:
        print(f"  {s}")
