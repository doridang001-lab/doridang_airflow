"""Repack to local temp first, then copy to OneDrive."""
import pathlib, zipfile, shutil

SRC = pathlib.Path(r"C:\airflow\pptx_unpacked2")
TMP = pathlib.Path(r"C:\airflow\output_temp.pptx")
DST = pathlib.Path(r"C:\Users\민준\OneDrive - 주식회사 도리당\data\report\PowerPoint\전사 DXAX 교육_260528_개선.pptx")

# Pack to local temp
with zipfile.ZipFile(TMP, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for f in SRC.rglob("*"):
        if f.is_file():
            arc = f.relative_to(SRC).as_posix()
            zf.write(f, arc)

print(f"Packed locally: {TMP}  ({TMP.stat().st_size:,} bytes)")

# Verify ZIP
with zipfile.ZipFile(TMP) as zf:
    names = zf.namelist()
    slides = sorted(n for n in names if "slides/slide" in n and n.endswith(".xml") and "_rels" not in n)
    print("Slides:", slides)
    has_ct = "[Content_Types].xml" in names
    has_rels = "_rels/.rels" in names
    print(f"[Content_Types].xml: {has_ct}, _rels/.rels: {has_rels}")

# Copy to destination
shutil.copy2(TMP, DST)
print(f"Copied to: {DST}")
