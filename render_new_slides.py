"""Render slide 13 and 14 to PNG for visual QA."""
import win32com.client
import pathlib, time

PPTX = r"C:\Users\민준\OneDrive - 주식회사 도리당\data\report\PowerPoint\전사 DXAX 교육_260528_개선.pptx"
OUT_DIR = pathlib.Path(r"C:\airflow\qa_slides")
OUT_DIR.mkdir(exist_ok=True)

ppt = win32com.client.Dispatch("PowerPoint.Application")
ppt.Visible = True

prs = ppt.Presentations.Open(PPTX, ReadOnly=True, WithWindow=False)
time.sleep(3)

total = prs.Slides.Count
print(f"Total slides: {total}")

for idx in [13, 14]:
    slide = prs.Slides(idx)
    out_path = str(OUT_DIR / f"slide{idx:02d}.png")
    slide.Export(out_path, "PNG", 1920, 1080)
    print(f"Saved: {out_path}")

prs.Close()
ppt.Quit()
print("Done.")
