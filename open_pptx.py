"""Try opening the local temp PPTX via COM to diagnose issues."""
import win32com.client
import pathlib, time

PPTX = r"C:\airflow\output_temp.pptx"
OUT_DIR = pathlib.Path(r"C:\airflow\qa_slides")
OUT_DIR.mkdir(exist_ok=True)

ppt = win32com.client.Dispatch("PowerPoint.Application")
ppt.Visible = True
time.sleep(1)

try:
    prs = ppt.Presentations.Open(PPTX, ReadOnly=False, WithWindow=True)
    time.sleep(4)
    total = prs.Slides.Count
    print(f"Total slides: {total}")

    for idx in range(1, total + 1):
        slide = prs.Slides(idx)
        out_path = str(OUT_DIR / f"slide{idx:02d}.png")
        slide.Export(out_path, "PNG", 1920, 1080)
        print(f"Saved slide {idx}: {out_path}")

    prs.Close()
    ppt.Quit()
    print("Done.")
except Exception as e:
    print(f"ERROR: {e}")
    try:
        ppt.Quit()
    except Exception:
        pass
