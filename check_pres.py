import pathlib, re

txt = pathlib.Path(r"C:\airflow\pptx_unpacked2\ppt\presentation.xml").read_text(encoding="utf-8")
m = re.search(r"<p:sldIdLst>(.*?)</p:sldIdLst>", txt)
if m:
    entries = re.findall(r'<p:sldId[^/]*/>', m.group(1))
    for e in entries:
        print(e)
else:
    print("NOT FOUND")
