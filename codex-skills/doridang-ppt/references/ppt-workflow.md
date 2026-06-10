# Doridang PPT Workflow

## Source Handling
- Treat the user argument as either a file path or raw source text.
- If the source is missing, ask for the document content before generating slides.

## Output Path
- Save the final static deck to:
  - `C:\Users\민준\OneDrive - 주식회사 도리당\data\report\PowerPoint\`
- Use the file pattern:
  - `{title_slug}_2_애니메이션없는_ppt.pptx`
- Build the slug from the source topic:
  - Replace spaces with `_`
  - Remove special characters
  - Keep it concise

## Slide Design Rules
- Use `python-pptx`.
- Use 16:9 widescreen.
- Use a dark background `#0d0d0d`.
- Use mint accent `#00e5a0`.
- Use `Malgun Gothic` when available.
- Add speaker notes to each slide when a speaking script is available.

## Deck Structure
- Prefer one of these flows:
  - Report: cover, background, goal, core findings, impact, next steps
  - Proposal: cover, executive summary, problem, goal, analysis, recommendation
- Keep one core message per slide.
- Keep tables and KPI blocks explicit when the source contains metrics.

## Workspace Examples
- Review these existing workspace examples before patching behavior:
  - `.claude/skills/ppt/temp_pptx_gen.py`
  - `.claude/skills/ppt/update_notes.py`
  - `.claude/skills/ppt/inject_images.py`
- Reuse their patterns before introducing a new layout or note-writing approach.
