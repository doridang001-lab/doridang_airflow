---
name: ppt
description: Convert a Doridang planning note, report, analysis memo, or markdown draft into a static PowerPoint deck for sharing. Use when Codex needs to create or revise the team's non-animated PPTX output, generate slide outlines from source text, or follow the existing OneDrive PowerPoint delivery workflow. Trigger on requests such as $ppt, ppt, or /ppt-style deck generation.
---

# PPT

Read [references/ppt-workflow.md](references/ppt-workflow.md) before generating slides.

## Workflow
- Read the source document from the provided path, or treat the user message as the source content.
- Summarize the core message, audience, and recommended slide flow before generating artifacts.
- Build a concise slide-by-slide outline with one message per slide.
- Generate or revise the static PPTX using the established Doridang layout and output path rules.
- If notes or images must be adjusted in an existing deck, reuse the workspace examples listed in the reference file instead of inventing a new pattern.

## Output Rules
- Preserve factual numbers, dates, and comparisons from the source.
- Favor tables, KPI blocks, and clear section titles over dense paragraphs.
- Keep the deliverable static and shareable; do not design for animation-first playback.
- Report the final file path, slide count, and estimated talk time.
