#!/usr/bin/env python3
"""Assemble dist/docs/*.html from one house style plus per-page bodies.

The deliverable is the HTML: self-contained, inline CSS, printable, no script.
Fifteen pages share one byte-identical style block, so a colour fix or an
accessibility repair lands on all of them at once instead of fifteen times.
Editing the generated HTML by hand is wasted work — the next build overwrites
it. Edit ``bodies/<stem>.html``, or the ``register(...)`` call for the page's
shell text, then rebuild.
"""

import argparse
import base64
import re
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
OUT = REPO / "dist/docs"
BODIES = HERE / "bodies"
STATIC = HERE / "static"

# Output names are pinned to the filenames README.md, the Makefile, the wheel
# build and the live /docs links already point at. Renaming one breaks every
# inbound link for no gain.
OUTNAME = {
    "index": "index",
    "quickstart": "quickstart",
    "configuration": "configuration",
    "ingestion": "ingestion",
    "queries": "queries",
    "streaming": "streaming",
    "dashboard": "dashboard",
    "camera": "camera",
    "clients": "clients",
    "backup": "backup",
    "systemd": "systemd",
    "api": "api",
    "architecture": "architecture",
    "codebase": "codebase-guide",
    "examples": "examples",
}

# Every contrast ratio below was computed, not judged by eye. The three text
# tiers are chosen so that EACH clears 4.5:1 on BOTH backgrounds it is ever
# painted on (--card and --brand-soft), which is why --faint is darker than
# looks natural: it is used for real small text (page numbers in the
# contents grid, "identical to" notes on cards), so a decorative value would
# have been a WCAG 1.4.3 failure. Measured: body 9.64:1, muted 6.28:1, faint
# 5.24:1 on --card. The footer uses --body, NOT --muted, so a reader who
# lightens it back does not silently drop the footer under the AA floor.
CSS = """
  :root{
    --ink:#0a1e28; --body:#2c4859; --muted:#4a6377; --faint:#55707f;
    --brand:#0d4f60; --brand-deep:#07303c; --brand-soft:#eaf4f6;
    --accent:#0b6e8c; --accent-soft:#e6f6fb;
    --ok:#046c4e; --ok-soft:#e9f7f1;
    --warn:#a4262c; --warn-soft:#fdefef;
    --line:#d5e0e6; --surface:#f5f8fa; --card:#ffffff;
    --mono:ui-monospace,"SF Mono",Menlo,Consolas,"Liberation Mono",monospace;
  }
  *{box-sizing:border-box}
  html{-webkit-text-size-adjust:100%}
  body{
    margin:0; background:var(--surface); color:var(--body);
    font:16px/1.65 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;
  }
  .sheet{max-width:1120px;margin:0 auto;background:var(--card);
    box-shadow:0 1px 3px rgba(10,30,40,.06),0 12px 40px rgba(10,30,40,.05)}

  header.masthead{
    background:linear-gradient(158deg,#062833 0%,#0d4f60 52%,#16697f 100%);
    color:#fff;padding:50px 60px 42px;position:relative;overflow:hidden}
  header.masthead::after{
    content:"";position:absolute;right:-140px;top:-150px;width:460px;height:460px;
    border-radius:50%;background:radial-gradient(circle,rgba(255,255,255,.10),transparent 68%)}
  .eyebrow{font:600 11px/1 var(--mono);letter-spacing:.18em;text-transform:uppercase;
    color:#a5cfdb;margin-bottom:16px}
  /* The eyebrow is also the way back to the index, so it has to look tappable
     without turning the page title into a link. The underline stays on hover
     and focus as well as the colour change: colour alone is not a sufficient
     cue (WCAG 1.4.1). */
  .backlink{color:inherit;text-decoration:none;border-bottom:1px solid transparent}
  .backlink:hover,.backlink:focus-visible{color:#fff;border-bottom-color:#a5cfdb}
  h1{font-size:40px;line-height:1.1;margin:0 0 6px;font-weight:650;letter-spacing:-.02em}
  .tagline{font-size:18px;color:#c9e2e9;margin:0 0 24px;max-width:66ch;font-weight:400}
  .meta{display:flex;flex-wrap:wrap;gap:10px 26px;font:12px/1.5 var(--mono);color:#a5cfdb;
    border-top:1px solid rgba(255,255,255,.16);padding-top:18px}
  .meta b{color:#e2f0f4;font-weight:500}

  main{padding:8px 60px 20px}
  section{padding:36px 0;border-bottom:1px solid var(--line)}
  section:last-child{border-bottom:0}
  h2{font:650 12px/1 var(--mono);letter-spacing:.16em;text-transform:uppercase;
    color:var(--brand);margin:0 0 10px;display:flex;align-items:center;gap:12px}
  h2::after{content:"";flex:1;height:1px;background:var(--line)}
  h3{font-size:19px;color:var(--ink);margin:30px 0 10px;font-weight:640;letter-spacing:-.01em}
  h4{font-size:14px;color:var(--ink);margin:22px 0 8px;font-weight:640;
    text-transform:uppercase;letter-spacing:.06em}
  p{margin:0 0 14px;max-width:80ch}
  .lede{font-size:17px;color:var(--ink);max-width:78ch}
  ul,ol{margin:0 0 16px;padding-left:22px;max-width:80ch}
  li{margin:0 0 7px}
  strong{color:var(--ink);font-weight:640}
  /* Inline code keeps the mono look but must be allowed to break. A short flag
     still never breaks, because overflow-wrap only engages when a word cannot
     fit a line by itself; a long path in a sentence does, and with nowrap it
     used to scroll the whole page sideways on a phone. */
  code{font:.855em/1.5 var(--mono);background:var(--brand-soft);color:var(--brand);
    padding:1px 1px 1px 4px;border-radius:3px;overflow-wrap:break-word}
  a{color:var(--brand)}
  a:hover{color:var(--accent)}
  .muted{color:var(--muted)}
  .small{font-size:14px}
  .nb{white-space:nowrap}

  /* Command blocks. A rule of three keeps a long argv readable and stops the
     right edge turning to ragged noise: three spaces between groups, a
     backslash for a real continuation, a comment on its own line. A pasted
     block is always valid to run as-is. */
  pre{margin:0 0 18px;background:#0a2129;color:#d3e6ec;border-radius:7px;
    padding:15px 18px;overflow-x:auto;font:13.5px/1.62 var(--mono);
    border:1px solid #061820;box-shadow:inset 0 1px 0 rgba(255,255,255,.05)}
  pre code{background:none;color:inherit;padding:0;font-size:inherit;white-space:pre}
  pre .c{color:#7fa3b0}          /* comment  */
  pre .p{color:#6fe3c4}          /* prompt   */
  pre .f{color:#f0c987}          /* filename */
  pre .v{color:#8fd0ef}          /* value    */
  pre .o{color:#ffa98f}          /* operator/flag */
  pre .ok{color:#7ee0a8}          /* good outcome */

  .stats{display:grid;grid-template-columns:repeat(5,1fr);gap:1px;
    background:var(--line);border:1px solid var(--line);border-radius:6px;
    overflow:hidden;margin:24px 0 0}
  .stat{background:var(--card);padding:15px 17px}
  .stat .n{font:650 24px/1.1 var(--mono);color:var(--brand);letter-spacing:-.02em}
  .stat .l{font-size:11px;color:var(--muted);text-transform:uppercase;
    letter-spacing:.07em;margin-top:5px}
  /* Four-up is the honest grid for a four-column fact strip; the five-up
     default would leave a hole, and a hole reads as a missing fact. */
  .stats.four{grid-template-columns:repeat(4,1fr)}
  .stats.three{grid-template-columns:repeat(3,1fr)}

  figure{margin:26px 0;padding:0}
  figcaption{font-size:12.5px;color:var(--muted);margin-top:12px;
    padding-left:2px;max-width:90ch}
  .diagram{border:1px solid var(--line);border-radius:8px;background:#fff;overflow:hidden}
  .diagram svg{display:block;width:100%;height:auto}

  /* Real screenshots of the running pages, so a reader can see the thing
     before running it. Each keeps its own aspect ratio inside a frame of one
     fixed height: the whole point is that the wide admin dashboard and the
     narrow phone layout must be comparable side by side, which normalising
     on width would hide. The frame is dark so a page's own dark chrome reads
     as chrome, not as page. */
  .gallery{display:grid;grid-template-columns:repeat(auto-fit,minmax(158px,1fr));
    gap:14px;margin:22px 0}
  .shot{border:1px solid var(--line);border-radius:7px;background:var(--card);
    padding:10px;display:flex;flex-direction:column;gap:9px}
  .shot .frame{background:#0a1e28;border-radius:4px;
    display:flex;align-items:center;justify-content:center;overflow:hidden}
  .gallery.wide .shot .frame{height:150px}
  .gallery.phone .shot .frame{height:252px}
  .shot .frame img{display:block;max-width:100%;max-height:100%;width:auto;height:auto}
  .shot .cap{font:600 11.5px/1.35 var(--mono);color:var(--brand);letter-spacing:.03em}
  .shot .sub{font-size:11.5px;line-height:1.45;color:var(--muted);margin-top:-4px}

  /* A table cannot shrink below the width its content demands, so it gets a
     box of its own that scrolls. On a wide screen the box is invisible and
     the table simply fits; on a narrow one the table scrolls sideways inside
     it while the page itself stays put. This is the guard that keeps a wide
     row from reaching the edge of the page. */
  .tablewrap{overflow-x:auto;max-width:100%}
  .tablewrap table{margin-left:0;margin-right:0}
  table{width:100%;border-collapse:collapse;margin:16px 0 8px;font-size:14px}
  th,td{text-align:left;padding:8px 11px;border-bottom:1px solid var(--line);
    vertical-align:top}
  th{font:600 11px/1.3 var(--mono);letter-spacing:.09em;text-transform:uppercase;
    color:var(--brand);background:var(--brand-soft);
    border-bottom:1px solid var(--line);white-space:nowrap}
  tbody tr:last-child td{border-bottom:0}
  tbody tr:hover{background:#fbfdfe}
  /* Table layout is automatic, so every cell's unbreakable minimum adds to
     the table's demanded width. A cell that cannot wrap therefore pushes the
     whole table past the page edge rather than growing taller — which is how
     a long option once threw 143px off the right of a page. These two
     classes keep the mono look but let the content flow, and break-word
     stops a single unbreakable token (a long URL) from ever setting a
     minimum wider than its column. td code needs the same treatment. */
  td code{font-size:12.5px;white-space:normal;overflow-wrap:break-word}
  td.nowrap,th.nowrap{white-space:nowrap}
  .opt{font:600 12.5px/1.45 var(--mono);color:var(--accent);
    white-space:normal;overflow-wrap:break-word}
  .def{font:12.5px/1.45 var(--mono);color:var(--muted);
    white-space:normal;overflow-wrap:break-word}
  /* An option or default that is genuinely one short token reads better held
     on one line. Applied per cell, never to the column. */
  .opt.nowrap,.def.nowrap{white-space:nowrap}
  /* Option tables carry a label, a default and prose, and the label column
     is the one that must never wrap mid-flag. Auto layout cannot know that,
     hence the explicit widths. */
  table.opts th:nth-child(1),table.opts td:nth-child(1){width:23%}
  table.opts th:nth-child(2),table.opts td:nth-child(2){width:20%}
  table.ports th:nth-child(1),table.ports td:nth-child(1){width:11%}
  table.ports th:nth-child(2),table.ports td:nth-child(2){width:13%}
  table.routes th:nth-child(1),table.routes td:nth-child(1){width:9%}

  .toc{border:1px solid var(--line);border-radius:7px;background:var(--card);
    padding:16px 20px;margin:24px 0 0}
  .toc h5{font:650 11px/1.3 var(--mono);letter-spacing:.12em;text-transform:uppercase;
    color:var(--brand);margin:0 0 12px}
  .tocgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:7px 26px;margin:0}
  .tocgrid a{display:block;font-size:14px;color:var(--body);text-decoration:none;
    padding:3px 0;border-bottom:1px solid transparent}
  .tocgrid a:hover{color:var(--accent);border-bottom-color:var(--line)}
  .tocgrid a .n{font:600 11px/1 var(--mono);color:var(--faint);margin-right:8px}

  .box{border:1px solid var(--line);border-left:3px solid var(--brand);
    background:var(--card);border-radius:0 6px 6px 0;padding:15px 18px;margin:20px 0}
  .box.warn{border-left-color:var(--warn);background:var(--warn-soft)}
  .box.ok{border-left-color:var(--ok);background:var(--ok-soft)}
  .box.key{border-left-color:var(--accent);background:var(--accent-soft)}
  .box p:last-child{margin-bottom:0}
  .box .lbl{font:650 11px/1 var(--mono);letter-spacing:.12em;text-transform:uppercase;
    color:var(--brand);display:block;margin-bottom:8px}
  .box.warn .lbl{color:var(--warn)} .box.ok .lbl{color:var(--ok)}
  .box.key .lbl{color:var(--accent)}
  .box pre{background:#0d2b35}

  .cards{display:grid;gap:14px;margin:20px 0}
  .card{border:1px solid var(--line);border-radius:7px;padding:16px 18px;background:var(--card)}
  .card h4{margin:0 0 8px;font-size:15px;text-transform:none;letter-spacing:0;
    color:var(--brand)}
  .card p{font-size:14px;margin:0 0 8px;max-width:none}
  .card p:last-child{margin:0}
  .tradeoff{font-size:13px;color:var(--muted);border-top:1px dashed var(--line);
    padding-top:9px;margin-top:9px}
  .tradeoff b{color:var(--accent);font-weight:640}

  .steps{counter-reset:s;list-style:none;padding:0;margin:20px 0}
  .steps li{counter-increment:s;position:relative;padding:0 0 16px 42px;margin:0;max-width:82ch}
  .steps li::before{content:counter(s);position:absolute;left:0;top:1px;width:26px;
    height:26px;border-radius:50%;background:var(--brand);color:#fff;
    font:600 12px/26px var(--mono);text-align:center}
  .steps li::after{content:"";position:absolute;left:13px;top:31px;bottom:2px;
    width:1px;background:var(--line)}
  .steps li:last-child::after{display:none}
  .steps b{display:block;color:var(--ink);font-weight:640;margin-bottom:2px}
  .steps span{font-size:14.5px;display:block}
  .steps pre{margin:10px 0 0}

  footer{background:var(--brand-soft);padding:32px 60px 26px;font-size:13px;
    color:var(--body);border-top:1px solid var(--line)}
  footer p{max-width:none;margin:0}
  .fgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:22px 40px;margin:0 0 26px}
  .fitem h5{font:650 11px/1.3 var(--mono);letter-spacing:.1em;text-transform:uppercase;
    color:var(--brand);margin:0 0 7px}
  .fitem p{font-size:14px;line-height:1.6;color:var(--body);max-width:40ch}
  .fclose{font-size:17px;color:var(--ink);margin:0 0 14px;letter-spacing:-.01em}
  .fpart{font-size:15px;line-height:1.62;color:var(--ink);max-width:90ch;margin:0;
    border-top:1px solid var(--line);padding-top:16px}

  @media (max-width:860px){
    header.masthead{padding:34px 24px 30px} main{padding:4px 24px 16px}
    section{padding:28px 0} footer{padding:22px 24px} h1{font-size:30px}
    .tagline{font-size:16px}
    .stats,.stats.four,.stats.three{grid-template-columns:repeat(2,1fr)}
    .tocgrid{grid-template-columns:1fr 1fr}
    .fgrid{grid-template-columns:1fr;gap:20px}
    /* Three columns cannot hold desktop spacing in a phone's width, and a
       header held on one line becomes the widest thing in its column. Tighten
       the table and let headers wrap, so a phone scrolls vertically only. */
    table{font-size:13px}
    th,td{padding:6px 7px}
    th{white-space:normal}
    table.opts th:nth-child(1),table.opts td:nth-child(1),
    table.opts th:nth-child(2),table.opts td:nth-child(2){width:auto}
    table.ports th,table.ports td{width:auto}
    table.routes th,table.routes td{width:auto}
  }
  @media print{
    body{background:#fff} .sheet{box-shadow:none;max-width:none}
    /* Sections are long, so forcing each one whole onto a page left two thirds
       of page 1 blank. Break freely, but never orphan a heading and never
       split a figure, table or callout. */
    section{padding:20px 0;break-inside:auto}
    h2,h3,h4{break-after:avoid;page-break-after:avoid}
    p,li{orphans:3;widows:3}
    /* .fgrid as well as .fitem: protecting only the items lets the grid break
       between its two rows, which split the pull lines across a page. */
    figure,table,.box,.card,.stat,.toc,.fgrid,.fitem,.shot{
      break-inside:avoid;page-break-inside:avoid}
    pre{break-inside:avoid;page-break-inside:avoid}
    /* The footer is the document's closing statement, so it moves to the next
       page whole rather than leaving the sign-off and parting behind. */
    footer{break-inside:avoid;page-break-inside:avoid}
    *{-webkit-print-color-adjust:exact;print-color-adjust:exact}
  }
"""

TPL = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<meta name="description" content="{description}">
<link rel="icon" type="image/svg+xml" href="favicon.svg">
<style>{css}</style>
</head>
<body>
<div class="sheet">

<header class="masthead">
  <div class="eyebrow">{eyebrow}</div>
  <h1>{h1}</h1>
  <p class="tagline">{tagline}</p>
  <div class="meta">
{meta}
  </div>
</header>

<main>
{body}
</main>

<footer>
  <div class="fgrid">
{footer}
  </div>
  <p class="fclose">{fclose}</p>
  <p class="fpart">{fpart}</p>
</footer>

</div>
</body>
</html>
"""

FRAGMENTS: dict[str, str] = {}


def fragment(name: str, text: str) -> None:
    """Register a reusable block that bodies pull in with {{name}}."""
    FRAGMENTS[name] = text


def expand(text: str) -> str:
    """Replace every {{name}} with its fragment, then its own {{...}}s."""
    for _ in range(4):
        if "{{" not in text:
            break
        out: list[str] = []
        index = 0
        while True:
            start = text.find("{{", index)
            if start < 0:
                out.append(text[index:])
                break
            end = text.find("}}", start)
            if end < 0:
                out.append(text[index:])
                break
            out.append(text[index:start])
            key = text[start + 2 : end].strip()
            if key not in FRAGMENTS:
                raise KeyError(f"unknown fragment: {key}")
            out.append(FRAGMENTS[key])
            index = end + 2
        text = "".join(out)
    return text


TABLE_RE = re.compile(r"(<table\b.*?</table>)", re.DOTALL)


def wrap_tables(text: str) -> str:
    """Put every table inside a horizontal scroll box.

    A table is the one element here that cannot shrink below the width its
    content demands, and on a narrow screen that demand pushed the whole page
    sideways. Contained in a box of its own, the table scrolls and the page
    does not. No table nests inside another, so a non-greedy match is enough
    and the SVG diagrams are untouched.
    """
    return TABLE_RE.sub(r'<div class="tablewrap">\1</div>', text)


IMG_SRC = re.compile(r'(<img\b[^>]*?\bsrc=")([^"]+)(")')

MIME = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".svg": "image/svg+xml",
    ".webp": "image/webp",
}


def inline_images(page: str, base: Path) -> tuple[str, int]:
    """Return (page, count) with local image sources replaced by data: URIs.

    Only <img src> is rewritten. Hyperlinks between pages stay links: they are
    navigation, not assets, and a page that cannot reach its siblings is still
    a readable page. Base64 costs a third more than the raw bytes, so this is
    opt-in per page rather than the default for the whole set.
    """
    inlined = 0

    def swap(match: re.Match[str]) -> str:
        nonlocal inlined
        target = base / match.group(2)
        if not target.exists():
            return match.group(0)
        mime = MIME.get(target.suffix.lower(), "application/octet-stream")
        data = base64.b64encode(target.read_bytes()).decode("ascii")
        inlined += 1
        return f"{match.group(1)}data:{mime};base64,{data}{match.group(3)}"

    return IMG_SRC.sub(swap, page), inlined


META_TITLE: dict[str, str] = {}
META_DESC: dict[str, str] = {}
EYEBROW: dict[str, str] = {}
H1: dict[str, str] = {}
TAGLINE: dict[str, str] = {}
META: dict[str, list[tuple[str, str]]] = {}
FOOT: dict[str, list[tuple[str, str]]] = {}
FCLOSE: dict[str, str] = {}
FPART: dict[str, str] = {}


def register(
    stem: str,
    title: str,
    description: str,
    eyebrow: str,
    h1: str,
    tagline: str,
    meta: list[tuple[str, str]],
    foot: list[tuple[str, str]],
    fclose: str | None = None,
    fpart: str | None = None,
) -> None:
    """Record one page's shell text so render() can pick it up."""
    META_TITLE[stem] = title
    META_DESC[stem] = description
    EYEBROW[stem] = eyebrow
    H1[stem] = h1
    TAGLINE[stem] = tagline
    META[stem] = meta
    FOOT[stem] = foot
    if fclose:
        FCLOSE[stem] = fclose
    if fpart:
        FPART[stem] = fpart


def render(
    stem: str, dest: Path | None = None, standalone_mode: bool = False
) -> tuple[int, int]:
    """Build one page from its body file plus the shared shell."""
    meta_lines = [
        f"    <span><b>{label}</b> {value}</span>" for label, value in META[stem]
    ]
    footer_items = [
        '    <div class="fitem">\n'
        f"      <h5>{head}</h5>\n"
        f"      <p>{text}</p>\n"
        "    </div>"
        for head, text in FOOT[stem]
    ]
    # Every page except the index itself gets its eyebrow as the way back, so
    # there is always one click from any page to the full list.
    eyebrow_html = EYEBROW[stem]
    if stem != "index":
        eyebrow_html = f'<a class="backlink" href="index.html">{eyebrow_html}</a>'

    page = (
        TPL.format(
            title=META_TITLE[stem],
            description=META_DESC[stem],
            css=CSS,
            eyebrow=eyebrow_html,
            h1=H1[stem],
            tagline=TAGLINE[stem],
            meta=expand("\n".join(meta_lines)),
            body=wrap_tables(expand((BODIES / f"{stem}.html").read_text())).rstrip(
                "\n"
            ),
            footer="\n".join(footer_items),
            fclose=FCLOSE[stem],
            fpart=FPART[stem],
        )
        + "\n"
    )
    inlined = 0
    if standalone_mode:
        page, inlined = inline_images(page, OUT)
    target_dir = dest if dest is not None else OUT
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / f"{OUTNAME[stem]}.html").write_text(page)
    return len(page), inlined


# ------------------------------------------------------------- page shell ----
# Per-page shell text lives in sqtseries_pages.py, not here. This file owns what
# must not drift (stylesheet, template, fragment expander); the other owns the
# prose. Merging them back would make a style change a fifteen-page diff.

CLOSES = {
    "index": "<strong>sqtseries — fifteen pages, every option, every argument.</strong>",
    "quickstart": "<strong>sqtseries — install, run, write, query, subscribe, done.</strong>",
    "configuration": "<strong>sqtseries — every setting, every file, every variable.</strong>",
    "ingestion": "<strong>sqtseries — a number in, from any language.</strong>",
    "queries": "<strong>sqtseries — the past, in milliseconds.</strong>",
    "streaming": "<strong>sqtseries — the present, the moment it lands.</strong>",
    "dashboard": "<strong>sqtseries — the service, on screen, second by second.</strong>",
    "camera": "<strong>sqtseries — a camera feed, stored and watched live.</strong>",
    "clients": "<strong>sqtseries — one wire format, six languages.</strong>",
    "backup": "<strong>sqtseries — one file, copied, and put back.</strong>",
    "systemd": "<strong>sqtseries — installed once, then forgotten.</strong>",
    "api": "<strong>sqtseries — every route, every command, every error code.</strong>",
    "architecture": "<strong>sqtseries — how the pieces fit, and why.</strong>",
    "codebase": "<strong>sqtseries — every module, and what it is for.</strong>",
    "examples": "<strong>sqtseries — the scripts, run and shown.</strong>",
}

PART = (
    "Every default on these pages is the default in the code, checked against "
    "<code>sqtseries --help</code>, the live settings loader and the running "
    "service by <code>scripts/docs-build/verify_docs.py</code>. The wire format "
    "is the same on every transport: ZMQ <code>PULL</code>, <code>ROUTER</code>, "
    "<code>REQ</code>, <code>XPUB</code>, HTTP and WebSocket."
)

for _key in CLOSES:
    FCLOSE.setdefault(_key, CLOSES[_key])
    FPART.setdefault(_key, PART)


def load_pages() -> list[str]:
    sys.path.insert(0, str(HERE))
    import sqtseries_frags
    import sqtseries_pages

    for key, text in sqtseries_frags.FRAGS.items():
        fragment(key, text.strip("\n"))
    return sorted(sqtseries_pages.registered())


def main() -> int:
    """Build the pages. --standalone emits single-file portable copies."""
    parser = argparse.ArgumentParser(
        description="Build the dist/docs pages from scripts/docs-build/bodies."
    )
    parser.add_argument(
        "stems",
        nargs="*",
        metavar="STEM",
        help="build only these pages (default: every registered page)",
    )
    parser.add_argument(
        "--standalone",
        action="store_true",
        help=(
            "also write self-contained copies into dist/docs/standalone/, with "
            "images inlined as data URIs, for moving a single page somewhere "
            "else. The normal build is the same thing without the images: every "
            "page already inlines the stylesheet, so there is no shared file to "
            "carry along."
        ),
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="print the known page stems and exit",
    )
    args = parser.parse_args()

    stems = load_pages()
    if args.list:
        for stem in stems:
            print(f"{stem:16s} -> {OUTNAME[stem]}.html")
        return 0

    if args.stems:
        wanted = {s.lower() for s in args.stems}
        unknown = wanted - {s.lower() for s in stems}
        if unknown:
            parser.error(
                f"unknown page(s): {', '.join(sorted(unknown))}. Known: {', '.join(stems)}"
            )
        stems = [s for s in stems if s.lower() in wanted]

    total = 0
    done = 0
    for stem in stems:
        body = BODIES / f"{stem}.html"
        if not body.exists():
            print(f"{OUTNAME[stem]:>16}.html        (body not written yet)")
            continue
        size, _ = render(stem)
        total += size
        done += 1
        print(f"{OUTNAME[stem]:>16}.html  {size:>7,} bytes")
    print(f"{'TOTAL':>17}  {total:>7,} bytes over {done} pages")

    # Published verbatim, not rendered. They live in static/ so a clean build
    # still emits them: hand-placed in dist/docs, `rm -rf dist` dropped both and
    # every page's <link rel="icon"> 404'd.
    OUT.mkdir(parents=True, exist_ok=True)
    for asset in sorted(STATIC.iterdir()):
        if asset.is_file():
            shutil.copy2(asset, OUT / asset.name)
            print(f"{asset.name:>16}      {asset.stat().st_size:>7,} bytes  (static)")

    if args.standalone:
        out_dir = OUT / "standalone"
        print(f"\nstandalone copies -> {out_dir}")
        stotal = 0
        for stem in stems:
            size, inlined = render(stem, dest=out_dir, standalone_mode=True)
            stotal += size
            note = f"  ({inlined} image(s) inlined)" if inlined else "  (no images)"
            print(f"{OUTNAME[stem]:>16}.html  {size:>7,} bytes{note}")
        print(f"{'TOTAL':>17}  {stotal:>7,} bytes")

    return 0


if __name__ == "__main__":
    # Delegate to the module under its real name. As a script this file is
    # __main__, but sqtseries_pages imports `register` from sqtseries_build, so
    # calling main() here would read a __main__ copy of the registries that the
    # page registrations never touched.
    sys.path.insert(0, str(HERE))
    import sqtseries_build

    sys.exit(sqtseries_build.main())
