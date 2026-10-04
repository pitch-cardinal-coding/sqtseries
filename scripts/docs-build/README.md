# docs-build — the documentation generator

Every page in `dist/docs/` is **generated**. Nothing there should be edited by hand:
the next build overwrites it. This folder holds the generator, the page sources it
reads, and the checks that prove the output is sound.

## Why it exists

The fifteen pages share one stylesheet, one masthead, one footer and one set of
accessibility decisions. Keeping those in a single place is the only way the pages
cannot drift apart — a colour fix or a contrast repair lands on all of them at once
instead of fifteen times.

The stylesheet is **inlined into every page** rather than linked. That is what makes
each page self-contained: copy one file to a USB stick and it still renders, with no
network and no sibling files. The cost is size, and the build accepts it deliberately.

## Layout

```
scripts/docs-build/
  sqtseries_build.py     the generator: CSS + template + shell -> dist/docs/*.html
  sqtseries_pages.py     per-page shell text: title, eyebrow, tagline, footer items
  sqtseries_frags.py     shared tables (CLI, settings, routes, errors, wire format)
  sqtseries_check.py     structural check: balanced tags, per-page coverage stats
  verify_docs.py         cross-check: every option, setting, route, command in the code
  bodies/*.html          one file per page: the prose, tables, commands, diagrams
  static/                published verbatim, not rendered: favicon.svg, benchmarks.md
```

Six files, six jobs, and the split matters:

| File | Owns | Why it is separate |
|------|------|--------------------|
| `sqtseries_build.py` | the stylesheet, the template, the fragment expander, the table wrapper | a style change is a one-file diff |
| `sqtseries_pages.py` | the shell text of each page | prose changes without touching the generator |
| `sqtseries_frags.py` | tables shared by several pages | one flag cannot be documented two ways |
| `sqtseries_check.py` | the structural check | a broken page fails the build, not a reader |
| `verify_docs.py` | the code-to-docs cross-check | drift becomes a test failure |
| `bodies/*.html` | everything else | the only file you normally edit |

## Usage

Run from the repository root:

```bash
PY=/home/iam/devcode/.env/sqtseries/bin/python3

# build every page
$PY scripts/docs-build/sqtseries_build.py

# build one page while iterating on it
$PY scripts/docs-build/sqtseries_build.py queries

# then always check the result
$PY scripts/docs-build/sqtseries_check.py
$PY scripts/docs-build/verify_docs.py
```

`sqtseries_check.py` fails on an unbalanced tag. That is not pedantry: the failure
that actually happens when a body is hand-written is one unclosed `<figure>` or
`<pre>` silently swallowing the rest of the page, and it renders as a page that looks
almost right. It also prints the per-page coverage counts — sections, tables,
documented options, diagrams, code blocks — which are the numbers these pages exist to
deliver.

`verify_docs.py` is the important one. It reads the **code**, not a hand-maintained
list, and proves both directions:

- every CLI flag, settings key, environment variable, HTTP route, aggregation, admin
  command and example script **is** documented on some page
- every `--flag` a page names is accepted by a real program, a real example script, or
  is on a small allow-list of flags belonging to other tools (`systemctl --user`,
  `journalctl --since`, `cargo --features`, and so on — each entry names its owner)

So a page cannot invent a flag, and cannot quietly fall behind the code.

## Editing a page

1. Edit `bodies/<stem>.html`. It is ordinary HTML with two extensions:
   - `{{fragment_name}}` expands to a shared block from `sqtseries_frags.py`
   - every `<table>` is wrapped in a scroll box automatically, so a wide table can
     never push the page sideways on a phone. **Do not add your own
     `<div class="tablewrap">`** — the build adds one and you would get two.
2. To change a page's title, tagline, meta strip or footer, edit its `page(...)` call
   in `sqtseries_pages.py`.
3. Rebuild and check (commands above).
4. Look at the page. The checks do not catch a layout that merely looks wrong.

To add a page: add a `bodies/<stem>.html`, add the stem to `OUTNAME`, and add a
`page(...)` call. The build loop picks up every registered stem that has a body file.

## Conventions the generator enforces

- **Colour contrast is computed, not eyeballed.** Every ratio in the palette comment
  was measured. The three text tiers are each chosen to clear 4.5:1 on *both*
  backgrounds they are ever painted on, which is why `--faint` is darker than looks
  natural — it is used for real small text, not decoration.
- **The footer uses `--body`, not `--muted`.** The two are deliberately coupled;
  lightening the footer back to `--muted` reintroduces a WCAG AA failure.
- **One font stack, one mono stack**, defined once as custom properties.
- **Print styles are part of the page**, not an afterthought — long tables, figures
  and callouts avoid being split across a page break.

## The one thing that ties them to the service

These pages are served by the gateway at `/docs`. The gateway's Content-Security-Policy
is `default-src 'self'`, which forbids an inline `<style>` — so inlining the
stylesheet required one narrow exception, scoped to that path and to `style-src` alone:

```python
# src/sqtseries/gateway/app.py, HeaderMiddleware
path = scope.get("path", "")
docs = path == "/docs" or path.startswith("/docs/")
headers["Content-Security-Policy"] = (
    "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'"
    if docs
    else "default-src 'self'; img-src 'self' data:"
)
```

`script-src` is not relaxed anywhere, and the JSON API keeps the strict default. This
mirrors what `/api-docs` and `/redoc` already do for their own vendored pages. If you
remove it, every page here renders unstyled — silently, because the browser drops the
block rather than reporting an error.

## What is not here

**Images.** These pages have no `<img>` tags, so there is no image pipeline. The one
optional feature, `--standalone`, inlines any local images as `data:` URIs for copying
a single page elsewhere:

```bash
$PY scripts/docs-build/sqtseries_build.py --standalone queries
```

Base64 costs about a third more than the raw bytes, which is why it is opt-in per page
rather than the default for the whole set. The output goes to
`dist/docs/standalone/`, which is generated and gitignored.

## When a check fails

| Symptom | Cause | Fix |
|---------|-------|-----|
| `KeyError` on a page name | the stem has no `page(...)` call | add one, or check the spelling against `--list` |
| `unknown fragment` | `{{name}}` with no matching `FRAGS` key | check `sqtseries_frags.py` |
| `BAD <page>` with a tag error | an unclosed tag in a body | the offset is into the `<main>` block; look there |
| `NOT DOCUMENTED` in `verify_docs` | the code has something the pages do not mention | document it, or the check is right and the page is incomplete |
| `NOT REAL` | a page names a flag nothing accepts | usually a typo; occasionally a real flag that moved |
