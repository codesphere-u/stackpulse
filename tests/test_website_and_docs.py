"""Validation tests for StackPulse website, SEO/GEO files, docs, and version alignment."""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WEBSITE = ROOT / "website"


def test_required_website_and_docs_files_exist() -> None:
    expected = [
        WEBSITE / "index.html",
        WEBSITE / "studio.html",
        WEBSITE / "proof.html",
        WEBSITE / "rules.html",
        WEBSITE / "docs.html",
        WEBSITE / "styles.css",
        WEBSITE / "app.js",
        WEBSITE / "favicon.svg",
        WEBSITE / "og-image.svg",
        WEBSITE / "robots.txt",
        WEBSITE / "sitemap.xml",
        WEBSITE / "manifest.webmanifest",
        WEBSITE / "llms.txt",
        WEBSITE / "llms-full.txt",
        WEBSITE / "marketing" / "LAUNCH_PLAYBOOK.md",
        WEBSITE / "marketing" / "PROGRAMMATIC_SEO.md",
        ROOT / "llms.txt",
        ROOT / "llms-full.txt",
        ROOT / "docs" / "GETTING_STARTED.md",
        ROOT / "docs" / "OTLP_SLO_AND_AEGIS.md",
        ROOT / ".github" / "workflows" / "pages.yml",
    ]
    for path in expected:
        assert path.exists(), f"Missing required asset: {path}"


def test_website_palette_has_zero_pink_and_valid_meta() -> None:
    pink_tokens = ("#ec4899", "#f472b6", "#db2777", "#be185d", "hotpink", "deeppink")
    for html_path in sorted(WEBSITE.glob("*.html")):
        text = html_path.read_text(encoding="utf-8")
        low = text.lower()
        for tok in pink_tokens:
            assert tok not in low, f"Disallowed pink token {tok} in {html_path.name}"
        m = re.search(r'<meta name="description" content="([^"]+)"', text)
        assert m is not None, f"Missing meta description in {html_path.name}"
        desc = m.group(1)
        assert 50 <= len(desc) <= 160, f"Meta description length {len(desc)} out of bounds in {html_path.name}"

    css = (WEBSITE / "styles.css").read_text(encoding="utf-8").lower()
    for tok in pink_tokens:
        assert tok not in css, f"Disallowed pink token {tok} in styles.css"


def test_index_json_ld_and_local_links_valid() -> None:
    index_text = (WEBSITE / "index.html").read_text(encoding="utf-8")
    m = re.search(r'<script type="application/ld\+json">\s*(\{.*?\})\s*</script>', index_text, re.DOTALL)
    assert m is not None
    data = json.loads(m.group(1))
    assert "@graph" in data

    for html_path in sorted(WEBSITE.glob("*.html")):
        text = html_path.read_text(encoding="utf-8")
        for href in re.findall(r'href="([^"]+)"', text):
            if href.startswith(("http://", "https://", "#", "mailto:")):
                continue
            target = href.split("#")[0]
            if target:
                assert (WEBSITE / target).exists(), f"Broken local href {href} in {html_path.name}"


def test_version_1_2_0_aligned_across_website_and_docs() -> None:
    assert "1.2.0" in (WEBSITE / "index.html").read_text(encoding="utf-8")
    assert "1.2.0" in (ROOT / "CITATION.cff").read_text(encoding="utf-8")
    assert "1.2.0" in (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
