"""Register widget HTML files as MCP App resources."""

from __future__ import annotations

from pathlib import Path

from mcp.server.fastmcp import FastMCP

MIME_TYPE = "text/html;profile=mcp-app"
WIDGETS_DIR = Path(__file__).resolve().parent / "widgets"

WIDGET_URIS: dict[str, str] = {
    "population_snapshot": "ui://homecare-cohort/population-snapshot.html",
    "highrisk_cohort": "ui://homecare-cohort/highrisk-cohort.html",
    "patient_profile": "ui://homecare-cohort/patient-profile.html",
    "care_gap_plan": "ui://homecare-cohort/care-gap-plan.html",
}


def register(mcp: FastMCP) -> None:
    for name, uri in WIDGET_URIS.items():
        html_file = WIDGETS_DIR / f"{name}.html"
        _register_widget(mcp, name, uri, html_file)


def _register_widget(
    mcp: FastMCP, name: str, uri: str, html_file: Path
) -> None:
    def _make_reader(path: Path):
        @mcp.resource(
            uri,
            name=name,
            description=f"UI widget for {name.replace('_', ' ')}",
            mime_type=MIME_TYPE,
        )
        def _read() -> str:
            return path.read_text()

    _make_reader(html_file)
