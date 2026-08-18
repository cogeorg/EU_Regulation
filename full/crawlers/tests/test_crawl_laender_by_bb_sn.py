#!/usr/bin/env python3
"""Regression tests for request-bound portal throttling."""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


MODULE_DIR = Path(__file__).resolve().parent
if MODULE_DIR.name == "tests":
    MODULE_DIR = MODULE_DIR.parent
sys.path.insert(0, str(MODULE_DIR))
import crawl_laender_by_bb_sn as crawler  # noqa: E402


class FakeSession:
    def __init__(self):
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return object()


class PoliteRequestTest(unittest.TestCase):
    def setUp(self):
        crawler._LAST_REQUEST_AT.clear()

    def tearDown(self):
        crawler._LAST_REQUEST_AT.clear()

    def test_bb_delay_is_between_real_gets(self):
        session = FakeSession()
        with mock.patch.object(crawler.random, "uniform", return_value=20.0), \
                mock.patch.object(
                    crawler.time, "monotonic", side_effect=[100.0, 105.0, 120.0]
                ), mock.patch.object(crawler.time, "sleep") as sleep:
            crawler.polite_get(session, "https://example/one", "bb", timeout=60)
            crawler.polite_get(session, "https://example/two", "bb", timeout=60)

        self.assertEqual(len(session.calls), 2)
        sleep.assert_called_once_with(15.0)
        self.assertEqual(crawler._LAST_REQUEST_AT["bb"], 120.0)

    def test_cached_bb_record_neither_requests_nor_sleeps(self):
        with tempfile.TemporaryDirectory() as temp:
            old_root = crawler.DATA_ROOT
            crawler.DATA_ROOT = temp
            try:
                state_dir = Path(temp) / "bb"
                state_dir.mkdir(parents=True)
                item = {
                    "id": "ges_fixture",
                    "title": "Fixture",
                    "source_url": "https://bravors.brandenburg.de/gesetze/fixture",
                }
                (state_dir / "index.json").write_text(json.dumps({
                    "state": "bb", "generated_at": "fixture", "count": 1,
                    "expected_total": None, "enumeration_complete": True,
                    "inventory_scope": crawler.SCOPE_NOTES["bb"], "items": [item],
                }), encoding="utf-8")
                (state_dir / "ges_fixture.html").write_text(
                    '<!doctype html><html><head><title>Fixture</title></head>'
                    '<body><div id="content">Valid BRAVORS text</div></body></html>',
                    encoding="utf-8",
                )
                (state_dir / "manifest.json").write_text(json.dumps([{
                    **item, "file": "ges_fixture.html", "format": "html",
                    "status": "ok", "downloaded_at": "fixture",
                }]), encoding="utf-8")

                session = FakeSession()
                args = SimpleNamespace(
                    refresh_index=False, dry_run=False, limit=None,
                )
                with mock.patch.object(crawler, "make_session", return_value=session), \
                        mock.patch.object(crawler.time, "sleep") as sleep:
                    result = crawler.crawl_state("bb", args)

                self.assertEqual(result, 0)
                self.assertEqual(session.calls, [])
                sleep.assert_not_called()
            finally:
                crawler.DATA_ROOT = old_root

    def test_legacy_complete_index_gets_explicit_completeness_fields(self):
        with tempfile.TemporaryDirectory() as temp:
            old_root = crawler.DATA_ROOT
            crawler.DATA_ROOT = temp
            try:
                state_dir = Path(temp) / "bb"
                state_dir.mkdir(parents=True)
                item = {
                    "id": "ges_fixture", "title": "Fixture",
                    "source_url": "https://bravors.brandenburg.de/gesetze/fixture",
                }
                index_path = state_dir / "index.json"
                index_path.write_text(json.dumps({
                    "state": "bb", "generated_at": "fixture", "count": 1,
                    "inventory_scope": crawler.SCOPE_NOTES["bb"], "items": [item],
                }), encoding="utf-8")

                self.assertEqual(crawler.get_index(FakeSession(), "bb", False), [item])
                normalized = json.loads(index_path.read_text(encoding="utf-8"))
                self.assertIsNone(normalized["expected_total"])
                self.assertTrue(normalized["enumeration_complete"])
            finally:
                crawler.DATA_ROOT = old_root

    def test_bb_supplemental_roles_do_not_overclaim_document_type(self):
        root = {
            "id": "ges_fixture", "title": "Fixture",
            "source_url": "https://bravors.brandenburg.de/gesetze/fixture",
        }
        html = """<html><body>
          <a href="/sixcms/detail.php/235934">Nichtamtliche Textversion A</a>
          <a href="/sixcms/detail.php/246500">NICHT AMTLICHE Textversion B</a>
          <a href="/sixcms/detail.php/231722">Gesetz- und Verordnungsblatt</a>
          <a href="/sixcms/detail.php/241254">Anlage: Verkehrszeichen</a>
        </body></html>"""

        items = {
            item["id"]: item
            for item in crawler.extract_supplemental_items("bb", root, html)
        }
        self.assertEqual(
            items["supp_detail_235934"]["record_role"],
            "supplemental_nonofficial_text_version",
        )
        self.assertEqual(
            items["supp_detail_246500"]["record_role"],
            "supplemental_nonofficial_text_version",
        )
        for doc_id in ("supp_detail_231722", "supp_detail_241254"):
            self.assertEqual(
                items[doc_id]["record_role"], "supplemental_linked_document"
            )
            self.assertIn(
                "outside the primary chronological inventory",
                items[doc_id]["category"],
            )

    def test_bb_broken_detail_recovery_and_terminal_records_are_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            old_root = crawler.DATA_ROOT
            old_delays = crawler.DEFAULT_DELAYS
            crawler.DATA_ROOT = temp
            crawler.DEFAULT_DELAYS = {**old_delays, "bb": (0, 0)}
            try:
                state_dir = Path(temp) / "bb"
                state_dir.mkdir(parents=True)
                root = {
                    "id": "ges_fixture", "title": "Fixture",
                    "source_url": "https://bravors.brandenburg.de/gesetze/fixture",
                }
                links = "".join(
                    '<a href="/sixcms/detail.php/%s">%s</a>' % pair
                    for pair in (
                        ("231722", "GVBl. I 2011 Nr. 3"),
                        ("235934", "Nichtamtliche Textversion A"),
                        ("241254", "Anlage Verkehrszeichen"),
                        ("246500", "Nichtamtliche Textversion B"),
                    )
                )
                (state_dir / "ges_fixture.html").write_text(
                    '<!doctype html><html><head><title>Fixture</title></head>'
                    '<body><div id="content">%s</div></body></html>' % links,
                    encoding="utf-8",
                )
                (state_dir / "index.json").write_text(json.dumps({
                    "state": "bb", "generated_at": "fixture", "count": 1,
                    "expected_total": None, "enumeration_complete": True,
                    "inventory_scope": crawler.SCOPE_NOTES["bb"], "items": [root],
                }), encoding="utf-8")
                (state_dir / "manifest.json").write_text(json.dumps([{
                    **root, "file": "ges_fixture.html", "format": "html",
                    "status": "ok", "downloaded_at": "fixture",
                }]), encoding="utf-8")

                pdf = b"%PDF-1.4\nfixture\n%%EOF\n"

                def response_for(url, **_kwargs):
                    response = crawler.requests.Response()
                    response.status_code = 200
                    response.url = url
                    response.headers["Content-Type"] = (
                        "application/pdf" if url.endswith(".pdf")
                        else "text/html; charset=utf-8"
                    )
                    response._content = (
                        pdf if url.endswith(".pdf") else b"invalid template"
                    )
                    response.encoding = "utf-8"
                    return response

                session = mock.Mock()
                session.get.side_effect = response_for
                args = SimpleNamespace(
                    refresh_index=False, dry_run=False, limit=None,
                )
                with mock.patch.object(crawler, "make_session", return_value=session):
                    self.assertEqual(crawler.crawl_state("bb", args), 0)

                manifest = {
                    item["id"]: item
                    for item in json.loads(
                        (state_dir / "manifest.json").read_text(encoding="utf-8")
                    )
                }
                for doc_id in ("supp_detail_231722", "supp_detail_241254"):
                    item = manifest[doc_id]
                    self.assertEqual(item["status"], "ok")
                    self.assertEqual(item["format"], "pdf")
                    self.assertTrue(item["file"].endswith(".pdf"))
                    self.assertEqual((state_dir / item["file"]).read_bytes(), pdf)
                    self.assertEqual(item["sha256"], crawler.hashlib.sha256(pdf).hexdigest())
                    self.assertIn("discovered_source_url", item)
                for doc_id in ("supp_detail_235934", "supp_detail_246500"):
                    item = manifest[doc_id]
                    self.assertEqual(item["status"], "source_link_invalid_template")
                    self.assertIsNone(item["file"])
                    self.assertIsNone(item["format"])
                    self.assertIsNone(item["downloaded_at"])
                    self.assertEqual(
                        item["source_payload_sha256"],
                        "d9c938f968a3eaf9a8695dbc5b95138895d59798550d3df45516c391f3edf3b2",
                    )

                second_session = FakeSession()
                with mock.patch.object(
                        crawler, "make_session", return_value=second_session), \
                        mock.patch.object(crawler.time, "sleep") as sleep:
                    self.assertEqual(crawler.crawl_state("bb", args), 0)
                self.assertEqual(second_session.calls, [])
                sleep.assert_not_called()
            finally:
                crawler.DATA_ROOT = old_root
                crawler.DEFAULT_DELAYS = old_delays


if __name__ == "__main__":
    unittest.main()
