#!/usr/bin/env python3
"""Regression fixtures for state-asset discovery completeness."""

import hashlib
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
import crawl_laender_assets as crawler  # noqa: E402


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def asset_id(url):
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]


class AssetCompletenessTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.old_root = crawler.DATA_ROOT
        self.old_delays = crawler.DELAYS
        crawler.DATA_ROOT = Path(self.temp.name)
        crawler.DELAYS = {"by": (0, 0), "sn": (0, 0), "nw": (0, 0)}

    def tearDown(self):
        crawler.DATA_ROOT = self.old_root
        crawler.DELAYS = self.old_delays
        self.temp.cleanup()

    def _by_partial_fixture(self):
        state_dir = crawler.DATA_ROOT / "by"
        state_dir.mkdir(parents=True)
        root_a = {
            "id": "r1", "title": "Root A", "source_url": "https://example/r1",
        }
        root_b = {
            "id": "r2", "title": "Root B", "source_url": "https://example/r2",
        }
        write_json(state_dir / "index.json", {
            "state": "by", "count": 2, "expected_total": 2,
            "enumeration_complete": True, "items": [root_a, root_b],
        })
        (state_dir / "r1.html").write_text(
            '<!doctype html><html><title>A</title><div id="content">'
            '<div class="document-all"><a href="/Content/Resource?path=A.pdf">'
            "A</a></div></div></html>", encoding="utf-8",
        )
        write_json(state_dir / "manifest.json", [{
            **root_a, "file": "r1.html", "format": "html", "status": "ok",
        }])

        url_a = "https://www.gesetze-bayern.de/Content/Resource?path=A.pdf"
        url_b = "https://www.gesetze-bayern.de/Content/Resource?path=B.pdf"
        records = []
        items = []
        for label, url in (("A", url_a), ("B", url_b)):
            doc_id = asset_id(url)
            rel = "assets/%s_%s.pdf" % (doc_id, label)
            payload = b"%PDF-1.4\nfixture " + label.encode("ascii") + b"\n%%EOF\n"
            path = state_dir / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
            discovery = {
                "id": doc_id, "title": label, "source_url": url,
                "record_role": "integral_linked_asset",
                "discovered_from_ids": ["r1" if label == "A" else "r2"],
            }
            items.append(discovery)
            records.append({
                **discovery, "file": rel, "format": "pdf",
                "content_type": "application/pdf", "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "downloaded_at": "2026-08-05T00:00:00Z", "status": "ok",
            })
        write_json(state_dir / "assets_index.json", {
            "state": "by", "count": 2, "expected_total": 2,
            "discovery_complete": True, "items": items,
        })
        write_json(state_dir / "assets_manifest.json", records)
        return state_dir, items, records

    def test_partial_root_retains_cached_assets_and_expected_count(self):
        state_dir, items, records = self._by_partial_fixture()
        index_before = (state_dir / "assets_index.json").read_bytes()
        manifest_before = (state_dir / "assets_manifest.json").read_bytes()
        file_bytes_before = {
            path.name: path.read_bytes() for path in (state_dir / "assets").iterdir()
        }
        discovered = crawler.discover("by")
        self.assertFalse(discovered[3])
        self.assertEqual(discovered[5], 1)
        result = crawler.crawl_state(
            "by", SimpleNamespace(dry_run=True, limit=None, refresh_index=False)
        )
        self.assertEqual(result, 0)
        self.assertEqual((state_dir / "assets_index.json").read_bytes(), index_before)
        self.assertEqual((state_dir / "assets_manifest.json").read_bytes(), manifest_before)
        self.assertEqual(
            {path.name: path.read_bytes() for path in (state_dir / "assets").iterdir()},
            file_bytes_before,
        )
        self.assertFalse((state_dir / "assets_manifest_stale.json").exists())

        result = crawler.crawl_state(
            "by", SimpleNamespace(dry_run=False, limit=None, refresh_index=False)
        )
        self.assertEqual(result, 0)
        index = json.loads((state_dir / "assets_index.json").read_text())
        self.assertFalse(index["discovery_complete"])
        self.assertFalse(index["root_discovery_complete"])
        self.assertEqual(index["expected_total"], 2)
        self.assertEqual({item["id"] for item in index["items"]},
                         {item["id"] for item in items})
        self.assertEqual(index["cached_assets_retained_due_to_incomplete_root"], 1)
        self.assertIn("root manifest count 1 differs from expected 2",
                      index["root_completeness_reason"])
        current_manifest = json.loads((state_dir / "assets_manifest.json").read_text())
        current_by_id = {item["id"]: item for item in current_manifest}
        for prior in records:
            current = current_by_id[prior["id"]]
            for key in ("file", "status", "sha256", "bytes", "downloaded_at"):
                self.assertEqual(current[key], prior[key])
        self.assertFalse((state_dir / "assets_manifest_stale.json").exists())

    def test_complete_root_archives_removed_asset(self):
        state_dir, items, _records = self._by_partial_fixture()
        root_a = {
            "id": "r1", "title": "Root A", "source_url": "https://example/r1",
        }
        write_json(state_dir / "index.json", {
            "state": "by", "count": 1, "expected_total": 1,
            "enumeration_complete": True, "items": [root_a],
        })
        write_json(state_dir / "manifest.json", [{
            **root_a, "file": "r1.html", "format": "html", "status": "ok",
        }])
        result = crawler.crawl_state(
            "by", SimpleNamespace(dry_run=False, limit=None, refresh_index=False)
        )
        self.assertEqual(result, 0)
        index = json.loads((state_dir / "assets_index.json").read_text())
        self.assertTrue(index["discovery_complete"])
        self.assertEqual(index["expected_total"], 1)
        self.assertEqual([item["id"] for item in index["items"]], [items[0]["id"]])
        manifest = json.loads((state_dir / "assets_manifest.json").read_text())
        self.assertEqual([item["id"] for item in manifest], [items[0]["id"]])
        stale = json.loads((state_dir / "assets_manifest_stale.json").read_text())
        self.assertEqual(stale[0]["id"], items[1]["id"])
        self.assertEqual(stale[0]["status"], "stale_not_currently_linked")

    def test_partial_root_keeps_cached_and_adds_new_discovery(self):
        state_dir, items, _records = self._by_partial_fixture()
        with (state_dir / "r1.html").open("a", encoding="utf-8") as fh:
            fh.write(
                '<a href="/Content/Resource?path=C.pdf">new C</a>'
            )
        discovered, _inspected, _manifest, complete, _reason, retained, _raw = (
            crawler.discover("by")
        )
        url_c = "https://www.gesetze-bayern.de/Content/Resource?path=C.pdf"
        self.assertFalse(complete)
        self.assertEqual(retained, 1)
        self.assertEqual(
            {item["id"] for item in discovered},
            {items[0]["id"], items[1]["id"], asset_id(url_c)},
        )

    def test_state_terminal_statuses_and_payloads(self):
        # SN's one explicit redirect-chain-unavailable source is terminal and
        # intentionally has no file.
        sn_dir = crawler.DATA_ROOT / "sn"
        sn_dir.mkdir(parents=True)
        sn_item = {"id": "sn-unavailable", "title": "Unavailable"}
        sn_index = {
            "items": [sn_item], "supplemental_items": [],
            "count_including_supplementals": 1,
            "enumeration_complete": True,
            "supplemental_discovery_complete": True,
        }
        sn_manifest = [{
            **sn_item, "status": "source_redirect_chain_unresolved", "file": None,
        }]
        complete, reason = crawler.root_completeness(
            "sn", sn_dir, sn_index, sn_manifest
        )
        self.assertTrue(complete, reason)

        # NW wrapper pages are complete only when the fallback is present.
        nw_dir = crawler.DATA_ROOT / "nw"
        nw_dir.mkdir(parents=True)
        nw_item = {"id": "nw-wrapper", "title": "Wrapper"}
        nw_index = {"items": [nw_item], "expected_total": 1}
        nw_manifest = [{
            **nw_item, "status": "wrapper_with_fulltext_fallback",
            "file": "nw-wrapper.html", "format": "html",
        }]
        wrapper = nw_dir / "nw-wrapper.html"
        wrapper.write_text(
            '<!doctype html><html><title>recht.nrw.de</title><h1>Law</h1>'
            '<iframe src="/system/files/BH/full.htm"></iframe></html>',
            encoding="utf-8",
        )
        complete, reason = crawler.root_completeness(
            "nw", nw_dir, nw_index, nw_manifest
        )
        self.assertTrue(complete, reason)
        wrapper.write_text("<html><title>Error</title></html>", encoding="utf-8")
        complete, reason = crawler.root_completeness(
            "nw", nw_dir, nw_index, nw_manifest
        )
        self.assertFalse(complete)
        self.assertIn("validated local HTML payload", reason)

    def test_stream_atomic_cleanup_validation_and_adoption(self):
        class FakeResponse:
            def __init__(self, chunks, length=None):
                self._chunks = chunks
                self.headers = {
                    "Content-Type": "application/pdf",
                    "Content-Disposition": 'attachment; filename="large.pdf"',
                }
                if length is not None:
                    self.headers["Content-Length"] = str(length)
                self.closed = False

            @property
            def content(self):
                raise AssertionError("streaming code must never access .content")

            def iter_content(self, chunk_size=None):
                del chunk_size
                for chunk in self._chunks:
                    if isinstance(chunk, BaseException):
                        raise chunk
                    yield chunk

            def close(self):
                self.closed = True

        assets_dir = crawler.DATA_ROOT / "nw" / "assets"
        item = {
            "id": "stream-fixture", "source_url": "https://example/large.pdf",
            "title": "Large fixture", "record_role": "integral_linked_asset",
            "discovered_from_ids": ["root"],
        }
        payload = b"%PDF-1.7\n" + (b"x" * (2 * 1024 * 1024)) + b"\n%%EOF\n"

        broken = FakeResponse(
            [payload[:1024], RuntimeError("injected midstream failure")],
            len(payload),
        )
        with self.assertRaisesRegex(RuntimeError, "midstream"):
            crawler.stream_response_to_asset(item, broken, assets_dir)
        self.assertTrue(broken.closed)
        self.assertEqual(list(assets_dir.glob("*")), [])

        success = FakeResponse(
            [payload[:900000], payload[900000:1800000], payload[1800000:]],
            len(payload),
        )
        metadata, error = crawler.stream_response_to_asset(item, success, assets_dir)
        self.assertIsNone(error)
        self.assertEqual(metadata["bytes"], len(payload))
        final = crawler.DATA_ROOT / "nw" / metadata["file"]
        self.assertTrue(final.exists())
        self.assertEqual(metadata["sha256"], hashlib.sha256(payload).hexdigest())
        adopted = crawler.adopt_existing_asset(item, assets_dir)
        self.assertEqual(adopted["sha256"], metadata["sha256"])
        self.assertTrue(adopted["adopted_existing_unmanifested"])

        bad_item = dict(item, id="bad-eof")
        bad_pdf = b"%PDF-1.7\ntruncated without marker"
        metadata, error = crawler.stream_response_to_asset(
            bad_item, FakeResponse([bad_pdf], len(bad_pdf)), assets_dir
        )
        self.assertIsNone(metadata)
        self.assertIn("validation", error)
        self.assertFalse(any(assets_dir.glob("bad-eof*")))
        self.assertFalse(any(assets_dir.glob(".bad-eof*")))

        short_disk_item = dict(item, id="short-disk")
        short_response = FakeResponse([payload], len(payload))
        with mock.patch.object(
            crawler.shutil, "disk_usage",
            return_value=SimpleNamespace(free=len(payload)),
        ):
            metadata, error = crawler.stream_response_to_asset(
                short_disk_item, short_response, assets_dir
            )
        self.assertIsNone(metadata)
        self.assertIn("insufficient disk space", error)
        self.assertTrue(short_response.closed)
        self.assertFalse(any(assets_dir.glob("short-disk*")))


if __name__ == "__main__":
    unittest.main()
