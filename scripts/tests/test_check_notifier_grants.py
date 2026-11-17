"""Tests for scripts/check-notifier-grants.py.

uv run python -m unittest discover -s scripts/tests
"""

import importlib.util
import sys
import unittest
from pathlib import Path
from types import ModuleType

SCRIPT = Path(__file__).resolve().parent.parent / "check-notifier-grants.py"


def load_script() -> ModuleType:
    """Import the hyphenated script as a module."""
    spec = importlib.util.spec_from_file_location("check_notifier_grants", SCRIPT)
    if spec is None or spec.loader is None:
        raise ImportError(SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_notifier_grants"] = module
    spec.loader.exec_module(module)
    return module


check = load_script()

GROUPED = """roles:
  notifier:
    groups: [read_shared, schema_gate]
tables:
  stations: {class: reference, grants: {api: S}}
  users: {class: personal, grants: {notifier: [{privileges: S, columns: [id]}]}}
  line_status: {class: ingest, grants: {api: S}}
"""
NARROWED = """roles:
  notifier:
    groups: [schema_gate]
tables:
  stations: {class: reference, grants: {api: S, notifier: S}}
  users: {class: personal, grants: {notifier: [{privileges: S, columns: [id]}]}}
  line_status: {class: ingest, grants: {api: S}}
  trains: {class: shared-train, grants: {notifier: [{privileges: S, columns: [id]}]}}
"""
SQL = (
    'sqlx::query("SELECT s.name FROM stations s JOIN users u ON u.id = s.id '
    'WHERE np.line_status")\n'
    "// FROM line_status in a comment\n"
    "#[cfg(test)]\nmod tests {\n"
    '    fn t() { query("SELECT 1 FROM line_status"); }\n'
    "}\n"
)


def used(sql: str, relations: dict[str, object]) -> dict[str, set[str]]:
    """Return the relations `sql` (one file) names."""
    result: dict[str, set[str]] = check.used_relations([("queries.rs", sql)], relations)
    return result


class NotifierGrantsTest(unittest.TestCase):
    """The check passes for granted relations and fails for a missed one."""

    def test_group_member_covers_group_classes(self) -> None:
        """A read_shared member needs no grant of its own on a group class."""
        relations, in_group = check.parse_grants(GROUPED)
        self.assertTrue(in_group)
        found = check.problems(used(SQL, relations), relations, in_group=in_group)
        self.assertEqual(found, [])

    def test_narrowed_role_with_its_own_grants_passes(self) -> None:
        """A narrowed role with a grant on every relation passes."""
        relations, in_group = check.parse_grants(NARROWED)
        self.assertFalse(in_group)
        self.assertEqual(sorted(used(SQL, relations)), ["stations", "users"])
        self.assertEqual(
            check.problems(used(SQL, relations), relations, in_group=in_group), []
        )

    def test_narrowed_role_missing_a_table_fails(self) -> None:
        """Leaving the group without its own grant on a table fails."""
        relations, in_group = check.parse_grants(GROUPED.replace("[read_shared, ", "["))
        self.assertFalse(in_group)
        found = check.problems(used(SQL, relations), relations, in_group=in_group)
        self.assertEqual(len(found), 1)
        self.assertIn("stations", found[0])

    def test_column_limited_grant_does_not_replace_the_group(self) -> None:
        """A column-limited grant does not stand in for the group."""
        relations, in_group = check.parse_grants(NARROWED)
        sql = 'query("SELECT id FROM trains")'
        found = check.problems(used(sql, relations), relations, in_group=in_group)
        self.assertEqual(len(found), 1)
        self.assertIn("table-level SELECT", found[0])

    def test_a_column_named_like_a_table_is_not_a_use(self) -> None:
        """A column that shares a table name is not a relation use."""
        relations, _ = check.parse_grants(NARROWED)
        self.assertEqual(used('query("WHERE np.line_status")', relations), {})

    def test_test_code_and_comments_are_skipped(self) -> None:
        """Test modules and comments are not scanned."""
        relations, _ = check.parse_grants(NARROWED)
        self.assertNotIn("line_status", used(SQL, relations))

    def test_table_level_select_detection(self) -> None:
        """Table-level SELECT is told from a column-limited grant."""
        self.assertTrue(check.has_table_select("S"))
        self.assertTrue(check.has_table_select("SIU"))
        self.assertTrue(check.has_table_select("[S, {privileges: U, columns: [a]}]"))
        self.assertFalse(check.has_table_select("[{privileges: S, columns: [a]}]"))
        self.assertFalse(check.has_table_select("I"))


if __name__ == "__main__":
    unittest.main()
