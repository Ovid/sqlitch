#!/usr/bin/env python3
"""
Extract translatable messages from sqlitch source code.

This script scans the sqlitch codebase for translatable strings marked with
__(), __x(), and __n() functions and generates/updates POT and PO files.
"""

import ast
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


class MessageExtractor(ast.NodeVisitor):
    """AST visitor to extract translatable messages from Python code."""

    def __init__(self):
        self.messages: Set[Tuple[str, Optional[str]]] = set()
        self.current_file = ""
        self.current_line = 0

    def extract_from_file(self, file_path: Path) -> None:
        """Extract messages from a Python file."""
        self.current_file = str(file_path)

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            tree = ast.parse(content, filename=str(file_path))
            self.visit(tree)
        except (SyntaxError, UnicodeDecodeError) as e:
            print(f"Warning: Could not parse {file_path}: {e}", file=sys.stderr)

    def visit_Call(self, node: ast.Call) -> None:
        """Visit function calls to find translation functions."""
        if isinstance(node.func, ast.Name):
            func_name = node.func.id

            if func_name == "__" and len(node.args) >= 1:
                # Simple translation: __("message")
                if isinstance(node.args[0], ast.Constant):
                    message = node.args[0].value
                    if isinstance(message, str):
                        self.messages.add((message, None))

            elif func_name == "__x" and len(node.args) >= 1:
                # Parameterized translation: __x("message", param=value)
                if isinstance(node.args[0], ast.Constant):
                    message = node.args[0].value
                    if isinstance(message, str):
                        self.messages.add((message, None))

            elif func_name == "__n" and len(node.args) >= 2:
                # Plural translation: __n("singular", "plural", count)
                if isinstance(node.args[0], ast.Constant) and isinstance(
                    node.args[1], ast.Constant
                ):
                    singular = node.args[0].value
                    plural = node.args[1].value
                    if isinstance(singular, str) and isinstance(plural, str):
                        self.messages.add((singular, plural))

        self.generic_visit(node)


def find_python_files(root_dir: Path) -> List[Path]:
    """Find all Python files in the sqlitch package."""
    python_files = []

    for root, dirs, files in os.walk(root_dir):
        # Skip certain directories
        dirs[:] = [d for d in dirs if not d.startswith(".") and d != "__pycache__"]

        for file in files:
            if file.endswith(".py"):
                python_files.append(Path(root) / file)

    return python_files


def extract_messages_from_codebase(sqlitch_dir: Path) -> Set[Tuple[str, Optional[str]]]:
    """Extract all translatable messages from the sqlitch codebase."""
    extractor = MessageExtractor()
    python_files = find_python_files(sqlitch_dir)

    print(f"Scanning {len(python_files)} Python files for translatable messages...")

    for file_path in python_files:
        extractor.extract_from_file(file_path)

    print(f"Found {len(extractor.messages)} unique translatable messages")
    return extractor.messages


def generate_pot_file(
    messages: Set[Tuple[str, Optional[str]]], output_path: Path
) -> None:
    """Generate a POT template file from extracted messages."""

    with open(output_path, "w", encoding="utf-8") as f:
        # Write POT header
        f.write(
            f"""# Sqlitch Localization Messages
# Copyright (c) 2025 Sqlitch Contributors
# This file is distributed under the same license as the sqlitch package.
#
#, fuzzy
msgid ""
msgstr ""
"Project-Id-Version: sqlitch 1.0.0\\n"
"Report-Msgid-Bugs-To: \\n"
"POT-Creation-Date: {datetime.now().strftime('%Y-%m-%d %H:%M%z')}\\n"
"PO-Revision-Date: YEAR-MO-DA HO:MI+ZONE\\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\\n"
"Language-Team: LANGUAGE <LL@li.org>\\n"
"Language: \\n"
"MIME-Version: 1.0\\n"
"Content-Type: text/plain; charset=UTF-8\\n"
"Content-Transfer-Encoding: 8bit\\n"
"Plural-Forms: nplurals=INTEGER; plural=EXPRESSION;\\n"

"""
        )

        # Write messages
        sorted_messages = sorted(messages)
        for message, plural in sorted_messages:
            f.write(f'msgid "{escape_po_string(message)}"\n')
            if plural:
                f.write(f'msgid_plural "{escape_po_string(plural)}"\n')
                f.write('msgstr[0] ""\n')
                f.write('msgstr[1] ""\n')
            else:
                f.write('msgstr ""\n')
            f.write("\n")


def escape_po_string(s: str) -> str:
    """Escape a string for use in PO files."""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def update_po_file(pot_path: Path, po_path: Path) -> None:
    """Update a PO file with new messages from POT file."""
    if not po_path.exists():
        print(f"PO file {po_path} does not exist, skipping update")
        return

    # For now, just print a message. In a full implementation,
    # we would use msgmerge or implement PO file merging logic
    print(f"Would update {po_path} with messages from {pot_path}")
    print("Note: Use 'msgmerge' command to update PO files in production")


def main():
    """Main entry point for message extraction."""
    script_dir = Path(__file__).parent
    sqlitch_dir = script_dir.parent
    locale_dir = script_dir / "locale"

    # Extract messages from codebase
    messages = extract_messages_from_codebase(sqlitch_dir)

    # Generate POT file
    pot_path = locale_dir / "sqlitch.pot"
    generate_pot_file(messages, pot_path)
    print(f"Generated POT file: {pot_path}")

    # Update PO files
    for lang in ["de_DE", "fr_FR", "it_IT"]:
        po_path = locale_dir / lang / "LC_MESSAGES" / "sqlitch.po"
        update_po_file(pot_path, po_path)


if __name__ == "__main__":
    main()
