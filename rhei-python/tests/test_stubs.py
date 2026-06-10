"""Guard against type-stub drift.

Parses the shipped ``.pyi`` files and checks that every class/method they
declare exists on the live module (and vice-versa for the public surface), so
the stubs can't silently fall out of sync with the compiled extension.
"""

import ast
from pathlib import Path

import rhei
from rhei import _rhei

PKG_DIR = Path(rhei.__file__).parent


def _class_methods_from_pyi(path: Path) -> dict[str, set[str]]:
    """Map ``class name -> set of def names`` declared in a .pyi file."""
    tree = ast.parse(path.read_text())
    out: dict[str, set[str]] = {}
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            out[node.name] = {
                b.name for b in node.body if isinstance(b, ast.FunctionDef)
            }
    return out


def test_py_typed_marker_present():
    assert (PKG_DIR / "py.typed").exists(), "PEP 561 py.typed marker missing"


def test_stub_files_present():
    assert (PKG_DIR / "_rhei.pyi").exists()
    assert (PKG_DIR / "__init__.pyi").exists()


def test_rhei_pyi_matches_compiled_module():
    stub = _class_methods_from_pyi(PKG_DIR / "_rhei.pyi")
    compiled_classes = {
        n for n in dir(_rhei) if isinstance(getattr(_rhei, n), type)
    }

    # Every compiled class must have a stub class.
    assert compiled_classes <= set(stub), (
        f"classes missing from _rhei.pyi: {compiled_classes - set(stub)}"
    )
    # Every stub class must exist on the module.
    assert set(stub) <= compiled_classes, (
        f"stub declares classes not in module: {set(stub) - compiled_classes}"
    )

    # Every method declared in a stub class must exist on the live class.
    for cls_name, stub_methods in stub.items():
        live = getattr(_rhei, cls_name)
        for m in stub_methods:
            assert hasattr(live, m), f"_rhei.{cls_name}.{m} in stub but not on class"

    # Every public (non-dunder) method on a live class must be in the stub.
    # Dunders (__repr__, __len__, __init__, ...) are auto-provided and optional
    # in the stub, so they are not required here.
    for cls_name in compiled_classes:
        live = getattr(_rhei, cls_name)
        live_methods = {
            n
            for n in dir(live)
            if not n.startswith("_") and callable(getattr(live, n))
        }
        missing = live_methods - stub[cls_name]
        assert not missing, f"_rhei.{cls_name} methods missing from stub: {missing}"


def test_init_pyi_exports_match_all():
    """Every name in rhei.__all__ is re-exported / declared in __init__.pyi."""
    tree = ast.parse((PKG_DIR / "__init__.pyi").read_text())
    declared: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            declared.add(node.name)
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                declared.add(alias.asname or alias.name)
    for name in rhei.__all__:
        assert name in declared, f"{name} in __all__ but not declared in __init__.pyi"
