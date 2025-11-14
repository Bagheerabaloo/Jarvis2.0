import ast
import sys
import pathlib
from collections import deque

# 🔧 QUI CAMBI SOLO QUESTO:
MAIN_FILE = "main.py"  # metti qui il tuo main: es. "bot.py", "main.py", ecc.

root = pathlib.Path(".").resolve()
entry = (root / MAIN_FILE).resolve()

if not entry.exists():
    print(f"❌ File di entrypoint non trovato: {entry}")
    sys.exit(1)

# Mappa di tutti i moduli locali (nome modulo -> path)
local_modules = {}

for path in root.rglob("*.py"):
    rel = path.relative_to(root)
    parts = list(rel.parts)

    # costruisci il nome modulo in stile package (es. pkg/sub/module.py -> pkg.sub.module)
    if parts[-1] == "__init__.py":
        parts = parts[:-1]
    else:
        parts[-1] = pathlib.Path(parts[-1]).stem

    if not parts:
        continue

    mod_name = ".".join(parts)
    local_modules[mod_name] = path

# funzione di utilità per trovare moduli locali da un import
def find_local_paths_from_import(current_path, node):
    """Dato un nodo Import/ImportFrom, prova a capire quali file locali (.py) tocca."""
    results = set()

    # Nome del package corrente (per import relativi)
    rel = current_path.relative_to(root)
    current_parts = list(rel.parts)
    if current_parts[-1] == "__init__.py":
        pkg_parts = current_parts[:-1]
    else:
        pkg_parts = current_parts[:-1]
    current_pkg = ".".join(pkg_parts) if pkg_parts else ""

    def resolve_relative(module, level):
        # livello 0 = assoluto
        if level == 0:
            return module

        base_parts = pkg_parts[: max(0, len(pkg_parts) - level)]
        if module:
            base_parts += module.split(".")
        if not base_parts:
            return None
        return ".".join(base_parts)

    if isinstance(node, ast.Import):
        for alias in node.names:
            name = alias.name  # es: "pkg.sub" o "requests"
            # prova match diretto con modulo locale (es. pkg.sub)
            for mod_name, path in local_modules.items():
                if mod_name == name or mod_name.startswith(name + "."):
                    results.add(path)
    elif isinstance(node, ast.ImportFrom):
        mod = node.module  # può essere None (from . import x)
        level = node.level or 0
        full_mod = resolve_relative(mod, level)
        if full_mod:
            # match con modulo locale (package o submodulo)
            for mod_name, path in local_modules.items():
                if mod_name == full_mod or mod_name.startswith(full_mod + "."):
                    results.add(path)

    return results

# 1. Trova tutti i .py raggiungibili dal MAIN_FILE
visited = set()
queue = deque()

visited.add(entry)
queue.append(entry)

while queue:
    current = queue.popleft()
    try:
        code = current.read_text()
    except UnicodeDecodeError:
        continue

    try:
        tree = ast.parse(code, filename=str(current))
    except SyntaxError:
        continue

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for target_path in find_local_paths_from_import(current, node):
                if target_path not in visited:
                    visited.add(target_path)
                    queue.append(target_path)

# 2. Su questi file (visited) ricava tutti i moduli importati
all_imports = set()

for path in visited:
    try:
        code = path.read_text()
    except UnicodeDecodeError:
        continue
    try:
        tree = ast.parse(code, filename=str(path))
    except SyntaxError:
        continue

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                all_imports.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.module:
            all_imports.add(node.module.split(".")[0])

# 3. Filtra: togli stdlib e moduli locali
stdlib = getattr(sys, "stdlib_module_names", set())

local_roots = set()
for mod_name in local_modules.keys():
    local_roots.add(mod_name.split(".")[0])

third_party = []
for m in sorted(all_imports):
    if m in stdlib:
        continue
    if m in local_roots:
        continue
    third_party.append(m)

# 4. Scrivi l'output in un file
out_path = root / f"requirements_for_{pathlib.Path(MAIN_FILE).stem}.txt"
with out_path.open("w") as f:
    for m in third_party:
        f.write(m + "\n")

print("✅ Analisi completata per main:", MAIN_FILE)
print("   File Python raggiungibili da questo main:")
for p in sorted(visited):
    print("   -", p.relative_to(root))
print("\n   Moduli importati totali:", sorted(all_imports))
print("   Librerie esterne (stimate):", third_party)
print(f"\n   Salvato in: {out_path}")
EOF
