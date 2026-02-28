import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function main() {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
  const comboboxSource = await readFile(
    path.resolve(root, "frontend/src/components/Combobox.jsx"),
    "utf8"
  );

  assert(
    /export\s+default\s+function\s+Combobox\s*\(/.test(comboboxSource),
    "Combobox component export is missing"
  );
  assert(
    comboboxSource.includes('role="combobox"'),
    "Combobox input should include role=\"combobox\""
  );
  assert(
    comboboxSource.includes('role="listbox"'),
    "Combobox list should include role=\"listbox\""
  );
  assert(
    comboboxSource.includes('role="option"'),
    "Combobox options should include role=\"option\""
  );
  assert(
    comboboxSource.includes("handleInputKeyDown"),
    "Combobox should implement keyboard handling"
  );
  assert(
    comboboxSource.includes("ArrowDown") &&
      comboboxSource.includes("ArrowUp") &&
      comboboxSource.includes("Enter") &&
      comboboxSource.includes("Escape"),
    "Combobox keyboard handling should support ArrowUp/ArrowDown/Enter/Escape"
  );
  assert(
    comboboxSource.includes("onInputChange"),
    "Combobox should expose onInputChange hook for typeahead flows"
  );
  assert(
    comboboxSource.includes("onChange"),
    "Combobox should expose onChange hook for selected option updates"
  );
  assert(
    comboboxSource.includes("loadingText"),
    "Combobox should include loading state rendering"
  );

  console.log("PR-UX10 smoke test passed (shared Combobox component shape + accessibility markers).");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
