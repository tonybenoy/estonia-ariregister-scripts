from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import Header, Footer, Input, DataTable, Static, Label
from textual.binding import Binding
from pathlib import Path
import json

class RegistryTUI(App):
    TITLE = "Estonian Registry Explorer"
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("s", "focus_search", "Search"),
    ]

    def __init__(self, chunks_dir: Path):
        super().__init__()
        self.chunks_dir = chunks_dir
        self.manifest = self._load_manifest()

    def _load_manifest(self):
        manifest_path = self.chunks_dir / 'manifest.json'
        if not manifest_path.exists():
            return None
        with open(manifest_path, 'r') as f:
            return json.load(f)

    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Vertical(
                Label("Search Company (Name or Code):"),
                Input(placeholder="Type to search...", id="search_input"),
                DataTable(id="results_table"),
                Static(id="details_pane", expand=True),
            )
        )
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.add_columns("Code", "Name", "Status", "Location")
        table.cursor_type = "row"
        self.query_one(Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.run_search(event.value)

    def run_search(self, term: str) -> None:
        if not self.manifest:
            self.notify("Manifest not found. Run sync first.", severity="error")
            return

        table = self.query_one(DataTable)
        table.clear()
        
        term_lower = term.lower()
        found = []
        
        # Simple search logic for TUI
        for chunk in self.manifest['chunks']:
            with open(self.chunks_dir / chunk['file'], 'r', encoding='utf-8') as f:
                data = json.load(f)
                for item in data:
                    if term_lower in item.get('nimi', '').lower() or str(item.get('ariregistri_kood')) == term:
                        found.append(item)
                        table.add_row(
                            str(item.get('ariregistri_kood')),
                            item.get('nimi', ''),
                            item.get('staatus', ''),
                            item.get('aadress_linn', '') or item.get('aadress_maakond', '')
                        )
                    if len(found) > 100: break # Limit TUI results for performance
            if len(found) > 100: break
        
        self.search_results = found
        if not found:
            self.notify("No results found.")

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        # Get the selected item's code from the first column of the selected row
        row_data = self.query_one(DataTable).get_row(event.row_key)
        code = row_data[0]
        
        # Find the full data in our search results
        company_data = next((c for c in self.search_results if str(c.get('ariregistri_kood')) == code), None)
        
        if company_data:
            details = self.query_one("#details_pane", Static)
            details.update(f"```json
{json.dumps(company_data, indent=2, ensure_ascii=False)}
```")

    def action_focus_search(self) -> None:
        self.query_one(Input).focus()

if __name__ == "__main__":
    import sys
    chunks = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("chunks")
    app = RegistryTUI(chunks)
    app.run()
