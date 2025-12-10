#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import os
import argparse
import sys
import re
from typing import List, Dict, Any
import zipfile
import xml.etree.ElementTree as ET

# --- Constantes de Normalisation ---

ERROR_PATTERN = (
    r"The specified file .* does not exists or is not readable\.: Invalid file"
)
NORMALIZED_ERROR_MESSAGE = (
    "The specified file {{file_full_path}} does not exists or is not readable.: Invalid file"
)

# --- Classe d'Analyse ---


class SQLiteAnalyzer:
    """
    Classe pour analyser une base de donnÃ©es SQLite spÃ©cifique
    et exÃ©cuter les requÃªtes statistiques requises.
    """

    def __init__(self, db_path: str, table_name: str = "TBL_FSADA"):
        self.db_path = db_path
        self.table_name = table_name

    def _execute_query(self, query: str, fetch_one: bool = False) -> Any:
        """ExÃ©cute une requÃªte et retourne les rÃ©sultats."""
        conn = None
        result = None

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(query)

            if fetch_one:
                fetch_result = cursor.fetchone()
                result = fetch_result[0] if fetch_result else 0
            else:
                result = cursor.fetchall()

        except sqlite3.OperationalError as e:
            sys.stderr.write(
                f"âš ï¸ Erreur OpÃ©rationnelle dans '{os.path.basename(self.db_path)}': {e}\n"
            )
            result = None
        except sqlite3.Error as e:
            sys.stderr.write(
                f"âš ï¸ Erreur SQLite gÃ©nÃ©rale dans '{os.path.basename(self.db_path)}': {e}\n"
            )
            result = None
        finally:
            if conn:
                conn.close()

        return result

    def get_stats(self) -> Dict[str, Any] | None:
        """Calcule toutes les statistiques requises, y compris la normalisation des messages d'erreur."""
        db_name = os.path.basename(self.db_path)
        print(f"â³ Analyse de la base de donnÃ©es: {db_name}")

        table = self.table_name

        try:
            # 1. Nombre total de lignes
            count_total = self._execute_query(
                f"SELECT COUNT(*) FROM {table}", fetch_one=True
            )
            if count_total is None:
                return None

            # 2. is_done = 1
            count_is_done_1 = self._execute_query(
                f"SELECT COUNT(*) FROM {table} WHERE is_done = 1",
                fetch_one=True,
            )

            # 3. cmx_document_id non vide
            count_cmx_not_empty = self._execute_query(
                f"""
                SELECT COUNT(*)
                FROM {table}
                WHERE cmx_document_id IS NOT NULL
                  AND TRIM(cmx_document_id) <> ''
                """,
                fetch_one=True,
            )

            # 4. is_done = 0
            count_is_done_0 = self._execute_query(
                f"SELECT COUNT(*) FROM {table} WHERE is_done = 0",
                fetch_one=True,
            )

            # 5. is_done = 0 et error_message non vide
            count_error_not_empty = self._execute_query(
                f"""
                SELECT COUNT(*)
                FROM {table}
                WHERE is_done = 0
                  AND error_message IS NOT NULL
                  AND TRIM(error_message) <> ''
                """,
                fetch_one=True,
            )

            # 6. RÃ©cupÃ©ration des messages d'erreur bruts
            error_message_counts_raw = self._execute_query(
                f"""
                SELECT error_message
                FROM {table}
                WHERE is_done = 0
                  AND error_message IS NOT NULL
                  AND TRIM(error_message) <> ''
                """
            )

            # --- Normalisation et AgrÃ©gation des Messages d'Erreur ---
            error_message_counts: Dict[str, int] = {}
            for row in (error_message_counts_raw if error_message_counts_raw else []):
                message = row[0] if row and row[0] is not None else ""
                if re.match(ERROR_PATTERN, message):
                    normalized_msg = NORMALIZED_ERROR_MESSAGE
                else:
                    normalized_msg = message

                error_message_counts[normalized_msg] = (
                    error_message_counts.get(normalized_msg, 0) + 1
                )

            return {
                "db_name": db_name,
                "db_path": self.db_path,
                "total_rows": count_total,
                "is_done_1_count": count_is_done_1 or 0,
                "cmx_document_id_not_empty_count": count_cmx_not_empty or 0,
                "is_done_0_count": count_is_done_0 or 0,
                "is_done_0_with_error_count": count_error_not_empty or 0,
                "error_message_breakdown": error_message_counts,
            }

        except Exception as e:
            sys.stderr.write(
                f"âŒ Erreur inattendue lors de l'analyse de '{db_name}': {e}\n"
            )
            return None


# --- Fonctions Utilitaires d'Affichage ---


def print_table(headers: List[str], rows: List[List[Any]], title: str | None = None):
    """Affiche une table simple en ASCII dans la console."""
    if title:
        print(title)

    if not rows:
        print("(aucune donnÃ©e)\n")
        return

    col_widths = [len(str(h)) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    sep = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
    fmt = "|" + "|".join(" {:<" + str(w) + "} " for w in col_widths) + "|"

    print(sep)
    print(fmt.format(*headers))
    print(sep)
    for row in rows:
        print(fmt.format(*[str(c) for c in row]))
    print(sep)
    print()


def format_stats_report(all_stats: List[Dict[str, Any]]):
    """PrÃ©sente les statistiques collectÃ©es sous forme de tableaux."""
    print("\n" + "=" * 80)
    print("âœ¨ RAPPORT D'ANALYSE STATISTIQUE DES BASES TBL_FSADA âœ¨")
    print("=" * 80)

    # Vue d'ensemble par base
    headers = [
        "Base",
        "Total",
        "is_done=1",
        "is_done=0",
        "cmx_document_idâ‰ vide",
        "is_done=0 & erreur",
    ]
    rows = []
    for s in all_stats:
        if not s:
            continue
        rows.append(
            [
                s["db_name"],
                s["total_rows"],
                s["is_done_1_count"],
                s["is_done_0_count"],
                s["cmx_document_id_not_empty_count"],
                s["is_done_0_with_error_count"],
            ]
        )

    print_table(headers, rows, title="\nðŸ“‹ Vue d'ensemble par base")

    # DÃ©tail des erreurs par base
    for s in all_stats:
        if not s:
            continue
        error_breakdown = s["error_message_breakdown"]
        if not error_breakdown:
            continue

        sorted_errors = sorted(
            error_breakdown.items(), key=lambda it: it[1], reverse=True
        )
        err_rows = []
        for msg, cnt in sorted_errors:
            truncated = (msg[:77] + "...") if len(msg) > 80 else msg
            display = (
                "[FICHIER MANQUANT NORMALISÃ‰]"
                if msg == NORMALIZED_ERROR_MESSAGE
                else truncated
            )
            err_rows.append([display, cnt])

        print_table(
            ["Message d'erreur", "Occurrences"],
            err_rows,
            title=f"\nðŸ” DÃ©tail des erreurs pour la base : {s['db_name']}",
        )


def ask_recursive_mode() -> bool:
    """Demande Ã  l'utilisateur s'il souhaite une analyse rÃ©cursive."""
    while True:
        choice = input(
            "\nSouhaitez-vous analyser uniquement le dossier passÃ© en paramÃ¨tre "
            "(S) ou descendre rÃ©cursivement dans tous les sous-dossiers (R) ? [S/R] : "
        ).strip().upper()
        if choice in {"S", "R"}:
            return choice == "R"
        print("RÃ©ponse invalide, merci de taper S ou R.")


# --- Fonction Principale ---


# --- Fonctions d’export Excel ---


def _col_idx_to_name(idx: int) -> str:
    name = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        name = chr(65 + rem) + name
    return name


def _build_excel_rows(all_stats: List[Dict[str, Any]], file_system: str) -> List[List[Any]]:
    rows: List[List[Any]] = []
    for stats in all_stats:
        db_path = stats.get("db_path") or stats.get("db_name") or ""
        total = stats.get("total_rows", 0)
        done = stats.get("is_done_1_count", 0)
        err_total = stats.get("is_done_0_count", 0)
        breakdown = stats.get("error_message_breakdown") or {}

        if breakdown:
            for msg, count in sorted(breakdown.items(), key=lambda it: it[1], reverse=True):
                rows.append(
                    [file_system, db_path, total, done, err_total, msg, count]
                )
        else:
            rows.append(
                [file_system, db_path, total, done, err_total, "", 0]
            )

    return rows


def export_stats_to_excel(template_path: str, output_path: str,
                          all_stats: List[Dict[str, Any]], file_system: str) -> None:
    """Génère un fichier Excel à partir du modèle fourni et des statistiques calculées.

    Le fichier `template_path` doit être le modèle 'entete_stats.xlsx' contenant déjà
    les deux premières lignes d'entête. Les données seront ajoutées à partir de la
    troisième ligne.
    """
    rows_data = _build_excel_rows(all_stats, file_system)
    if not rows_data:
        raise ValueError("Aucune donnée à écrire dans le fichier Excel.")

    with zipfile.ZipFile(template_path, "r") as zin:
        sheet_xml = zin.read("xl/worksheets/sheet1.xml")
        root = ET.fromstring(sheet_xml)

        ns = {
            "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
            "ac": "http://schemas.microsoft.com/office/spreadsheetml/2009/9/ac",
        }

        sheet_data = root.find("main:sheetData", ns)
        if sheet_data is None:
            raise RuntimeError("Impossible de trouver <sheetData> dans le modèle Excel.")

        existing_rows = sheet_data.findall("main:row", ns)
        max_r = 0
        for r in existing_rows:
            try:
                rr = int(r.get("r"))
                if rr > max_r:
                    max_r = rr
            except (TypeError, ValueError):
                pass

        start_row = max_r + 1 if max_r else 1
        ac_ns = "http://schemas.microsoft.com/office/spreadsheetml/2009/9/ac"

        for i, row_values in enumerate(rows_data):
            row_index = start_row + i
            row_attrib = {
                "r": str(row_index),
                "spans": "1:7",
                f"{{{ac_ns}}}dyDescent": "0.25",
            }

            row_elem = ET.Element(
                "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}row",
                row_attrib,
            )

            for col_idx, value in enumerate(row_values, start=1):
                col_name = _col_idx_to_name(col_idx)
                cell_ref = f"{col_name}{row_index}"
                cell_attrib = {"r": cell_ref, "s": "2"}  # style 2 comme la ligne d'exemple

                cell_elem = ET.Element(
                    "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}c",
                    cell_attrib,
                )

                if value is None or value == "":
                    # Cellule vide : on ne met ni <v> ni texte.
                    pass
                elif isinstance(value, (int, float)):
                    v_elem = ET.SubElement(
                        cell_elem,
                        "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v",
                    )
                    v_elem.text = str(value)
                else:
                    cell_elem.set("t", "inlineStr")
                    is_elem = ET.SubElement(
                        cell_elem,
                        "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}is",
                    )
                    t_elem = ET.SubElement(
                        is_elem,
                        "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t",
                    )
                    t_elem.text = str(value)

                row_elem.append(cell_elem)

            sheet_data.append(row_elem)

        new_sheet_xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)

        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data_bytes = zin.read(item.filename)
                if item.filename == "xl/worksheets/sheet1.xml":
                    data_bytes = new_sheet_xml
                zout.writestr(item, data_bytes)



def main():
    parser = argparse.ArgumentParser(
        description=(
            "Analyse des statistiques des bases de donnÃ©es SQLite contenant "
            "la table TBL_FSADA."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "folder_path",
        type=str,
        help=(
            "Chemin du dossier contenant les bases de donnÃ©es SQLite "
            "(extensions .sqlite ou .db)."
        ),
    )
    parser.add_argument(
        "-x",
        "--excel-output",
        dest="excel_output",
        type=str,
        help=(
            "Chemin du fichier Excel de sortie à générer. "
            "L'entête sera basée sur le modèle 'entete_stats.xlsx' situé à côté du script."
        ),
    )


    args = parser.parse_args()

    if not os.path.isdir(args.folder_path):
        sys.stderr.write(
            f"\nðŸ›‘ Le chemin spÃ©cifiÃ© n'est pas un rÃ©pertoire valide : {args.folder_path}\n"
        )
        sys.exit(1)

    recursive = ask_recursive_mode()

    # { dossier_absolu: [liste de fichiers .db/.sqlite] }
    folder_to_dbs: Dict[str, List[str]] = {}

    if recursive:
        for root, _, files in os.walk(args.folder_path):
            dbs = [
                os.path.join(root, f)
                for f in files
                if f.endswith((".sqlite", ".db"))
            ]
            if dbs:
                folder_to_dbs[root] = dbs
    else:
        dbs = [
            os.path.join(args.folder_path, f)
            for f in os.listdir(args.folder_path)
            if f.endswith((".sqlite", ".db"))
        ]
        if dbs:
            folder_to_dbs[os.path.abspath(args.folder_path)] = dbs

    if not folder_to_dbs:
        print(
            f"\nðŸ›‘ Aucun fichier '.sqlite' ou '.db' trouvÃ© dans : {args.folder_path}"
        )
        return

    all_stats: List[Dict[str, Any]] = []
    folder_summary: Dict[str, Dict[str, int]] = {}

    for folder, db_files in folder_to_dbs.items():
        print(f"\nðŸ“‚ Dossier : {folder}")
        folder_totals = {
            "total_rows": 0,
            "is_done_1_count": 0,
            "is_done_0_count": 0,
            "cmx_document_id_not_empty_count": 0,
            "is_done_0_with_error_count": 0,
        }

        for db_file in db_files:
            analyzer = SQLiteAnalyzer(db_file)
            stats = analyzer.get_stats()
            if not stats:
                continue
            all_stats.append(stats)

            folder_totals["total_rows"] += stats["total_rows"]
            folder_totals["is_done_1_count"] += stats["is_done_1_count"]
            folder_totals["is_done_0_count"] += stats["is_done_0_count"]
            folder_totals[
                "cmx_document_id_not_empty_count"
            ] += stats["cmx_document_id_not_empty_count"]
            folder_totals[
                "is_done_0_with_error_count"
            ] += stats["is_done_0_with_error_count"]

        folder_summary[folder] = folder_totals

    if all_stats:
        format_stats_report(all_stats)

        synth_rows = []
        for folder, agg in folder_summary.items():
            synth_rows.append(
                [
                    folder,
                    agg["total_rows"],
                    agg["is_done_1_count"],
                    agg["is_done_0_count"],
                    agg["cmx_document_id_not_empty_count"],
                    agg["is_done_0_with_error_count"],
                ]
            )

        print_table(
            ["Dossier", "Total", "is_done=1", "is_done=0",
             "cmx_document_idâ‰ vide", "is_done=0 & erreur"],
            synth_rows,
            title="\nðŸ“Š SynthÃ¨se globale par dossier",
        )
        # Export Excel si demandé
        if getattr(args, "excel_output", None):
            file_system_name = input(
                "Veuillez saisir le nom du File système pour la colonne 'File Système' : "
            ).strip()
            if file_system_name:
                template_path = os.path.join(
                    os.path.dirname(__file__),
                    "entete_stats.xlsx",
                )
                if not os.path.exists(template_path):
                    sys.stderr.write(
                        "\n⚠️ Modèle 'entete_stats.xlsx' introuvable à côté du script. "
                        "Export Excel annulé.\n"
                    )
                else:
                    try:
                        export_stats_to_excel(
                            template_path=template_path,
                            output_path=args.excel_output,
                            all_stats=all_stats,
                            file_system=file_system_name,
                        )
                        print(
                            f"\n📊 Fichier Excel généré : {args.excel_output}"
                        )
                    except Exception as exc:
                        sys.stderr.write(
                            f"\n⚠️ Échec de la génération du fichier Excel : {exc}\n"
                        )
            else:
                print(
                    "\n⚠️ Aucun nom de File système renseigné, export Excel annulé."
                )


    else:
        print(
            "\nðŸ˜” Aucune statistique n'a pu Ãªtre collectÃ©e avec succÃ¨s. "
            "VÃ©rifiez les erreurs ci-dessus."
        )


if __name__ == "__main__":
    main();
