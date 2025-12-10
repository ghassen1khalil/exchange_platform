#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import os
import argparse
import sys
import re
import csv
from typing import List, Dict, Any

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
    Classe pour analyser une base de données SQLite spécifique
    et exécuter les requêtes statistiques requises.
    """

    def __init__(self, db_path: str, table_name: str = "TBL_FSADA"):
        self.db_path = db_path
        self.table_name = table_name

    def _execute_query(self, query: str, fetch_one: bool = False) -> Any:
        """Exécute une requête et retourne les résultats."""
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
                f"⚠️  Erreur Opérationnelle dans '{os.path.basename(self.db_path)}': {e}\n"
            )
            result = None
        except sqlite3.Error as e:
            sys.stderr.write(
                f"⚠️  Erreur SQLite générale dans '{os.path.basename(self.db_path)}': {e}\n"
            )
            result = None
        finally:
            if conn:
                conn.close()

        return result

    def get_stats(self) -> Dict[str, Any] | None:
        """Calcule toutes les statistiques requises."""
        db_name = os.path.basename(self.db_path)
        print(f"⏳ Analyse de la base de données: {db_name}")

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

            # 6. Récupération des messages d'erreur bruts
            error_message_counts_raw = self._execute_query(
                f"""
                SELECT error_message
                FROM {table}
                WHERE is_done = 0
                  AND error_message IS NOT NULL
                  AND TRIM(error_message) <> ''
                """
            )

            # --- Normalisation et Agrégation des Messages d'Erreur ---
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
                "db_path": self.db_path,  # Ajout du chemin complet pour l'export Excel
                "total_rows": count_total,
                "is_done_1_count": count_is_done_1 or 0,
                "cmx_document_id_not_empty_count": count_cmx_not_empty or 0,
                "is_done_0_count": count_is_done_0 or 0,
                "is_done_0_with_error_count": count_error_not_empty or 0,
                "error_message_breakdown": error_message_counts,
            }

        except Exception as e:
            sys.stderr.write(
                f"❌ Erreur inattendue lors de l'analyse de '{db_name}': {e}\n"
            )
            return None


# --- Fonctions Utilitaires d'Affichage ---


def print_table(headers: List[str], rows: List[List[Any]], title: str | None = None):
    """Affiche une table simple en ASCII dans la console."""
    if title:
        print(title)

    if not rows:
        print("(aucune donnée)\n")
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
    """Présente les statistiques collectées sous forme de tableaux."""
    print("\n" + "=" * 80)
    print("✨ RAPPORT D'ANALYSE STATISTIQUE DES BASES TBL_FSADA ✨")
    print("=" * 80)

    # Vue d'ensemble par base
    headers = [
        "Base",
        "Total",
        "is_done=1",
        "is_done=0",
        "cmx_document_id≠vide",
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

    print_table(headers, rows, title="\n📋 Vue d'ensemble par base")

    # Détail des erreurs par base
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
                "[FICHIER MANQUANT NORMALISÉ]"
                if msg == NORMALIZED_ERROR_MESSAGE
                else truncated
            )
            err_rows.append([display, cnt])

        print_table(
            ["Message d'erreur", "Occurrences"],
            err_rows,
            title=f"\n🔍 Détail des erreurs pour la base : {s['db_name']}",
        )


# --- Fonction d'Export Excel / CSV ---


def export_to_excel_csv(all_stats: List[Dict[str, Any]]):
    """
    Génère un fichier CSV compatible Excel avec les colonnes demandées.
    Utilise ';' comme séparateur et UTF-8-SIG pour compatibilité Excel.
    """
    print("\n" + "=" * 80)
    print("📂 GÉNÉRATION DE L'EXPORT EXCEL")
    print("=" * 80)
    
    fs_system_name = input("Veuillez renseigner le nom du 'File Système' : ").strip()
    
    filename = "stats_fsada_export.csv"
    
    # En-têtes demandés
    headers = [
        "File système",
        "Database",
        "Total",
        "Total migré",
        "Total erreur",
        "Message d'erreur",
        "Total (par erreur)"
    ]

    try:
        # encoding='utf-8-sig' ajoute le BOM, ce qui force Excel à lire en UTF-8 correctement
        with open(filename, mode='w', newline='', encoding='utf-8-sig') as csvfile:
            # Le délimiteur ';' est standard pour les CSV s'ouvrant dans Excel en version FR
            writer = csv.writer(csvfile, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            
            writer.writerow(headers)
            
            rows_written = 0
            
            for stat in all_stats:
                if not stat:
                    continue

                # Données communes à la base de données
                base_info = [
                    fs_system_name,
                    stat["db_path"],      # Chemin complet
                    stat["total_rows"],
                    stat["is_done_1_count"],
                    stat["is_done_0_count"]
                ]
                
                errors = stat["error_message_breakdown"]
                
                if errors:
                    # Si on a des erreurs, on crée une ligne par type d'erreur
                    # Tri par nombre décroissant pour la lisibilité
                    sorted_errors = sorted(errors.items(), key=lambda item: item[1], reverse=True)
                    
                    for msg, count in sorted_errors:
                        # On nettoie un peu le message (sauts de ligne) pour le CSV
                        clean_msg = msg.replace("\n", " ").replace("\r", "")
                        row = base_info + [clean_msg, count]
                        writer.writerow(row)
                        rows_written += 1
                else:
                    # Si pas d'erreurs, on écrit quand même une ligne pour la base
                    row = base_info + ["", 0]
                    writer.writerow(row)
                    rows_written += 1
                    
        print(f"\n✅ Fichier généré avec succès : {os.path.abspath(filename)}")
        print(f"ℹ️  Nombre de lignes générées : {rows_written}")
        print("ℹ️  Note : Ce fichier est un CSV formaté pour Excel (;).")

    except PermissionError:
        print(f"\n❌ Erreur : Impossible d'écrire dans '{filename}'. Le fichier est peut-être déjà ouvert.")
    except Exception as e:
        print(f"\n❌ Erreur lors de la génération du fichier : {e}")


def ask_recursive_mode() -> bool:
    """Demande à l'utilisateur s'il souhaite une analyse récursive."""
    while True:
        choice = input(
            "\nSouhaitez-vous analyser uniquement le dossier passé en paramètre "
            "(S) ou descendre récursivement dans tous les sous-dossiers (R) ? [S/R] : "
        ).strip().upper()
        if choice in {"S", "R"}:
            return choice == "R"
        print("Réponse invalide, merci de taper S ou R.")


def ask_export_mode() -> bool:
    """Demande à l'utilisateur s'il souhaite générer le fichier Excel."""
    while True:
        choice = input(
            "\nSouhaitez-vous générer un fichier export Excel des résultats ? [O/N] : "
        ).strip().upper()
        if choice in {"O", "N", "Y"}: # Accepte Y pour Yes au cas où
            return choice in {"O", "Y"}
        print("Réponse invalide, merci de taper O (Oui) ou N (Non).")


# --- Fonction Principale ---


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Analyse des statistiques des bases de données SQLite contenant "
            "la table TBL_FSADA."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "folder_path",
        type=str,
        help=(
            "Chemin du dossier contenant les bases de données SQLite "
            "(extensions .sqlite ou .db)."
        ),
    )

    args = parser.parse_args()

    if not os.path.isdir(args.folder_path):
        sys.stderr.write(
            f"\n⛔ Le chemin spécifié n'est pas un répertoire valide : {args.folder_path}\n"
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
            f"\n⛔ Aucun fichier '.sqlite' ou '.db' trouvé dans : {args.folder_path}"
        )
        return

    all_stats: List[Dict[str, Any]] = []
    folder_summary: Dict[str, Dict[str, int]] = {}

    for folder, db_files in folder_to_dbs.items():
        print(f"\n📂 Dossier : {folder}")
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
        # Affichage Console
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
             "cmx_document_id≠vide", "is_done=0 & erreur"],
            synth_rows,
            title="\n📊 Synthèse globale par dossier",
        )
        
        # Demande d'Export Excel
        if ask_export_mode():
            export_to_excel_csv(all_stats)
            
    else:
        print(
            "\n😔 Aucune statistique n'a pu être collectée avec succès. "
            "Vérifiez les erreurs ci-dessus."
        )


if __name__ == "__main__":
    main()
