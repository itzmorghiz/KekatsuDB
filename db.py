import sys
import os
import json
import struct
import shutil
import xml.etree.ElementTree as ET
import re
import requests
import urllib.parse
from PIL import Image
from io import BytesIO
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QLineEdit, QPushButton, QTableWidget, QTableWidgetItem, 
    QLabel, QHeaderView, QMessageBox, QTabWidget, QFileDialog, QGroupBox,
    QMenu, QProgressBar, QTextEdit, QFrame, QAbstractItemView
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QSize
from PyQt6.QtGui import QIcon, QPixmap, QAction, QColor, QFont

def sanitize_filename(filename):
    """Pulisce il nome del file per compatibilità web e filesystem."""
    name, ext = os.path.splitext(filename)
    name = name.replace(" ", "_")
    name = re.sub(r'[^a-zA-Z0-9_]', '', name)
    return f"{name.lower()}{ext.lower()}"

class BatchProcessor(QThread):
    progress_signal = pyqtSignal(int, str)
    row_updated_signal = pyqtSignal(dict)
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal()

    def __init__(self, tasks, boxarts_dir, no_intro_db, maker_codes):
        super().__init__()
        self.tasks = tasks
        self.boxarts_dir = boxarts_dir
        self.no_intro_db = no_intro_db
        self.maker_codes = maker_codes
        self.regions_tdb = ["EN", "US", "JA", "FR", "DE", "IT", "ES"]

    def run(self):
        total = len(self.tasks)
        for i, task in enumerate(self.tasks):
            path = task['path']
            plat = task['plat']
            mode = task.get('mode', 'full')

            filename = os.path.basename(path)
            self.progress_signal.emit(int((i / total) * 100), f"Analisi: {filename}")

            h_title, code, region, author, ver = self.extract_metadata(path)
            
            plat_db = self.no_intro_db.get(plat, {})
            is_homebrew = (code == "" or code not in plat_db)
            
            if is_homebrew:
                real_title = h_title.replace('\n', ' ').strip()
                if not real_title or real_title == "Unknown": real_title = filename
                box_path = None
                final_path = path
            else:
                real_title = plat_db.get(code, h_title).replace('\n', ' ').strip()
                clean_title = sanitize_filename(real_title)
                ext = os.path.splitext(path)[1]
                new_filename = f"{clean_title}{ext}"
                dir_path = os.path.dirname(path)
                final_path = os.path.join(dir_path, new_filename)
                
                if path != final_path:
                    try:
                        if not os.path.exists(final_path):
                            os.rename(path, final_path)
                            filename = new_filename
                    except Exception as e:
                        self.log_signal.emit(f"[ERR] Rinomina fallita: {e}")

                if mode == 'full' and code:
                    box_path = self.process_boxart(code, real_title, plat)
                else:
                    local_box = os.path.join(self.boxarts_dir, plat, f"{code}.png")
                    box_path = local_box if os.path.exists(local_box) else None
            
            self.row_updated_signal.emit({
                "title": real_title, "region": region, "version": ver,
                "author": author, "code": code, "size": os.path.getsize(final_path),
                "boxart_path": box_path, "is_homebrew": is_homebrew,
                "filename": filename, "plat": plat
            })

        self.progress_signal.emit(100, "Completato")
        self.finished_signal.emit()

    def extract_metadata(self, filepath):
        ext = os.path.splitext(filepath)[1].lower()
        title, game_code, region, author, version = "Unknown", "", "ANY", "Sconosciuto", "1.0"
        try:
            with open(filepath, 'rb') as f:
                if ext in [".nds", ".dsi"]:
                    f.seek(0x0C); game_code = f.read(4).decode('ascii', errors='ignore').strip()
                    f.seek(0x10); m_code = f.read(2).decode('ascii', errors='ignore').strip()
                    author = self.maker_codes.get(m_code, f"Cod {m_code}")
                    f.seek(0x1C); version = f"1.{int.from_bytes(f.read(1), 'little')}"
                    region = self.get_region_from_code(game_code)
                    f.seek(0x68); banner_offset = int.from_bytes(f.read(4), 'little')
                    if banner_offset > 0:
                        f.seek(banner_offset + 0x240) 
                        title = f.read(128).decode('utf-16-le', errors='ignore').split('\x00')[0].strip()
                elif ext == ".gba":
                    f.seek(0xA0); title = f.read(12).decode('latin-1', errors='ignore').split('\x00')[0].strip()
                    f.seek(0xAC); game_code = f.read(4).decode('ascii', errors='ignore').strip()
                    f.seek(0xB0); m_code = f.read(2).decode('ascii', errors='ignore').strip()
                    author = self.maker_codes.get(m_code, f"Cod {m_code}")
                    region = self.get_region_from_code(game_code)
        except: pass
        return title, game_code, region, author, version

    def get_region_from_code(self, code):
        if len(code) < 4: return "ANY"
        mapping = {'J':'NTSC-J', 'E':'NTSC-U', 'P':'PAL', 'D':'GER', 'F':'FRA', 'I':'ITA', 'S':'ESP', 'K':'KOR', 'X':'PAL'}
        return mapping.get(code[3].upper(), "ANY")

    def process_boxart(self, code, name, plat):
        plat_box_dir = os.path.join(self.boxarts_dir, plat)
        os.makedirs(plat_box_dir, exist_ok=True)
        dest = os.path.join(plat_box_dir, f"{code}.png")
        if os.path.exists(dest): return dest
        
        # Prova GameTDB (per DS)
        if plat in ["nds", "dsi"]:
            for reg in self.regions_tdb:
                url = f"https://art.gametdb.com/ds/cover/{reg}/{code}.jpg"
                try:
                    r = requests.get(url, timeout=2)
                    if r.status_code == 200:
                        Image.open(BytesIO(r.content)).save(dest, "PNG")
                        return dest
                except: continue
        return None

class KekatsuManager(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Kekatsu ROM Manager")
        self.resize(1100, 750)

        # Directory Setup
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.roms_dir = os.path.join(self.base_dir, "roms")
        self.boxarts_dir = os.path.join(self.base_dir, "boxarts")
        self.dbnames_dir = os.path.join(self.base_dir, "dbnames")
        self.docs_dir = os.path.join(self.base_dir, "docs") # Nuova cartella
        self.url_file = os.path.join(self.base_dir, "url.txt")
        
        self.maker_codes = {"01": "Nintendo", "08": "Capcom", "33": "Ocean", "34": "Konami", "41": "Ubisoft", "52": "Activision"}
        self.no_intro_db = {"nds": {}, "gba": {}, "dsi": {}}

        self.ensure_dirs()
        self.load_no_intro()
        self.setup_ui()
        self.load_url_config()
        self.scan_local_roms(mode='fast')

    def ensure_dirs(self):
        for d in [self.roms_dir, self.boxarts_dir, self.dbnames_dir, self.docs_dir]:
            os.makedirs(d, exist_ok=True)
        for p in ["nds", "gba", "dsi"]:
            os.makedirs(os.path.join(self.roms_dir, p), exist_ok=True)
            os.makedirs(os.path.join(self.boxarts_dir, p), exist_ok=True)

    def setup_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)

        # Header: URL Configuration
        url_group = QGroupBox("Impostazioni Server Remoto")
        url_layout = QVBoxLayout(url_group)
        
        grid_urls = QHBoxLayout()
        self.url_roms_input = QLineEdit("http://server.com/roms/")
        self.url_box_input = QLineEdit("http://server.com/boxarts/")
        
        grid_urls.addWidget(QLabel("URL ROMs:")); grid_urls.addWidget(self.url_roms_input)
        grid_urls.addWidget(QLabel("URL Boxarts:")); grid_urls.addWidget(self.url_box_input)
        
        btn_save = QPushButton("Salva URL")
        btn_save.clicked.connect(self.save_url_config)
        grid_urls.addWidget(btn_save)
        url_layout.addLayout(grid_urls)
        main_layout.addWidget(url_group)

        # Tabs
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        # Tab Ufficiali
        self.tab_off = QWidget(); off_layout = QVBoxLayout(self.tab_off)
        self.table_off = self.create_table(["Box", "Titolo", "Sist.", "Regione", "Ver", "Editore", "File", "Dimensione"])
        self.table_off.setColumnWidth(0, 60)
        off_layout.addWidget(self.table_off)
        self.tabs.addTab(self.tab_off, "Ufficiali")

        # Tab Homebrew
        self.tab_hb = QWidget(); hb_layout = QVBoxLayout(self.tab_hb)
        self.table_hb = self.create_table(["Titolo", "Sist.", "Regione", "Ver", "File", "Dimensione"])
        hb_layout.addWidget(self.table_hb)
        self.tabs.addTab(self.tab_hb, "Homebrew")

        # Tab Console/Log
        self.log_text = QTextEdit(); self.log_text.setReadOnly(True); self.log_text.setStyleSheet("background-color: #1e1e1e; color: #d4d4d4;")
        self.tabs.addTab(self.log_text, "Log Operazioni")

        # Footer
        footer = QHBoxLayout()
        self.status_label = QLabel("Pronto")
        self.pbar = QProgressBar(); self.pbar.setFixedWidth(200); self.pbar.setVisible(False)
        
        self.btn_scan = QPushButton("🔍 Avvia Scansione"); self.btn_scan.clicked.connect(lambda: self.scan_local_roms(mode='full'))
        self.btn_export = QPushButton("💾 Genera DATABASE.TXT"); self.btn_export.clicked.connect(self.export_db)
        self.btn_export.setStyleSheet("font-weight: bold; padding: 5px 15px;")
        
        footer.addWidget(self.status_label)
        footer.addStretch()
        footer.addWidget(self.pbar)
        footer.addWidget(self.btn_scan)
        footer.addWidget(self.btn_export)
        main_layout.addLayout(footer)

    def create_table(self, headers):
        table = QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setAlternatingRowColors(True)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        table.horizontalHeader().setStretchLastSection(True)
        table.setIconSize(QSize(40, 40))
        table.verticalHeader().setDefaultSectionSize(45)
        return table

    def load_no_intro(self):
        mapping = {"nds.dat": "nds", "gba.dat": "gba", "dsi.dat": "dsi"}
        for f, plat in mapping.items():
            path = os.path.join(self.dbnames_dir, f)
            if not os.path.exists(path): continue
            try:
                tree = ET.parse(path)
                for game in tree.getroot().findall('game'):
                    rom = game.find('rom')
                    if rom is not None and rom.get('serial'):
                        serial = rom.get('serial').replace("-", "").strip().upper()
                        self.no_intro_db[plat][serial] = game.get('name')
            except: pass

    def scan_local_roms(self, mode='full'):
        self.table_off.setRowCount(0); self.table_hb.setRowCount(0)
        tasks = []
        for p in ["nds", "gba", "dsi"]:
            p_path = os.path.join(self.roms_dir, p)
            if not os.path.exists(p_path): continue
            for f in os.listdir(p_path):
                if f.lower().endswith(('.nds', '.gba', '.dsi')):
                    tasks.append({"path": os.path.join(p_path, f), "plat": p, "mode": mode})
        
        if tasks:
            self.pbar.setVisible(True)
            self.btn_scan.setEnabled(False)
            self.processor = BatchProcessor(tasks, self.boxarts_dir, self.no_intro_db, self.maker_codes)
            self.processor.row_updated_signal.connect(self.add_row)
            self.processor.progress_signal.connect(self.update_progress)
            self.processor.finished_signal.connect(self.scan_finished)
            self.processor.start()

    def update_progress(self, val, msg):
        self.pbar.setValue(val)
        self.status_label.setText(msg)

    def scan_finished(self):
        self.pbar.setVisible(False)
        self.btn_scan.setEnabled(True)
        self.status_label.setText("Scansione completata")
        QMessageBox.information(self, "Fine", "Scansione e analisi completate correttamente.")

    def add_row(self, d):
        if not d['is_homebrew']:
            t = self.table_off; r = t.rowCount(); t.insertRow(r)
            if d['boxart_path']: t.setItem(r, 0, QTableWidgetItem()); t.item(r, 0).setIcon(QIcon(d['boxart_path']))
            t.setItem(r, 1, QTableWidgetItem(d['title']))
            t.setItem(r, 2, QTableWidgetItem(d['plat'].upper()))
            t.setItem(r, 3, QTableWidgetItem(d['region']))
            t.setItem(r, 4, QTableWidgetItem(d['version']))
            t.setItem(r, 5, QTableWidgetItem(d['author']))
            t.setItem(r, 6, QTableWidgetItem(d['filename']))
            t.setItem(r, 7, QTableWidgetItem(f"{d['size']/1024/1024:.2f} MB"))
        else:
            t = self.table_hb; r = t.rowCount(); t.insertRow(r)
            t.setItem(r, 0, QTableWidgetItem(d['title']))
            t.setItem(r, 1, QTableWidgetItem(d['plat'].upper()))
            t.setItem(r, 2, QTableWidgetItem(d['region']))
            t.setItem(r, 3, QTableWidgetItem(d['version']))
            t.setItem(r, 4, QTableWidgetItem(d['filename']))
            t.setItem(r, 5, QTableWidgetItem(f"{d['size']/1024/1024:.2f} MB"))

    def export_db(self):
        # Percorso modificato come richiesto
        dest = os.path.join(self.docs_dir, "database.txt")
        try:
            lines = ["1", "\t"]
            base_rom = self.url_roms_input.text().strip().rstrip('/')
            base_box = self.url_box_input.text().strip().rstrip('/')

            # Export Ufficiali
            for r in range(self.table_off.rowCount()):
                plat = self.table_off.item(r, 2).text().lower()
                fname = self.table_off.item(r, 6).text()
                # Costruiamo la riga per il DB (9 colonne tipiche)
                row = [
                    self.table_off.item(r, 1).text(), # Titolo
                    plat,                             # Sistema
                    self.table_off.item(r, 3).text(), # Regione
                    self.table_off.item(r, 4).text(), # Ver
                    self.table_off.item(r, 5).text(), # Editore
                    f"{base_rom}/{plat}/{fname}",      # URL ROM
                    fname,                            # Nome File
                    self.table_off.item(r, 7).text(), # Peso
                    f"{base_box}/{plat}/{fname.replace('.nds', '.png')}" # URL Box (esempio)
                ]
                lines.append("\t".join(row))

            with open(dest, 'w', encoding='utf-8') as f:
                f.write("\n".join(lines))
            QMessageBox.information(self, "Successo", f"Database esportato in:\n{dest}")
        except Exception as e:
            QMessageBox.critical(self, "Errore", f"Impossibile esportare il database: {e}")

    def load_url_config(self):
        if os.path.exists(self.url_file):
            try:
                with open(self.url_file, 'r') as f:
                    lns = f.read().splitlines()
                    if len(lns) >= 1: self.url_roms_input.setText(lns[0])
                    if len(lns) >= 2: self.url_box_input.setText(lns[1])
            except: pass

    def save_url_config(self):
        with open(self.url_file, 'w') as f:
            f.write(f"{self.url_roms_input.text()}\n{self.url_box_input.text()}")
        QMessageBox.information(self, "OK", "Configurazione salvata.")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion") # Stile pulito e cross-platform
    window = KekatsuManager()
    window.show()
    sys.exit(app.exec())