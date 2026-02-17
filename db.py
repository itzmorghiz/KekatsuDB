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
    QMenu, QProgressBar
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QSize
from PyQt6.QtGui import QIcon, QPixmap, QAction

def sanitize_filename(filename):
    """Rimuove spazi e caratteri speciali dal nome del file per compatibilità web."""
    name, ext = os.path.splitext(filename)
    # Remplaza spazi con underscore
    name = name.replace(" ", "_")
    # Rimuove tutto ciò che non è alfanumerico o underscore
    name = re.sub(r'[^a-zA-Z0-9_]', '', name)
    # Converte in minuscolo (opzionale, ma consigliato per web server)
    return f"{name.lower()}{ext.lower()}"

class BatchProcessor(QThread):
    """Thread per l'elaborazione massiva di metadati e download copertine"""
    progress_signal = pyqtSignal(int, str)
    row_updated_signal = pyqtSignal(dict)
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
            self.progress_signal.emit(int((i / total) * 100), f"Elaborazione: {filename}")

            h_title, code, region, author, ver = self.extract_metadata(path)
            
            is_homebrew = (code == "" or code not in self.no_intro_db)
            
            if is_homebrew:
                real_title = h_title if h_title != "Unknown" else filename
                author = "" 
                box_path = None
            else:
                real_title = self.no_intro_db.get(code, h_title)
                if mode in ['full', 'box'] and code:
                    box_path = self.process_boxart(code, real_title, plat)
                else:
                    box_path = None
            
            update_data = {
                "title": real_title,
                "region": region,
                "version": ver,
                "author": author,
                "code": code,
                "size": os.path.getsize(path),
                "boxart_path": box_path,
                "is_homebrew": is_homebrew,
                "filename": filename,
                "plat": plat
            }

            self.row_updated_signal.emit(update_data)

        self.progress_signal.emit(100, "Completato")
        self.finished_signal.emit()

    def extract_metadata(self, filepath):
        ext = os.path.splitext(filepath)[1].lower()
        title, game_code, region, author, version = "Unknown", "", "ANY", "Unknown", "1.0"
        try:
            with open(filepath, 'rb') as f:
                if ext in [".nds", ".dsi"]:
                    f.seek(0); title = f.read(12).decode('latin-1', errors='ignore').split('\x00')[0].strip()
                    f.seek(0x0C); game_code = f.read(4).decode('ascii', errors='ignore').strip()
                    f.seek(0x10); m_code = f.read(2).decode('ascii', errors='ignore').strip()
                    author = self.maker_codes.get(m_code, f"Codice {m_code}")
                    f.seek(0x1C); version = f"1.{int.from_bytes(f.read(1), 'little')}"
                    region = self.get_region_from_code(game_code)
                elif ext == ".gba":
                    f.seek(0xA0); title = f.read(12).decode('latin-1', errors='ignore').split('\x00')[0].strip()
                    f.seek(0xAC); game_code = f.read(4).decode('ascii', errors='ignore').strip()
                    f.seek(0xB0); m_code = f.read(2).decode('ascii', errors='ignore').strip()
                    author = self.maker_codes.get(m_code, f"Codice {m_code}")
                    f.seek(0xBC); version = f"1.{int.from_bytes(f.read(1), 'little')}"
                    region = self.get_region_from_code(game_code)
        except: pass
        return title, game_code, region, author, version

    def get_region_from_code(self, code):
        mapping = {'J':'NTSC-J', 'E':'NTSC-U', 'P':'PAL', 'D':'GER', 'F':'FRA', 'I':'ITA', 'S':'ESP'}
        return mapping.get(code[3], "ANY") if len(code) >= 4 else "ANY"

    def process_boxart(self, code, name, plat):
        dest_std = os.path.join(self.boxarts_dir, f"{code}.png")
        dest_hq = os.path.join(self.boxarts_dir, "HQ", f"{code}_HQ.png")
        if os.path.exists(dest_std): return dest_std
        success = False
        if plat in ["nds", "ds", "dsi"]:
            success = self.fetch_gametdb(code, dest_std, dest_hq)
            if not success:
                success = self.fetch_libretro("Nintendo%20-%20Nintendo%20DS", name, dest_std, dest_hq)
        elif plat == "gba":
            success = self.fetch_libretro("Nintendo%20-%20Game%20Boy%20Advance", name, dest_std, dest_hq)
        return dest_std if success else None

    def fetch_gametdb(self, code, dest, dest_hq):
        for reg in self.regions_tdb:
            url = f"https://art.gametdb.com/ds/cover/{reg}/{code}.jpg"
            try:
                r = requests.get(url, timeout=3)
                if r.status_code == 200:
                    img = Image.open(BytesIO(r.content))
                    img.save(dest, "PNG")
                    url_hq = f"https://art.gametdb.com/ds/coverHQ/{reg}/{code}.jpg"
                    rhq = requests.get(url_hq, timeout=3)
                    if rhq.status_code == 200:
                        Image.open(BytesIO(rhq.content)).save(dest_hq, "PNG")
                    return True
            except: continue
        return False

    def fetch_libretro(self, system, name, dest, dest_hq):
        encoded = urllib.parse.quote(name)
        url = f"https://thumbnails.libretro.com/{system}/Named_Boxarts/{encoded}.png"
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                img = Image.open(BytesIO(r.content))
                img.save(dest_hq, "PNG")
                w, h = img.size
                img.resize((int(256/h*w), 256), Image.Resampling.LANCZOS).save(dest, "PNG")
                return True
        except: pass
        return False

class KekatsuManager(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Kekatsu Manager - Editor Database")
        self.resize(1350, 900)

        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.roms_dir = os.path.join(self.base_dir, "roms")
        self.boxarts_dir = os.path.join(self.base_dir, "boxarts")
        self.dbnames_dir = os.path.join(self.base_dir, "dbnames")
        self.url_file_path = os.path.join(self.base_dir, "url.txt")
        
        self.maker_codes = {
            "01": "Nintendo", "08": "Capcom", "13": "Electronic Arts", "18": "Hudson Soft",
            "41": "Ubisoft", "6E": "Sega", "78": "THQ", "82": "Namco", "A4": "Konami"
        }
        self.no_intro_db = {}
        self.base_url = "https://itzmorghiz.github.io/KekatsuDB/"
        self.DELIMITER = "\t"

        self.ensure_directories()
        self.load_no_intro_dat()
        self.setup_ui()
        self.load_base_url()
        self.scan_local_roms()

    def ensure_directories(self):
        for d in [self.roms_dir, self.dbnames_dir, self.boxarts_dir]:
            if not os.path.exists(d): os.makedirs(d)
        hq_path = os.path.join(self.boxarts_dir, "HQ")
        if not os.path.exists(hq_path): os.makedirs(hq_path)

    def load_no_intro_dat(self):
        for f in ["nds.dat", "gba.dat", "dsi.dat"]:
            path = os.path.join(self.dbnames_dir, f)
            if not os.path.exists(path): continue
            try:
                root = ET.parse(path).getroot()
                for g in root.findall('game'):
                    name = g.get('name')
                    rom = g.find('rom')
                    if rom is not None and rom.get('serial'):
                        self.no_intro_db[rom.get('serial').strip().upper()] = name
            except: pass

    def setup_ui(self):
        self.central = QWidget()
        self.setCentralWidget(self.central)
        layout = QVBoxLayout(self.central)

        top_bar = QHBoxLayout()
        top_bar.addWidget(QLabel("Base URL:"))
        self.url_input = QLineEdit()
        top_bar.addWidget(self.url_input)
        self.btn_save_url = QPushButton("Salva URL")
        self.btn_save_url.clicked.connect(self.save_base_url)
        top_bar.addWidget(self.btn_save_url)
        layout.addLayout(top_bar)

        btns = QHBoxLayout()
        self.btn_import = QPushButton("➕ Importa ROM")
        self.btn_import.clicked.connect(self.import_rom)
        self.btn_refresh = QPushButton("✨ Aggiorna Tutto (Meta + Boxart)")
        self.btn_refresh.clicked.connect(self.full_refresh)
        self.btn_export = QPushButton("🚀 Esporta Database.txt")
        self.btn_export.setStyleSheet("background-color: #2196F3; color: white; font-weight: bold;")
        self.btn_export.clicked.connect(self.export_db)
        
        btns.addWidget(self.btn_import)
        btns.addWidget(self.btn_refresh)
        btns.addStretch()
        btns.addWidget(self.btn_export)
        layout.addLayout(btns)

        tables_layout = QVBoxLayout()
        
        self.group_official = QGroupBox("ROM Ufficiali (Database No-Intro)")
        off_layout = QVBoxLayout(self.group_official)
        self.table_off = QTableWidget(0, 10)
        self.table_off.setHorizontalHeaderLabels([
            "Icona", "Titolo", "Sistema", "Regione", "Ver", 
            "Autore", "URL ROM", "File", "Size", "URL Boxart"
        ])
        self.setup_table_style(self.table_off)
        off_layout.addWidget(self.table_off)
        tables_layout.addWidget(self.group_official, 3)

        self.group_homebrew = QGroupBox("ROM Homebrew (Personalizzate)")
        hb_layout = QVBoxLayout(self.group_homebrew)
        self.table_hb = QTableWidget(0, 7)
        self.table_hb.setHorizontalHeaderLabels([
            "Titolo", "Sistema", "Regione", "Ver", "URL ROM", "File", "Size"
        ])
        self.setup_table_style(self.table_hb, hide_icons=True)
        hb_layout.addWidget(self.table_hb)
        tables_layout.addWidget(self.group_homebrew, 2)

        layout.addLayout(tables_layout)

        self.status_bar = QHBoxLayout()
        self.pbar = QProgressBar()
        self.pbar.setVisible(False)
        self.pbar.setFixedWidth(300)
        self.lbl_status = QLabel("Pronto")
        self.status_bar.addWidget(self.lbl_status)
        self.status_bar.addStretch()
        self.status_bar.addWidget(self.pbar)
        layout.addLayout(self.status_bar)

    def setup_table_style(self, table, hide_icons=False):
        table.verticalHeader().setDefaultSectionSize(40 if hide_icons else 60)
        if not hide_icons: table.setIconSize(QSize(48, 48))
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        table.customContextMenuRequested.connect(lambda pos, t=table: self.context_menu(pos, t))

    def context_menu(self, pos, table):
        row = table.currentRow()
        if row < 0: return
        menu = QMenu()
        a1 = menu.addAction("🔄 Aggiorna Metadati")
        
        is_official = table == self.table_off
        a2 = None
        if is_official:
            a2 = menu.addAction("🖼️ Riscarica Boxart")
            
        a3 = menu.addAction("🗑️ Elimina")

        action = menu.exec(table.viewport().mapToGlobal(pos))
        if action == a1: self.refresh_rows([row], table, mode='meta')
        elif a2 and action == a2: self.refresh_rows([row], table, mode='box')
        elif action == a3: self.delete_rom(row, table)

    def scan_local_roms(self):
        self.table_off.setRowCount(0)
        self.table_hb.setRowCount(0)
        if not os.path.exists(self.roms_dir): return
        
        tasks = []
        for plat in ["nds", "gba", "dsi"]:
            p_path = os.path.join(self.roms_dir, plat)
            if not os.path.exists(p_path): continue
            for f in os.listdir(p_path):
                if f.lower().endswith(('.nds', '.gba', '.dsi')):
                    old_path = os.path.join(p_path, f)
                    new_name = sanitize_filename(f)
                    new_path = os.path.join(p_path, new_name)
                    
                    # Rinomina se necessario
                    if f != new_name:
                        try:
                            os.rename(old_path, new_path)
                            f = new_name
                        except: pass
                        
                    tasks.append({
                        "path": new_path, 
                        "plat": plat, 
                        "mode": "full"
                    })
        
        if tasks: self.start_batch_from_tasks(tasks)

    def start_batch_from_tasks(self, tasks):
        self.toggle_ui(False)
        self.pbar.setVisible(True)
        self.processor = BatchProcessor(tasks, self.boxarts_dir, self.no_intro_db, self.maker_codes)
        self.processor.progress_signal.connect(self.update_progress)
        self.processor.row_updated_signal.connect(self.add_or_update_row)
        self.processor.finished_signal.connect(self.batch_done)
        self.processor.start()

    def add_or_update_row(self, data):
        if not data['is_homebrew']:
            table = self.table_off
            row = table.rowCount()
            table.insertRow(row)
            for i in range(10): table.setItem(row, i, QTableWidgetItem(""))
            
            table.item(row, 1).setText(data['title'])
            table.item(row, 2).setText(data['plat'])
            table.item(row, 3).setText(data['region'])
            table.item(row, 4).setText(data['version'])
            table.item(row, 5).setText(data['author'])
            table.item(row, 6).setText(f"{self.base_url}roms/{data['plat']}/{data['filename']}")
            table.item(row, 7).setText(data['filename'])
            table.item(row, 8).setText(str(data['size']))
            
            if data['boxart_path'] and os.path.exists(data['boxart_path']):
                pix = QPixmap(data['boxart_path']).scaled(48, 48, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                table.item(row, 0).setIcon(QIcon(pix))
                table.item(row, 9).setText(f"{self.base_url}boxarts/{data['code']}.png")
        else:
            table = self.table_hb
            row = table.rowCount()
            table.insertRow(row)
            for i in range(7): table.setItem(row, i, QTableWidgetItem(""))
            
            table.item(row, 0).setText(data['title'])
            table.item(row, 1).setText(data['plat'])
            table.item(row, 2).setText(data['region'])
            table.item(row, 3).setText(data['version'])
            table.item(row, 4).setText(f"{self.base_url}roms/{data['plat']}/{data['filename']}")
            table.item(row, 5).setText(data['filename'])
            table.item(row, 6).setText(str(data['size']))

    def refresh_rows(self, row_indices, table, mode='full'):
        self.scan_local_roms()

    def update_progress(self, val, msg):
        self.pbar.setValue(val)
        self.lbl_status.setText(msg)

    def batch_done(self):
        self.toggle_ui(True)
        self.pbar.setVisible(False)
        self.lbl_status.setText("Pronto")

    def toggle_ui(self, enabled):
        self.btn_import.setEnabled(enabled)
        self.btn_refresh.setEnabled(enabled)
        self.btn_export.setEnabled(enabled)

    def import_rom(self):
        f, _ = QFileDialog.getOpenFileName(self, "Seleziona ROM", "", "ROM (*.nds *.gba *.dsi)")
        if f:
            ext = os.path.splitext(f)[1].lower()
            plat = "gba" if ext == ".gba" else "nds"
            if ext == ".dsi": plat = "dsi"
            
            # Sanitizzazione immediata all'importazione
            clean_name = sanitize_filename(os.path.basename(f))
            dest = os.path.join(self.roms_dir, plat, clean_name)
            
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.copy2(f, dest)
            self.scan_local_roms()

    def full_refresh(self):
        self.scan_local_roms()

    def delete_rom(self, row, table):
        plat_col = 2 if table == self.table_off else 1
        file_col = 7 if table == self.table_off else 5
        plat = table.item(row, plat_col).text()
        file = table.item(row, file_col).text()
        if QMessageBox.warning(self, "Conferma", f"Eliminare definitivamente {file}?", QMessageBox.StandardButton.Yes|QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes:
            try:
                os.remove(os.path.join(self.roms_dir, plat, file))
                self.scan_local_roms()
            except Exception as e:
                QMessageBox.critical(self, "Errore", f"Impossibile eliminare: {e}")

    def load_base_url(self):
        if os.path.exists(self.url_file_path):
            with open(self.url_file_path, 'r') as f:
                self.base_url = f.read().strip()
                if not self.base_url.endswith('/'): self.base_url += '/'
        self.url_input.setText(self.base_url)

    def save_base_url(self):
        self.base_url = self.url_input.text().strip()
        if not self.base_url.endswith('/'): self.base_url += '/'
        with open(self.url_file_path, 'w') as f: f.write(self.base_url)
        self.scan_local_roms()

    def export_db(self):
        """Esporta il database senza indicare il tipo (Ufficiale/HB) e senza a capo finale"""
        dest = os.path.join(self.base_dir, "database.txt")
        lines = []
        try:
            # Riga 1: Header
            lines.append("1")
            # Riga 2: Delimitatore TAB
            lines.append(self.DELIMITER)
            
            # Esporta dati dalle tabelle Ufficiali
            for r in range(self.table_off.rowCount()):
                row_data = [
                    self.table_off.item(r, 1).text(), # Titolo
                    self.table_off.item(r, 2).text(), # Sistema
                    self.table_off.item(r, 3).text(), # Regione
                    self.table_off.item(r, 4).text(), # Ver
                    self.table_off.item(r, 5).text(), # Autore
                    self.table_off.item(r, 6).text(), # URL ROM
                    self.table_off.item(r, 7).text(), # File
                    self.table_off.item(r, 8).text(), # Size
                    self.table_off.item(r, 9).text()  # URL Boxart
                ]
                lines.append(self.DELIMITER.join(row_data))

            # Esporta dati dalle tabelle Homebrew
            for r in range(self.table_hb.rowCount()):
                row_data = [
                    self.table_hb.item(r, 0).text(),  # Titolo
                    self.table_hb.item(r, 1).text(),  # Sistema
                    self.table_hb.item(r, 2).text(),  # Regione
                    self.table_hb.item(r, 3).text(),  # Ver
                    "Homebrew",                       # Autore
                    self.table_hb.item(r, 4).text(),  # URL ROM
                    self.table_hb.item(r, 5).text(),  # File
                    self.table_hb.item(r, 6).text(),  # Size
                    ""                                # Boxart
                ]
                lines.append(self.DELIMITER.join(row_data))
            
            with open(dest, 'w', encoding='utf-8') as f:
                f.write("\n".join(lines))
            
            QMessageBox.information(self, "Esportazione", f"Database esportato con successo ({len(lines)-2} titoli).")
        except Exception as e:
            QMessageBox.critical(self, "Errore Esportazione", str(e))

if __name__ == "__main__":
    app = QApplication(sys.argv)
    ex = KekatsuManager()
    ex.show()
    sys.exit(app.exec())