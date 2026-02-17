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

class BatchProcessor(QThread):
    """Thread per l'elaborazione massiva di metadati e download copertine"""
    progress_signal = pyqtSignal(int, str)  # percentuale, messaggio di stato
    row_updated_signal = pyqtSignal(int, dict) # indice riga, dati aggiornati
    finished_signal = pyqtSignal()

    def __init__(self, tasks, boxarts_dir, no_intro_db, maker_codes):
        super().__init__()
        self.tasks = tasks # Lista di dict {row, path, plat, mode: 'full'|'meta'|'box'}
        self.boxarts_dir = boxarts_dir
        self.no_intro_db = no_intro_db
        self.maker_codes = maker_codes
        self.regions_tdb = ["EN", "US", "JA", "FR", "DE", "IT", "ES"]

    def run(self):
        total = len(self.tasks)
        for i, task in enumerate(self.tasks):
            row = task['row']
            path = task['path']
            plat = task['plat']
            mode = task.get('mode', 'full')

            filename = os.path.basename(path)
            self.progress_signal.emit(int((i / total) * 100), f"Elaborazione: {filename}")

            # 1. Estrazione Metadati
            h_title, code, region, author, ver = self.extract_metadata(path)
            real_title = self.no_intro_db.get(code, h_title)
            
            update_data = {
                "title": real_title,
                "region": region,
                "version": ver,
                "author": author,
                "code": code,
                "size": os.path.getsize(path),
                "boxart_path": None
            }

            # 2. Download Boxart (se richiesto o se manca)
            if mode in ['full', 'box'] and code:
                box_path = self.process_boxart(code, real_title, plat)
                update_data["boxart_path"] = box_path

            self.row_updated_signal.emit(row, update_data)

        self.progress_signal.emit(100, "Completato")
        self.finished_signal.emit()

    def extract_metadata(self, filepath):
        ext = os.path.splitext(filepath)[1].lower()
        title, game_code, region, author, version = "Unknown", "", "ANY", "Unknown", "1.0"
        try:
            with open(filepath, 'rb') as f:
                if ext == ".nds":
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
        
        # Saltiamo se esiste già
        if os.path.exists(dest_std): return dest_std

        success = False
        # Logica Fallback: GameTDB -> Libretro
        if plat in ["nds", "ds"]:
            success = self.fetch_gametdb(code, dest_std, dest_hq)
            if not success:
                success = self.fetch_libretro("Nintendo%20-%20Nintendo%20DS", name, dest_std, dest_hq)
        elif plat == "gba":
            success = self.fetch_libretro("Nintendo%20-%20Game%20Boy%20Advance", name, dest_std, dest_hq)
        elif plat == "dsi":
            success = self.fetch_libretro("Nintendo%20-%20Nintendo%20DSi", name, dest_std, dest_hq)

        return dest_std if success else None

    def fetch_gametdb(self, code, dest, dest_hq):
        for reg in self.regions_tdb:
            url = f"https://art.gametdb.com/ds/cover/{reg}/{code}.jpg"
            try:
                r = requests.get(url, timeout=3)
                if r.status_code == 200:
                    img = Image.open(BytesIO(r.content))
                    img.save(dest, "PNG")
                    # Prova anche HQ
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
                img.save(dest_hq, "PNG") # Libretro è HQ di base
                # Resize per versione standard (altezza 256px)
                w, h = img.size
                img.resize((int(256/h*w), 256), Image.Resampling.LANCZOS).save(dest, "PNG")
                return True
        except: pass
        return False

class KekatsuManager(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Kekatsu Manager - Editor Database")
        self.resize(1300, 850)

        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.roms_dir = os.path.join(self.base_dir, "roms")
        self.boxarts_dir = os.path.join(self.base_dir, "boxarts")
        self.dbnames_dir = os.path.join(self.base_dir, "dbnames")
        self.url_file_path = os.path.join(self.base_dir, "url.txt")
        
        # Database codici produttore
        self.maker_codes = {
            "01": "Nintendo", "08": "Capcom", "13": "Electronic Arts", "18": "Hudson Soft",
            "41": "Ubisoft", "6E": "Sega", "78": "THQ", "82": "Namco", "A4": "Konami"
        }
        self.no_intro_db = {}
        self.base_url = "https://itzmorghiz.github.io/KekatsuDB/"
        
        # Delimitatore fisso come richiesto (doppio TAB)
        self.DELIMITER = "\t\t"

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

        # Configurazione Base URL
        top_bar = QHBoxLayout()
        top_bar.addWidget(QLabel("Base URL:"))
        self.url_input = QLineEdit()
        top_bar.addWidget(self.url_input)
        self.btn_save_url = QPushButton("Salva URL")
        self.btn_save_url.clicked.connect(self.save_base_url)
        top_bar.addWidget(self.btn_save_url)
        layout.addLayout(top_bar)

        # Tabs principale
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        # Tab Editor ROM
        self.tab_editor = QWidget()
        ed_layout = QVBoxLayout(self.tab_editor)
        
        btns = QHBoxLayout()
        self.btn_import = QPushButton("➕ Importa ROM")
        self.btn_import.clicked.connect(self.import_rom)
        self.btn_refresh = QPushButton("✨ Aggiorna Tutto (Metadati + Boxart)")
        self.btn_refresh.clicked.connect(self.full_refresh)
        self.btn_export = QPushButton("🚀 Esporta Database.txt")
        self.btn_export.setStyleSheet("background-color: #2196F3; color: white; font-weight: bold;")
        self.btn_export.clicked.connect(self.export_db)
        
        btns.addWidget(self.btn_import)
        btns.addWidget(self.btn_refresh)
        btns.addStretch()
        btns.addWidget(self.btn_export)
        ed_layout.addLayout(btns)

        self.table = QTableWidget(0, 10)
        self.table.setHorizontalHeaderLabels([
            "Icona", "Titolo", "Sistema", "Regione", "Ver", 
            "Autore", "URL ROM", "File", "Size", "URL Boxart"
        ])
        self.table.verticalHeader().setDefaultSectionSize(60)
        self.table.setIconSize(QSize(48, 48))
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.context_menu)
        ed_layout.addWidget(self.table)
        
        self.tabs.addTab(self.tab_editor, "Editor ROM")

        # Barra di stato e Progresso
        self.status_bar = QHBoxLayout()
        self.pbar = QProgressBar()
        self.pbar.setVisible(False)
        self.pbar.setFixedWidth(300)
        self.lbl_status = QLabel("Pronto")
        self.status_bar.addWidget(self.lbl_status)
        self.status_bar.addStretch()
        self.status_bar.addWidget(self.pbar)
        layout.addLayout(self.status_bar)

    def context_menu(self, pos):
        row = self.table.currentRow()
        if row < 0: return
        menu = QMenu()
        a1 = menu.addAction("🔄 Aggiorna Metadati")
        a2 = menu.addAction("🖼️ Riscarica Boxart")
        a3 = menu.addAction("🗑️ Elimina")
        action = menu.exec(self.table.viewport().mapToGlobal(pos))
        if action == a1: self.start_batch([row], mode='meta')
        elif action == a2: self.start_batch([row], mode='box')
        elif action == a3: self.delete_rom(row)

    def scan_local_roms(self):
        self.table.setRowCount(0)
        if not os.path.exists(self.roms_dir): return
        
        tasks = []
        for plat in ["nds", "gba", "dsi"]:
            p_path = os.path.join(self.roms_dir, plat)
            if not os.path.exists(p_path): continue
            for f in os.listdir(p_path):
                if f.lower().endswith(('.nds', '.gba')):
                    row = self.table.rowCount()
                    self.table.insertRow(row)
                    # Inizializzazione celle
                    for i in range(10): self.table.setItem(row, i, QTableWidgetItem(""))
                    self.table.item(row, 2).setText(plat)
                    self.table.item(row, 7).setText(f)
                    
                    tasks.append({
                        "row": row, 
                        "path": os.path.join(p_path, f), 
                        "plat": plat, 
                        "mode": "full"
                    })
        
        if tasks: self.start_batch_from_tasks(tasks)

    def start_batch_from_tasks(self, tasks):
        self.toggle_ui(False)
        self.pbar.setVisible(True)
        self.processor = BatchProcessor(tasks, self.boxarts_dir, self.no_intro_db, self.maker_codes)
        self.processor.progress_signal.connect(self.update_progress)
        self.processor.row_updated_signal.connect(self.update_row_ui)
        self.processor.finished_signal.connect(self.batch_done)
        self.processor.start()

    def start_batch(self, rows, mode='full'):
        tasks = []
        for r in rows:
            plat = self.table.item(r, 2).text()
            file = self.table.item(r, 7).text()
            tasks.append({
                "row": r, 
                "path": os.path.join(self.roms_dir, plat, file), 
                "plat": plat, 
                "mode": mode
            })
        self.start_batch_from_tasks(tasks)

    def update_progress(self, val, msg):
        self.pbar.setValue(val)
        self.lbl_status.setText(msg)

    def update_row_ui(self, row, data):
        self.table.item(row, 1).setText(data['title'])
        self.table.item(row, 3).setText(data['region'])
        self.table.item(row, 4).setText(data['version'])
        self.table.item(row, 5).setText(data['author'])
        self.table.item(row, 8).setText(str(data['size']))
        
        plat = self.table.item(row, 2).text()
        file = self.table.item(row, 7).text()
        self.table.item(row, 6).setText(f"{self.base_url}roms/{plat}/{file}")

        if data['boxart_path'] and os.path.exists(data['boxart_path']):
            pix = QPixmap(data['boxart_path']).scaled(48, 48, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
            self.table.item(row, 0).setIcon(QIcon(pix))
            self.table.item(row, 9).setText(f"{self.base_url}boxarts/{data['code']}.png")

    def batch_done(self):
        self.toggle_ui(True)
        self.pbar.setVisible(False)
        self.lbl_status.setText("Pronto")

    def toggle_ui(self, enabled):
        self.btn_import.setEnabled(enabled)
        self.btn_refresh.setEnabled(enabled)
        self.btn_export.setEnabled(enabled)

    def import_rom(self):
        f, _ = QFileDialog.getOpenFileName(self, "Seleziona ROM", "", "ROM (*.nds *.gba)")
        if f:
            ext = os.path.splitext(f)[1].lower()
            plat = "gba" if ext == ".gba" else "nds"
            if plat == "nds":
                if QMessageBox.question(self, "Sistema", "È un titolo DSiWare?", QMessageBox.StandardButton.Yes|QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes:
                    plat = "dsi"
            dest = os.path.join(self.roms_dir, plat, os.path.basename(f))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.copy2(f, dest)
            self.scan_local_roms()

    def full_refresh(self):
        rows = list(range(self.table.rowCount()))
        self.start_batch(rows)

    def delete_rom(self, row):
        plat = self.table.item(row, 2).text()
        file = self.table.item(row, 7).text()
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
        """Esporta il database usando il doppio TAB come delimitatore fisso"""
        dest = os.path.join(self.base_dir, "database.txt")
        try:
            with open(dest, 'w', encoding='utf-8') as f:
                # Intestazione: versione del DB e delimitatore utilizzato
                # (Nota: scriviamo il delimitatore letteralmente nel file se il parser Kekatsu lo richiede)
                f.write("1\n" + self.DELIMITER + "\n")
                
                for r in range(self.table.rowCount()):
                    # Campi: Titolo, Sistema, Regione, Ver, Autore, URL ROM, File, Size, URL Boxart
                    row_data = []
                    for c in range(1, 10):
                        item = self.table.item(r, c)
                        row_data.append(item.text() if item else "")
                    
                    f.write(self.DELIMITER.join(row_data) + "\n")
            
            QMessageBox.information(self, "Esportazione", f"Database esportato con successo in:\n{dest}")
        except Exception as e:
            QMessageBox.critical(self, "Errore Esportazione", str(e))

if __name__ == "__main__":
    app = QApplication(sys.argv)
    ex = KekatsuManager()
    ex.show()
    sys.exit(app.exec())