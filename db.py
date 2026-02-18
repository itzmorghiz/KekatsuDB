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
    QMenu, QProgressBar, QTextEdit
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QSize
from PyQt6.QtGui import QIcon, QPixmap, QAction, QColor, QFont

def sanitize_filename(filename):
    """Rimuove spazi e caratteri speciali dal nome del file per compatibilità web."""
    name, ext = os.path.splitext(filename)
    name = name.replace(" ", "_")
    name = re.sub(r'[^a-zA-Z0-9_]', '', name)
    return f"{name.lower()}{ext.lower()}"

class BatchProcessor(QThread):
    """Thread per l'elaborazione massiva di metadati e download copertine"""
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
            
            # Controllo No-Intro
            plat_db = self.no_intro_db.get(plat, {})
            is_homebrew = (code == "" or code not in plat_db)
            
            if is_homebrew:
                # Pulisce titolo homebrew da eventuali newline rimasti
                real_title = h_title.replace('\n', ' ').replace('\r', '').strip()
                if real_title == "Unknown": real_title = filename
                box_path = None
                final_path = path
                self.log_signal.emit(f"[INFO] {filename} identificato come Homebrew.")
            else:
                real_title = plat_db.get(code, h_title).replace('\n', ' ').replace('\r', '').strip()
                self.log_signal.emit(f"[OK] {filename} trovato nel DB: {real_title}")
                
                # Rinomina per compatibilità web
                clean_title = sanitize_filename(real_title)
                ext = os.path.splitext(path)[1]
                new_filename = f"{clean_title}{ext}"
                dir_path = os.path.dirname(path)
                final_path = os.path.join(dir_path, new_filename)
                
                if path != final_path:
                    try:
                        counter = 1
                        temp_final = final_path
                        while os.path.exists(temp_final) and temp_final != path:
                            temp_final = os.path.join(dir_path, f"{clean_title}_{counter}{ext}")
                            counter += 1
                        os.rename(path, temp_final)
                        final_path = temp_final
                        filename = os.path.basename(final_path)
                    except Exception as e:
                        self.log_signal.emit(f"[ERRORE] Rinomina fallita: {e}")
                        final_path = path

                # Boxart
                if mode != 'fast' and code:
                    box_path = self.process_boxart(code, real_title, plat)
                else:
                    local_box = os.path.join(self.boxarts_dir, f"{code}.png")
                    box_path = local_box if os.path.exists(local_box) else None
            
            update_data = {
                "title": real_title,
                "region": region,
                "version": ver,
                "author": author,
                "code": code,
                "size": os.path.getsize(final_path),
                "boxart_path": box_path,
                "is_homebrew": is_homebrew,
                "filename": filename,
                "plat": plat
            }
            self.row_updated_signal.emit(update_data)

        self.progress_signal.emit(100, "Operazione completata")
        self.finished_signal.emit()

    def extract_metadata(self, filepath):
        ext = os.path.splitext(filepath)[1].lower()
        title, game_code, region, author, version = "Unknown", "", "ANY", "Unknown", "1.0"
        try:
            with open(filepath, 'rb') as f:
                if ext in [".nds", ".dsi"]:
                    f.seek(0)
                    h_title = f.read(12).decode('latin-1', errors='ignore').split('\x00')[0].strip()
                    f.seek(0x0C); game_code = f.read(4).decode('ascii', errors='ignore').strip()
                    f.seek(0x10); m_code = f.read(2).decode('ascii', errors='ignore').strip()
                    author = self.maker_codes.get(m_code, f"Codice {m_code}")
                    f.seek(0x1C); version = f"1.{int.from_bytes(f.read(1), 'little')}"
                    region = self.get_region_from_code(game_code)

                    f.seek(0x68)
                    banner_offset = int.from_bytes(f.read(4), 'little')
                    if banner_offset > 0:
                        f.seek(banner_offset + 0x240)
                        b_title_data = f.read(128)
                        try:
                            # Decodifica e rimuove newline o null byte residui
                            decoded = b_title_data.decode('utf-16-le').split('\x00')[0]
                            decoded = decoded.replace('\n', ' ').replace('\r', '').strip()
                            if decoded: title = decoded
                            else: title = h_title
                        except: title = h_title
                    else: title = h_title

                elif ext == ".gba":
                    f.seek(0xA0); title = f.read(12).decode('latin-1', errors='ignore').split('\x00')[0].strip()
                    f.seek(0xAC); game_code = f.read(4).decode('ascii', errors='ignore').strip()
                    f.seek(0xB0); m_code = f.read(2).decode('ascii', errors='ignore').strip()
                    author = self.maker_codes.get(m_code, f"Codice {m_code}")
                    f.seek(0xBC); version = f"1.{int.from_bytes(f.read(1), 'little')}"
                    region = self.get_region_from_code(game_code)
        except Exception as e:
            self.log_signal.emit(f"[ERRORE] Header {filepath}: {e}")
            
        return title, game_code, region, author, version

    def get_region_from_code(self, code):
        if len(code) < 4: return "ANY"
        c = code[3].upper()
        mapping = {'J':'NTSC-J', 'E':'NTSC-U', 'P':'PAL', 'D':'GER', 'F':'FRA', 'I':'ITA', 'S':'ESP', 'K':'KOR', 'X':'PAL'}
        return mapping.get(c, "ANY")

    def process_boxart(self, code, name, plat):
        dest = os.path.join(self.boxarts_dir, f"{code}.png")
        if os.path.exists(dest): return dest
        success = False
        if plat in ["nds", "dsi"]:
            for reg in self.regions_tdb:
                url = f"https://art.gametdb.com/ds/cover/{reg}/{code}.jpg"
                try:
                    r = requests.get(url, timeout=3)
                    if r.status_code == 200:
                        Image.open(BytesIO(r.content)).save(dest, "PNG"); success = True; break
                except: continue
        if not success:
            sys_name = "Nintendo%20-%20Nintendo%20DS" if plat != "gba" else "Nintendo%20-%20Game%20Boy%20Advance"
            url = f"https://thumbnails.libretro.com/{sys_name}/Named_Boxarts/{urllib.parse.quote(name)}.png"
            try:
                r = requests.get(url, timeout=3)
                if r.status_code == 200:
                    Image.open(BytesIO(r.content)).save(dest, "PNG"); success = True
            except: pass
        return dest if success else None

class KekatsuManager(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Kekatsu Manager")
        self.resize(1200, 850)

        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.roms_dir = os.path.join(self.base_dir, "roms")
        self.boxarts_dir = os.path.join(self.base_dir, "boxarts")
        self.dbnames_dir = os.path.join(self.base_dir, "dbnames")
        self.url_file = os.path.join(self.base_dir, "url.txt")
        
        # Database Maker Codes (da headers.pdf)
        self.maker_codes = {
            "01": "Nintendo", "08": "Capcom", "09": "Hot-B", "0A": "Jaleco", "0B": "Coconuts Japan",
            "13": "Electronic Arts", "18": "Hudson Soft", "19": "B-AI", "1A": "Yanoman", "1F": "Virgin Games",
            "24": "PCM Complete", "25": "San-X", "28": "Kotobuki Systems", "32": "Bandai", "33": "Ocean/Acclaim",
            "34": "Konami", "41": "Ubisoft", "42": "Atlus", "49": "Irem", "4A": "Virgin Games", "51": "Acclaim",
            "52": "Activision", "54": "Konami", "56": "LJN", "5D": "THQ", "60": "Titus", "69": "Electronic Arts",
            "70": "Infogrames", "71": "Interplay", "78": "THQ", "79": "Accolade", "7F": "Kemco", "8B": "Bullet-Proof",
            "8C": "Vic Tokai", "91": "Chunsoft", "9B": "Tecmo", "9D": "Banpresto", "A2": "Bandai", "A4": "Konami",
            "AF": "Namco", "B0": "Acclaim", "B4": "Enix", "BB": "SunSoft", "C0": "Taito", "C3": "Square", "EB": "Atlus"
        }
        
        self.no_intro_db = {"nds": {}, "gba": {}, "dsi": {}}
        self.base_url = "https://example.com/"
        self.DELIMITER = "," # Default come da README

        self.ensure_dirs()
        self.load_no_intro()
        self.setup_ui()
        self.load_base_url()
        self.scan_local_roms(mode='fast')

    def ensure_dirs(self):
        for d in [self.roms_dir, self.boxarts_dir, self.dbnames_dir]:
            os.makedirs(d, exist_ok=True)
        for p in ["nds", "gba", "dsi"]:
            os.makedirs(os.path.join(self.roms_dir, p), exist_ok=True)

    def load_no_intro(self):
        for f, k in [("nds.dat", "nds"), ("gba.dat", "gba"), ("dsi.dat", "dsi")]:
            path = os.path.join(self.dbnames_dir, f)
            if not os.path.exists(path): continue
            try:
                root = ET.parse(path).getroot()
                for g in root.findall('game'):
                    name = g.get('name')
                    rom = g.find('rom')
                    if rom is not None and rom.get('serial'):
                        s = rom.get('serial').replace("-", "").strip().upper()
                        self.no_intro_db[k][s] = name
            except: pass

    def setup_ui(self):
        self.central = QWidget(); self.setCentralWidget(self.central)
        layout = QVBoxLayout(self.central)

        url_box = QHBoxLayout()
        url_box.addWidget(QLabel("<b>Server URL:</b>"))
        self.url_in = QLineEdit(); url_box.addWidget(self.url_in)
        self.btn_save_url = QPushButton("Salva URL"); self.btn_save_url.clicked.connect(self.save_base_url)
        url_box.addWidget(self.btn_save_url)
        layout.addLayout(url_box)

        self.tabs = QTabWidget(); layout.addWidget(self.tabs)
        
        # Tab Ufficiali
        self.tab_off = QWidget(); off_lay = QVBoxLayout(self.tab_off)
        self.table_off = QTableWidget(0, 10)
        self.table_off.setHorizontalHeaderLabels(["Box", "Titolo", "Sist", "Reg", "Ver", "Autore", "URL", "File", "Size", "BoxURL"])
        self.table_off.verticalHeader().setDefaultSectionSize(50); self.table_off.setIconSize(QSize(40, 40))
        off_lay.addWidget(self.table_off); self.tabs.addTab(self.tab_off, "Ufficiali")

        # Tab Homebrew
        self.tab_hb = QWidget(); hb_lay = QVBoxLayout(self.tab_hb)
        self.table_hb = QTableWidget(0, 7)
        self.table_hb.setHorizontalHeaderLabels(["Titolo", "Sist", "Reg", "Ver", "URL", "File", "Size"])
        hb_lay.addWidget(self.table_hb); self.tabs.addTab(self.tab_hb, "Homebrew")

        # Log
        self.log_box = QTextEdit(); self.log_box.setReadOnly(True); self.tabs.addTab(self.log_box, "Log")

        btn_bar = QHBoxLayout()
        self.btn_refresh = QPushButton("Aggiorna Metadati"); self.btn_refresh.clicked.connect(lambda: self.scan_local_roms(mode='full'))
        self.btn_export = QPushButton("Esporta DATABASE.TXT"); self.btn_export.clicked.connect(self.export_db)
        btn_bar.addWidget(self.btn_refresh); btn_bar.addWidget(self.btn_export)
        layout.addLayout(btn_bar)

        self.pbar = QProgressBar(); self.pbar.setVisible(False); layout.addWidget(self.pbar)

    def load_base_url(self):
        if os.path.exists(self.url_file):
            with open(self.url_file, 'r') as f: self.base_url = f.read().strip()
        if not self.base_url.endswith('/'): self.base_url += '/'
        self.url_in.setText(self.base_url)

    def save_base_url(self):
        self.base_url = self.url_in.text().strip()
        if not self.base_url.endswith('/'): self.base_url += '/'
        with open(self.url_file, 'w') as f: f.write(self.base_url)
        QMessageBox.information(self, "OK", "URL salvato.")

    def scan_local_roms(self, mode='fast'):
        self.table_off.setRowCount(0); self.table_hb.setRowCount(0)
        tasks = []
        for p in ["nds", "gba", "dsi"]:
            path = os.path.join(self.roms_dir, p)
            for f in os.listdir(path):
                if f.lower().endswith(('.nds', '.gba', '.dsi', '.zip')):
                    tasks.append({"path": os.path.join(path, f), "plat": p, "mode": mode})
        
        if tasks:
            self.pbar.setVisible(True)
            self.proc = BatchProcessor(tasks, self.boxarts_dir, self.no_intro_db, self.maker_codes)
            self.proc.row_updated_signal.connect(self.add_row)
            self.proc.progress_signal.connect(self.pbar.setValue)
            self.proc.log_signal.connect(self.log_box.append)
            self.proc.finished_signal.connect(lambda: self.pbar.setVisible(False))
            self.proc.start()

    def add_row(self, d):
        base = self.url_in.text()
        if not base.endswith('/'): base += '/'
        if not d['is_homebrew']:
            t = self.table_off; r = t.rowCount(); t.insertRow(r)
            if d['boxart_path']: t.setItem(r, 0, QTableWidgetItem()); t.item(r, 0).setIcon(QIcon(d['boxart_path']))
            t.setItem(r, 1, QTableWidgetItem(d['title']))
            t.setItem(r, 2, QTableWidgetItem(d['plat']))
            t.setItem(r, 3, QTableWidgetItem(d['region']))
            t.setItem(r, 4, QTableWidgetItem(d['version']))
            t.setItem(r, 5, QTableWidgetItem(d['author']))
            t.setItem(r, 6, QTableWidgetItem(f"{base}roms/{d['plat']}/{d['filename']}"))
            t.setItem(r, 7, QTableWidgetItem(d['filename']))
            t.setItem(r, 8, QTableWidgetItem(str(d['size'])))
            t.setItem(r, 9, QTableWidgetItem(f"{base}boxarts/{d['code']}.png" if d['code'] else ""))
        else:
            t = self.table_hb; r = t.rowCount(); t.insertRow(r)
            t.setItem(r, 0, QTableWidgetItem(d['title']))
            t.setItem(r, 1, QTableWidgetItem(d['plat']))
            t.setItem(r, 2, QTableWidgetItem(d['region']))
            t.setItem(r, 3, QTableWidgetItem(d['version']))
            t.setItem(r, 4, QTableWidgetItem(f"{base}roms/{d['plat']}/{d['filename']}"))
            t.setItem(r, 5, QTableWidgetItem(d['filename']))
            t.setItem(r, 6, QTableWidgetItem(str(d['size'])))

    def export_db(self):
        dest = os.path.join(self.base_dir, "database.txt")
        # Riga 1: Version, Riga 2: Delimiter (come richiesto dal README)
        lines = ["1", self.DELIMITER]
        
        # Helper per pulire ogni cella
        def clean(val):
            return str(val).replace('\n', ' ').replace('\r', '').strip()

        # Ufficiali
        for r in range(self.table_off.rowCount()):
            row = [clean(self.table_off.item(r, i).text()) for i in range(1, 10)]
            lines.append(self.DELIMITER.join(row))
        
        # Homebrew
        for r in range(self.table_hb.rowCount()):
            # Titolo, Plat, Reg, Ver, Author, URL, File, Size, BoxURL
            row = [
                clean(self.table_hb.item(r, 0).text()),
                clean(self.table_hb.item(r, 1).text()),
                clean(self.table_hb.item(r, 2).text()),
                clean(self.table_hb.item(r, 3).text()),
                "Homebrew",
                clean(self.table_hb.item(r, 4).text()),
                clean(self.table_hb.item(r, 5).text()),
                clean(self.table_hb.item(r, 6).text()),
                ""
            ]
            lines.append(self.DELIMITER.join(row))

        with open(dest, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines))
        QMessageBox.information(self, "Fatto", f"Esportato in {dest}")

if __name__ == "__main__":
    app = QApplication(sys.argv); w = KekatsuManager(); w.show(); sys.exit(app.exec())