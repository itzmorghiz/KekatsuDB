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
    """Pulisce il nome del file per compatibilità web e filesystem."""
    name, ext = os.path.splitext(filename)
    name = name.replace(" ", "_")
    name = re.sub(r'[^a-zA-Z0-9_]', '', name)
    return f"{name.lower()}{ext.lower()}"

class BatchProcessor(QThread):
    """Thread per l'analisi massiva e il recupero dei metadati."""
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
            self.progress_signal.emit(int((i / total) * 100), f"Analisi in corso: {filename}")

            h_title, code, region, author, ver = self.extract_metadata(path)
            
            # Controllo incrociato con No-Intro DB
            plat_db = self.no_intro_db.get(plat, {})
            is_homebrew = (code == "" or code not in plat_db)
            
            if is_homebrew:
                # Rimozione rigorosa di newline per evitare righe rotte nel DB
                real_title = h_title.replace('\n', ' ').replace('\r', '').strip()
                if not real_title or real_title == "Unknown": real_title = filename
                box_path = None
                final_path = path
                self.log_signal.emit(f"[INFO] {filename} identificato come Homebrew.")
            else:
                real_title = plat_db.get(code, h_title).replace('\n', ' ').replace('\r', '').strip()
                self.log_signal.emit(f"[OK] {filename} identificato: {real_title}")
                
                # Rinomina file per coerenza web
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
                        else:
                            self.log_signal.emit(f"[WARN] Nome file esistente, salto rinomina per {filename}")
                    except Exception as e:
                        self.log_signal.emit(f"[ERR] Errore rinomina: {e}")

                # Recupero Boxart
                if mode == 'full' and code:
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

        self.progress_signal.emit(100, "Completato")
        self.finished_signal.emit()

    def extract_metadata(self, filepath):
        ext = os.path.splitext(filepath)[1].lower()
        title, game_code, region, author, version = "Unknown", "", "ANY", "Sconosciuto", "1.0"
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

                    # Titolo dal Banner NDS
                    f.seek(0x68)
                    banner_offset = int.from_bytes(f.read(4), 'little')
                    if banner_offset > 0:
                        f.seek(banner_offset + 0x240) 
                        b_title_data = f.read(128)
                        try:
                            decoded = b_title_data.decode('utf-16-le').split('\x00')[0]
                            # Pulizia caratteri di controllo nel titolo
                            decoded = decoded.replace('\n', ' ').replace('\r', '').strip()
                            title = decoded if decoded else h_title
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
            self.log_signal.emit(f"[ERR] Errore header: {e}")
            
        return title, game_code, region, author, version

    def get_region_from_code(self, code):
        if len(code) < 4: return "ANY"
        c = code[3].upper()
        mapping = {
            'J':'NTSC-J', 'E':'NTSC-U', 'P':'PAL', 'D':'GER', 
            'F':'FRA', 'I':'ITA', 'S':'ESP', 'K':'KOR', 'X':'PAL'
        }
        return mapping.get(c, "ANY")

    def process_boxart(self, code, name, plat):
        dest = os.path.join(self.boxarts_dir, f"{code}.png")
        if os.path.exists(dest): return dest
        
        if plat in ["nds", "dsi"]:
            for reg in self.regions_tdb:
                url = f"https://art.gametdb.com/ds/cover/{reg}/{code}.jpg"
                try:
                    r = requests.get(url, timeout=3)
                    if r.status_code == 200:
                        img = Image.open(BytesIO(r.content))
                        img.save(dest, "PNG")
                        return dest
                except: continue
        
        system = "Nintendo%20-%20Nintendo%20DS" if plat != "gba" else "Nintendo%20-%20Game%20Boy%20Advance"
        url_libretro = f"https://thumbnails.libretro.com/{system}/Named_Boxarts/{urllib.parse.quote(name)}.png"
        try:
            r = requests.get(url_libretro, timeout=3)
            if r.status_code == 200:
                Image.open(BytesIO(r.content)).save(dest, "PNG")
                return dest
        except: pass
        return None

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
        
        # Maker Codes Integrali
        self.maker_codes = {
            "01": "Nintendo", "02": "Rocket Games", "08": "Capcom", "09": "Hot-B", "0A": "Jaleco",
            "0B": "Coconuts Japan", "0C": "Elite Systems", "13": "Electronic Arts", "18": "Hudson Soft",
            "19": "B-AI", "1A": "Yanoman", "1F": "Virgin Games", "24": "PCM Complete", "25": "San-X",
            "28": "Kotobuki Systems", "29": "Seta", "30": "Viacom", "31": "Nintendo", "32": "Bandai",
            "33": "Ocean/Acclaim", "34": "Konami", "35": "HectorSoft", "37": "Taito", "38": "Hudson",
            "39": "Banpresto", "41": "Ubisoft", "42": "Atlus", "44": "Malibu", "46": "Angel",
            "47": "Bullet-Proof", "49": "Irem", "4A": "Virgin Games", "4B": "Seta", "4F": "Epyx",
            "50": "Acclaim", "51": "Acclaim", "52": "Activision", "53": "American Sammy", "54": "Konami",
            "55": "Hi Tech Expressions", "56": "LJN", "57": "Matchbox", "58": "Mattel", "59": "Milton Bradley",
            "5A": "Mindscape", "5B": "Romstar", "5C": "Taxan", "5D": "THQ", "5E": "TradeWest",
            "60": "Titus Interactive", "61": "Virgin Games", "67": "Ocean Software", "69": "Electronic Arts",
            "6E": "Elite Systems", "6F": "Electro Brain", "70": "Infogrames", "71": "Interplay",
            "72": "Broderbund", "73": "Sculptured Software", "75": "The Sales Curve", "78": "THQ",
            "79": "Accolade", "7A": "Triffix", "7C": "MicroProse", "7F": "Kemco", "80": "Misawa",
            "83": "LOZC", "86": "Tokuma Shoten", "8B": "Bullet-Proof", "8C": "Vic Tokai", "8E": "Ape Inc.",
            "8F": "I'Max", "91": "Chunsoft", "92": "Video System", "93": "Tsuburaya", "95": "Varie",
            "96": "Yonezawa/S'Pal", "97": "Kaneko", "99": "Arc", "9A": "Nihon Bussan", "9B": "Tecmo",
            "9C": "Imagineer", "9D": "Banpresto", "9F": "Nova", "A1": "Hori Electric", "A2": "Bandai",
            "A4": "Konami", "A6": "Kawada", "A7": "Takara", "A9": "Technos Japan", "AA": "Broderbund",
            "AC": "Toei Animation", "AD": "Toho", "AF": "Namco", "B0": "Acclaim", "B1": "ASCII",
            "B2": "Bandai", "B4": "Enix", "B6": "HAL", "B7": "SNK", "B9": "Pony Canyon", "BA": "Culture Brain",
            "BB": "SunSoft", "BD": "Sony Imagesoft", "BF": "Sammy", "C0": "Taito", "C2": "Kemco",
            "C3": "Square", "C4": "Tokuma Shoten", "C5": "Data East", "C6": "Tonkin House", "C8": "Koei",
            "C9": "UFL", "CA": "Ultra", "CB": "Vap", "CC": "Use", "CD": "Meldac", "CE": "Pony Canyon",
            "CF": "Angel", "D0": "Taito", "D1": "Sofel", "D2": "Quest", "D3": "Sigma Enterprises",
            "D4": "ASK Kodansha", "D6": "Naxat Soft", "D7": "Copya System", "D9": "Banpresto",
            "DA": "Tomy", "DB": "LJN", "DD": "NCS", "DE": "Human", "DF": "Altron", "E0": "Jaleco",
            "E1": "Towachiki", "E2": "Uutaka", "E3": "Varie", "E5": "Epoch", "E7": "Athena", "E8": "Asmik",
            "E9": "Natsume", "EA": "King Records", "EB": "Atlus", "EC": "Epic/Sony Records", "EE": "IGS",
            "F0": "A Wave", "F3": "Extreme Entertainment", "FF": "LJN"
        }

        self.no_intro_db = {"nds": {}, "gba": {}, "dsi": {}}
        self.DELIMITER = "\t"

        self.ensure_dirs()
        self.load_no_intro()
        self.setup_ui()
        self.load_url_config()
        self.scan_local_roms(mode='fast')

    def ensure_dirs(self):
        for d in [self.roms_dir, self.boxarts_dir, self.dbnames_dir]:
            os.makedirs(d, exist_ok=True)
        for p in ["nds", "gba", "dsi"]:
            os.makedirs(os.path.join(self.roms_dir, p), exist_ok=True)

    def load_url_config(self):
        """Carica l'URL dal file url.txt se esiste."""
        if os.path.exists(self.url_file):
            try:
                with open(self.url_file, 'r', encoding='utf-8') as f:
                    url = f.read().strip()
                    if url:
                        self.url_input.setText(url)
            except: pass

    def save_url_config(self):
        """Salva l'URL corrente nel file url.txt."""
        try:
            with open(self.url_file, 'w', encoding='utf-8') as f:
                f.write(self.url_input.text().strip())
            QMessageBox.information(self, "Salvato", "URL salvato correttamente in url.txt")
        except Exception as e:
            QMessageBox.warning(self, "Errore", f"Impossibile salvare l'URL: {e}")

    def load_no_intro(self):
        mapping = {"nds.dat": "nds", "gba.dat": "gba", "dsi.dat": "dsi"}
        for f, plat in mapping.items():
            path = os.path.join(self.dbnames_dir, f)
            if not os.path.exists(path): continue
            try:
                tree = ET.parse(path)
                for game in tree.getroot().findall('game'):
                    name = game.get('name')
                    rom = game.find('rom')
                    if rom is not None and rom.get('serial'):
                        serial = rom.get('serial').replace("-", "").strip().upper()
                        self.no_intro_db[plat][serial] = name
            except: pass

    def setup_ui(self):
        self.central = QWidget()
        self.setCentralWidget(self.central)
        layout = QVBoxLayout(self.central)

        url_bar = QHBoxLayout()
        url_bar.addWidget(QLabel("<b>Server URL:</b>"))
        self.url_input = QLineEdit("https://myserver.com/kekatsu/")
        url_bar.addWidget(self.url_input)
        
        self.btn_save_url = QPushButton("💾 Salva URL")
        self.btn_save_url.clicked.connect(self.save_url_config)
        url_bar.addWidget(self.btn_save_url)
        layout.addLayout(url_bar)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        # Tab Ufficiali
        self.tab_off = QWidget()
        off_layout = QVBoxLayout(self.tab_off)
        self.table_off = QTableWidget(0, 10)
        self.table_off.setHorizontalHeaderLabels([
            "Box", "Titolo", "Sist.", "Regione", "Ver", "Autore", "URL ROM", "File", "Peso", "URL Box"
        ])
        self.table_off.setIconSize(QSize(48, 48))
        self.table_off.verticalHeader().setDefaultSectionSize(60)
        off_layout.addWidget(self.table_off)
        self.tabs.addTab(self.tab_off, "🎮 Ufficiali")

        # Tab Homebrew
        self.tab_hb = QWidget()
        hb_layout = QVBoxLayout(self.tab_hb)
        self.table_hb = QTableWidget(0, 7)
        self.table_hb.setHorizontalHeaderLabels(["Titolo", "Sist.", "Regione", "Ver", "URL ROM", "File", "Peso"])
        hb_layout.addWidget(self.table_hb)
        self.tabs.addTab(self.tab_hb, "🛠️ Homebrew")

        # Log
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setFont(QFont("Monospace", 9))
        self.tabs.addTab(self.log_text, "📝 Log")

        btn_bar = QHBoxLayout()
        self.btn_scan = QPushButton("🔍 Scansione Completa")
        self.btn_scan.clicked.connect(lambda: self.scan_local_roms(mode='full'))
        self.btn_export = QPushButton("💾 Esporta DATABASE.TXT")
        self.btn_export.clicked.connect(self.export_db)
        btn_bar.addWidget(self.btn_scan)
        btn_bar.addWidget(self.btn_export)
        layout.addLayout(btn_bar)

        self.pbar = QProgressBar()
        self.pbar.setVisible(False)
        layout.addWidget(self.pbar)

    def scan_local_roms(self, mode='full'):
        self.table_off.setRowCount(0)
        self.table_hb.setRowCount(0)
        tasks = []
        for p in ["nds", "gba", "dsi"]:
            p_path = os.path.join(self.roms_dir, p)
            if not os.path.exists(p_path): continue
            for f in os.listdir(p_path):
                if f.lower().endswith(('.nds', '.gba', '.dsi', '.zip')):
                    tasks.append({"path": os.path.join(p_path, f), "plat": p, "mode": mode})
        
        if tasks:
            self.pbar.setVisible(True)
            self.processor = BatchProcessor(tasks, self.boxarts_dir, self.no_intro_db, self.maker_codes)
            self.processor.row_updated_signal.connect(self.add_row)
            self.processor.progress_signal.connect(self.pbar.setValue)
            self.processor.log_signal.connect(self.log_text.append)
            self.processor.finished_signal.connect(lambda: self.pbar.setVisible(False))
            self.processor.start()

    def add_row(self, d):
        base = self.url_input.text()
        if not base.endswith("/"): base += "/"

        if not d['is_homebrew']:
            t = self.table_off
            row = t.rowCount()
            t.insertRow(row)
            if d['boxart_path']:
                t.setItem(row, 0, QTableWidgetItem())
                t.item(row, 0).setIcon(QIcon(d['boxart_path']))
            t.setItem(row, 1, QTableWidgetItem(d['title']))
            t.setItem(row, 2, QTableWidgetItem(d['plat']))
            t.setItem(row, 3, QTableWidgetItem(d['region']))
            t.setItem(row, 4, QTableWidgetItem(d['version']))
            t.setItem(row, 5, QTableWidgetItem(d['author']))
            t.setItem(row, 6, QTableWidgetItem(f"{base}roms/{d['plat']}/{d['filename']}"))
            t.setItem(row, 7, QTableWidgetItem(d['filename']))
            t.setItem(row, 8, QTableWidgetItem(str(d['size'])))
            t.setItem(row, 9, QTableWidgetItem(f"{base}boxarts/{d['code']}.png" if d['code'] else ""))
        else:
            t = self.table_hb
            row = t.rowCount()
            t.insertRow(row)
            t.setItem(row, 0, QTableWidgetItem(d['title']))
            t.setItem(row, 1, QTableWidgetItem(d['plat']))
            t.setItem(row, 2, QTableWidgetItem(d['region']))
            t.setItem(row, 3, QTableWidgetItem(d['version']))
            t.setItem(row, 4, QTableWidgetItem(f"{base}roms/{d['plat']}/{d['filename']}"))
            t.setItem(row, 5, QTableWidgetItem(d['filename']))
            t.setItem(row, 6, QTableWidgetItem(str(d['size'])))

    def export_db(self):
        dest = os.path.join(self.base_dir, "database.txt")
        lines = ["1", self.DELIMITER]
        
        def clean(txt):
            return str(txt).replace('\n', ' ').replace('\r', '').strip()

        for r in range(self.table_off.rowCount()):
            row = [clean(self.table_off.item(r, i).text()) for i in range(1, 10)]
            lines.append(self.DELIMITER.join(row))
        
        for r in range(self.table_hb.rowCount()):
            row = [
                clean(self.table_hb.item(r, 0).text()), clean(self.table_hb.item(r, 1).text()),
                clean(self.table_hb.item(r, 2).text()), clean(self.table_hb.item(r, 3).text()),
                "Homebrew", clean(self.table_hb.item(r, 4).text()), 
                clean(self.table_hb.item(r, 5).text()), clean(self.table_hb.item(r, 6).text()), ""
            ]
            lines.append(self.DELIMITER.join(row))

        with open(dest, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines))
        QMessageBox.information(self, "Esportato", f"Database salvato in {dest}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = KekatsuManager()
    window.show()
    sys.exit(app.exec())