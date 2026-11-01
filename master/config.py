"""Stałe konfiguracyjne, ścieżki, rejestry i dekoratory."""

import os
import secrets
from pathlib import Path

# ─── ścieżki ──────────────────────────────────────────────────────────────────
KATALOG = Path(__file__).resolve().parent.parent          # katalog projektu (nad master/)
KATALOG_DANYCH = Path(os.environ.get("MASTER_DATA_DIR", str(KATALOG)))
HOST = os.environ.get("MASTER_HOST", "127.0.0.1")
PORT = int(os.environ.get("MASTER_PORT", "9000"))
PANEL_HOST = os.environ.get("MASTER_PANEL_HOST", "127.0.0.1")
PANEL_PORT = int(os.environ.get("MASTER_PANEL_PORT", "8080"))

# Publiczny endpoint do którego faktycznie łączą się Slaves (gdy master siedzi
# za reverse proxy / Cloudflare Tunnel). Panel używa tego do generowania CLI.
# Puste → fallback do PORT/False (lokalny dev).
SLAVE_PUBLIC_PORT = int(os.environ.get("MASTER_SLAVES_PUBLIC_PORT", "0") or "0")
_pub_https = os.environ.get("MASTER_SLAVES_PUBLIC_HTTPS", "")
SLAVE_PUBLIC_HTTPS = None if _pub_https == "" else _pub_https.strip().lower() in ("1", "true", "yes", "on")
PLIK_BAZY = KATALOG_DANYCH / "master.db"
PLIK_TOKEN_REJESTRACJI = KATALOG_DANYCH / "master_registration_token.txt"
PLIK_PANELU = KATALOG / "master_center.html"
MACRO_DIR = KATALOG / "macros"
PLIK_SLAVE = KATALOG / "slave.py"
STARY_STAN = KATALOG_DANYCH / "master_stan.json"
LEGACY_STARY_STAN = KATALOG_DANYCH / "gru_stan.json"

# ─── limity ───────────────────────────────────────────────────────────────────
AKTYWNY_DO_S = 15                            # Slave bez kontaktu dłużej = sesja -> łączenie
CISZA_ROZLACZ_S = 60                         # timeout żądania HTTP Slaves
MAX_LINIA = 4 * 1024 * 1024                  # maksymalny rozmiar treści HTTP (4 MiB)
BACKLOG = 4096                               # kolejka połączeń oczekujących na accept()
POLECENIE_TTL_S = 300                        # nieodebrane polecenie wygasa po tylu sekundach
MAX_POLECEN = 200                            # ile ostatnich poleceń pamięta Master
LOG_ZADAN_NA_S = 20                          # limit linii "zadanie rozwiązane" w konsoli na sekundę
RAPORT_CO_S = 2                              # co ile s Master zbiera rejestracje/odrzucenia w jedną linię

# ─── transfer plików ──────────────────────────────────────────────────────────
PLIKI_MAX_B = int(os.environ.get("MASTER_PLIKI_MAX_MB", "1024")) * 1024 * 1024  # domyślnie 1 GiB
PLIKI_CHUNK_B = 3 * 1024 * 1024              # chunk strumienia Slave↔Master (mieści się w MAX_LINIA 4 MiB)
KATALOG_PLIKOW = KATALOG_DANYCH / "pliki"    # staging przychodzących plików (slave→master) i downloads

# ─── rejestry ─────────────────────────────────────────────────────────────────
# Tu lądują funkcje oznaczone dekoratorami. Reszta kodu tylko czyta te rejestry.

OPERACJE = {"+", "-", "*", "//"}
TYPY_PAKIETOW = {}   # typ -> (funkcja(stan, slave_id, dane) -> dict | None, anonimowy)
KOMENDY = {}         # nazwa/alias -> funkcja(stan, arg) -> str | None | KONIEC
OPISY_KOMEND = []    # (nazwa, opis) - do komendy 'pomoc'
ENDPOINTY = []       # (metoda, sciezka, prefix, strumien, funkcja)
KONIEC = object()    # zwróć z komendy, żeby zakończyć Master

# Polecenia, które Master potrafi wydać Slaves (nazwa -> opis).
POLECENIA_SLAVES = {
    "exec": "<program> [flagi...] - uruchamia program bez shella",
    "makro": "sekwencja z pliku .sh w katalogu macros/ (bez shella)",
    "promuj_ssh": "<klucz pub> - zainstaluj klucz Mastera i zwróć IP/user/hostname",
    "pauza": "wstrzymuje pobieranie zadań",
    "wznow": "wznawia pracę po pauzie",
    "status": "Slave odsyła swoje liczniki",
    "stop": "Slave kończy pracę",
    "wyslij": "<sciezka> - Slave strumieniuje plik do staging mastera (chunked HTTP)",
    "pobierz": "<sciezka> <sha256> <rozmiar> - Slave pobiera plik ze staging mastera",
}
PANEL_ADMIN_COMMANDS = {"pauza", "wznow", "stop"}


# ─── dekoratory rejestrujące ─────────────────────────────────────────────────

def typ_pakietu(nazwa, anonimowy=False):
    """anonimowy=True: pakiet idzie z ID "0" i tokenem bootstrapowym (np. rejestracja)."""
    def dekorator(f):
        TYPY_PAKIETOW[nazwa] = (f, anonimowy)
        return f
    return dekorator


def komenda(nazwa, opis, aliasy=()):
    def dekorator(f):
        OPISY_KOMEND.append((nazwa, opis))
        for n in (nazwa, *aliasy):
            KOMENDY[n] = f
        return f
    return dekorator


def endpoint(sciezka, prefix=False, metoda="GET", strumien=False):
    """strumien=True: handler sam czyta ciało z zapyt.rfile (do PLIKI_MAX_B).
    Pomija parsowanie JSON i limit 10 KiB — używać dla uploadów."""
    def dekorator(f):
        ENDPOINTY.append((metoda, sciezka, prefix, strumien, f))
        return f
    return dekorator


# ─── tokeny z plików ─────────────────────────────────────────────────────────

def wczytaj_token_rejestracji():
    env = os.environ.get("MASTER_REGISTRATION_TOKEN")
    if env:
        return env.strip()
    if PLIK_TOKEN_REJESTRACJI.exists():
        return PLIK_TOKEN_REJESTRACJI.read_text(encoding="utf-8").strip()
    token = secrets.token_urlsafe(32)
    PLIK_TOKEN_REJESTRACJI.write_text(token, encoding="utf-8")
    try:
        os.chmod(PLIK_TOKEN_REJESTRACJI, 0o600)
    except OSError:
        pass
    return token
