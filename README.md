# Master–Slave — system zdalnego sterowania

Projekt implementuje rozproszoną architekturę Master–Slave do koordynowania
pracy wielu klientów roboczych. Koordynator (Master) jest zawsze aktywny
i nasłuchuje Slave'ów na stałym porcie TCP. Każdy Slave ma własną sesję —
jak host SSH — którą admin otwiera i zamyka z panelu.

## Główne funkcje

- **Modularny koordynator** — pakiet `master/` (11 modułów) z dekoratorowym
  systemem rejestracji endpointów, komend i typów pakietów.
- **Panel Master Center** — interfejs przeglądarkowy bez logowania, z zakładkami
  Slaves / SSH / Znane hosty, terminalem poleceń, makrami, historią i KPI
  w czasie rzeczywistym.
- **Sesje per Slave** — każdy Slave ma pole `sesja` (`rozłączony` / `łączenie`
  / `połączony` / `błąd`), tak samo jak host SSH. Admin otwiera/zamyka sesję
  pojedynczym przyciskiem w panelu lub komendą `polacz` / `rozlacz` w konsoli.
- **SSH Remote Control** — zarządzanie sesjami SSH z poziomu panelu
  (ControlMaster, połączenia sesyjne, terminal SSH).
- **Makra z pełną składnią shella** — potoki, przekierowania, zmienne.
- **Transfer plików** — w terminalu panelu: `wyslij <ścieżka>` (file picker) i
  `pobierz <ścieżka>` (auto-download), działa dla Slaves i SSH. Dla SSH streaming
  przez istniejącą sesję ControlMaster; dla Slaves chunked HTTP ze sha256.
- **SQLite z WAL** — trwałe przechowywanie wyników i historii.
- **Docker** — `Dockerfile` + `compose.yaml` do konteneryzacji Mastera.

## Struktura projektu

```
├── master/                  # pakiet koordynatora (Python)
│   ├── main.py              # punkt startowy, montaż komponentów
│   ├── config.py            # stałe, ścieżki, rejestry, dekoratory
│   ├── stan.py              # struktury danych (Stan, Slave, Polecenie)
│   ├── baza.py              # SQLite, migracje, persystencja
│   ├── logika.py            # rejestracja, sesje per Slave, polecenia, makra
│   ├── panel.py             # HTTP panel + endpointy API
│   ├── serwer_slaves.py     # asyncio TCP serwer dla slave'ów + monitor sesji
│   ├── ssh.py               # SSH ControlMaster — sesje i endpointy
│   ├── konsola.py           # interaktywna konsola Mastera
│   ├── widoki.py            # serializacja stanu do JSON (snapshoty)
│   └── zadania.py           # ocena odpowiedzi, tempo, statystyki
├── master.py                # wrapper uruchamiający master/main.py
├── master_center.html       # panel webowy (Slaves + SSH + Znane hosty)
├── slave.py                 # klient roboczy
├── slave/slave.py           # kopia slave.py
├── run_slaves.py            # uruchamianie wielu slave'ów (testy obciążeniowe)
├── slave_board.py           # osobny mini-serwer HTTP
├── macros/                  # pliki .sh z poleceniami
├── Dockerfile               # obraz Mastera
└── compose.yaml             # Docker Compose
```

## Szybki start

### Koordynator

```bash
python3 master.py
# lub z Dockerem:
docker compose up
```

Master uruchomi:
- serwer slave'ów na porcie 9000 (zawsze aktywny — brak start/stop)
- panel webowy na http://127.0.0.1:8080 (bez logowania)

### Klient roboczy

W panelu w zakładce **Slaves**, sekcja „Dodaj Slave'a": podaj adres Master
i opcjonalną nazwę — panel wygeneruje gotowe polecenie z tokenem rejestracji.
Skopiuj je i uruchom na docelowej maszynie:

```bash
python3 slave.py --master <adres> --token-rejestracji <token> --nazwa <nazwa>
```

Slave obsługuje też zmienne środowiskowe: `SLAVE_MASTER`, `SLAVE_TOKEN`,
`SLAVE_NAME`, `SLAVE_HTTPS`.

Po rejestracji sesja Slave'a jest od razu `połączony`. Jeśli admin kliknie
„Rozłącz" w panelu, kolejne polling'i dostają `503` aż ktoś kliknie „Połącz".

## SSH Remote Control

Zakładka SSH w panelu pozwala:
- Dodawać hosty SSH (user@host:port)
- Nawiązywać sesje ControlMaster (stałe połączenie ~5ms per komenda)
- Wykonywać polecenia na zdalnych maszynach z terminala SSH
- Łączyć/rozłączać wszystkie hosty jednym kliknięciem

SSH i sterowanie slave'ami to osobne, niezależne funkcjonalności.

## Transfer plików

Dostępny w terminalach obu zakładek (Slaves i SSH):

```
wyslij  /zdalna/sciezka    # otwiera file picker; wysyła wybrany plik do hosta
pobierz /zdalna/sciezka    # pobiera plik z hosta i zapisuje przez przeglądarkę
```

Działanie zależy od zakładki:

- **Zakładka SSH** — streaming przez istniejącą sesję ControlMaster
  (`cat`-piping, 8-bit clean, bez tempfile). Wymaga aktywnej sesji
  (`Połącz` najpierw). Prędkość jak natywne `scp`.
- **Zakładka Slaves** — chunked HTTP przez dedykowane endpointy `/plik/*`
  (3 MiB chunki, raw bajty, sha256 kontrolny). Osobny kanał od pollingowego
  `/api/slave/<id>` — pokazuje dydaktycznie, że beacony często otwierają
  side-channel na duże transfery.

Limit domyślny: 1 GiB per plik (zmiana przez `MASTER_PLIKI_MAX_MB`).

## Bezpieczeństwo

- Slave'y autoryzowane tokenami bearer po rejestracji.
- Token rejestracji w pliku `master_registration_token.txt` (lub env var
  `MASTER_REGISTRATION_TOKEN`) — Slave podaje go przy pierwszym pakiecie.
- Panel nie wymaga logowania — nie wystawiaj go publicznie, używaj tunelu SSH.
- `.gitignore` wyklucza bazy, tokeny, klucze i pliki stanu.

Więcej szczegółów w [ARCHITECTURE.md](./ARCHITECTURE.md).
