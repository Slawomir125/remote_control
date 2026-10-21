# Master–Slave — system zdalnego sterowania

Projekt implementuje rozproszoną architekturę Master–Slave do koordynowania
pracy wielu klientów roboczych. Koordynator (Master) zarządza kolejką zadań,
wydaje polecenia i udostępnia panel webowy. Klienci (Slaves) łączą się
z Masterem, pobierają zadania, wykonują polecenia i odsyłają wyniki.

## Główne funkcje

- **Modularny koordynator** — pakiet `master/` (11 modułów) z dekoratorowym
  systemem rejestracji endpointów, komend i typów pakietów.
- **Panel Master Center** — interfejs przeglądarkowy z zakładkami Slaves i SSH,
  terminalem poleceń, makrami, historią i KPI w czasie rzeczywistym.
- **SSH Remote Control** — zarządzanie sesjami SSH z poziomu panelu
  (ControlMaster, połączenia sesyjne, terminal SSH).
- **Curl bootstrap** — uruchamianie slave'a jednym poleceniem:
  `curl -s http://master:8080/join/<kod> | python3` — bez kopiowania plików
  ani przepisywania tokenów.
- **Makra z pełną składnią shella** — potoki, przekierowania, zmienne.
- **Tokeny jednorazowe** — kody startowe z 10-minutowym wygaśnięciem.
- **SQLite z WAL** — trwałe przechowywanie wyników i historii.
- **Docker** — `Dockerfile` + `compose.yaml` do konteneryzacji Mastera.

## Struktura projektu

```
├── master/                  # pakiet koordynatora (Python)
│   ├── main.py              # punkt startowy, montaż komponentów
│   ├── config.py            # stałe, ścieżki, rejestry, dekoratory
│   ├── stan.py              # struktury danych (Stan, Slave, Polecenie)
│   ├── baza.py              # SQLite, migracje, persystencja
│   ├── logika.py            # rejestracja, polecenia, tokeny, makra
│   ├── panel.py             # HTTP panel + endpointy API + /join/ bootstrap
│   ├── serwer_slaves.py     # asyncio TCP serwer dla slave'ów
│   ├── ssh.py               # SSH ControlMaster — sesje i endpointy
│   ├── konsola.py           # interaktywna konsola Mastera
│   ├── widoki.py            # serializacja stanu do JSON (snapshoty)
│   └── zadania.py           # ocena odpowiedzi, tempo, statystyki
├── master.py                # wrapper uruchamiający master/main.py
├── master_center.html       # panel webowy (Slaves + SSH)
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
- serwer slave'ów na porcie 9000
- panel webowy na http://127.0.0.1:8080

### Klient roboczy

**Szybki sposób** (curl bootstrap — nie wymaga pliku slave.py na maszynie):

1. W panelu Master Center wygeneruj jednorazowy kod startowy
2. Na zdalnej maszynie:
```bash
curl -s http://master:8080/join/<kod> | python3
```

**Tradycyjny sposób** (wymaga slave.py):

```bash
python3 slave.py --master <adres> --token-rejestracji <token> --nazwa moj-slave
```

Slave obsługuje też zmienne środowiskowe: `SLAVE_MASTER`, `SLAVE_TOKEN`,
`SLAVE_NAME`, `SLAVE_HTTPS`.

## SSH Remote Control

Zakładka SSH w panelu pozwala:
- Dodawać hosty SSH (user@host:port)
- Nawiązywać sesje ControlMaster (stałe połączenie ~5ms per komenda)
- Wykonywać polecenia na zdalnych maszynach z terminala SSH
- Łączyć/rozłączać wszystkie hosty jednym kliknięciem

SSH i sterowanie slave'ami to osobne, niezależne funkcjonalności.

## Bezpieczeństwo

- Panel chroniony tokenem (plik `master_panel_token.txt`).
- Slave'y autoryzowane tokenami bearer po rejestracji.
- Tokeny jednorazowe wygasają po 10 minutach i działają tylko raz.
- Nie wystawiaj panelu publicznie — używaj tunelu SSH.
- `.gitignore` wyklucza bazy, tokeny, klucze i pliki stanu.

Więcej szczegółów w [ARCHITECTURE.md](./ARCHITECTURE.md).
