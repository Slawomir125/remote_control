# Architektura projektu

## Przegląd

System składa się z trzech warstw: **koordynator** (Master), **klienci roboczi**
(Slaves) i **panel webowy** (Master Center). Komunikacja Master↔Slave odbywa się
po HTTP/HTTPS z tokenami bearer. Panel łączy się z Masterem przez lokalne API HTTP
chronione tokenem. Dodatkowo Master może zarządzać sesjami SSH do dowolnych hostów.

## Koordynator — pakiet `master/`

Monolityczny `master.py` został rozbity na 11 modułów w pakiecie `master/`.
Punkty rozbudowy to **rejestry wypełniane dekoratorami**:

| Dekorator | Rejestr | Zastosowanie |
|-----------|---------|-------------|
| `@endpoint(ścieżka)` | `ENDPOINTY` | nowy adres HTTP w panelu |
| `@komenda(nazwa, opis)` | `KOMENDY` | nowa komenda konsoli |
| `@typ_pakietu(nazwa)` | `TYPY_PAKIETOW` | nowy typ wiadomości od Slaves |

### Moduły

| Moduł | Odpowiedzialność |
|-------|-----------------|
| `main.py` | Punkt startowy — montuje serwery, uruchamia konsolę |
| `config.py` | Stałe, ścieżki, limity, rejestry dekoratorów |
| `stan.py` | Dataclassy: `Stan`, `Slave`, `Polecenie` |
| `baza.py` | SQLite (WAL), migracje, persystencja, odtwarzanie stanu |
| `logika.py` | Rejestracja slave'ów, polecenia, tokeny jednorazowe, kody startowe, makra |
| `panel.py` | Serwer HTTP panelu, endpointy API, routing, `/join/` bootstrap |
| `serwer_slaves.py` | asyncio TCP serwer — komunikacja ze slave'ami |
| `ssh.py` | SSH ControlMaster — sesje, endpointy `/api/ssh/*` |
| `konsola.py` | Interaktywna konsola (komendy `start`, `stop`, `polecenie`, `makro`, …) |
| `widoki.py` | Serializacja stanu do JSON (snapshoty dla panelu) |
| `zadania.py` | Ocena odpowiedzi, okno czasowe, tempo, statystyki |

### Przepływ danych

```
Slave ──HTTP/HTTPS──► serwer_slaves.py ──► logika.py ──► baza.py (SQLite)
                                               │
                                               ▼
Panel ◄──HTTP──── panel.py ◄──── widoki.py ◄── stan.py (pamięć)
                     │
                     ▼
               ssh.py ──SSH ControlMaster──► zdalne hosty
```

1. Slave łączy się z `serwer_slaves.py`, przedstawia token.
2. `logika.py` przetwarza pakiet (rejestracja / wynik / potwierdzenie polecenia).
3. `baza.py` zapisuje do SQLite; `stan.py` trzyma stan w pamięci.
4. Panel odpytuje `widoki.py` co sekundę (`/api/stan`).
5. SSH działa niezależnie — `ssh.py` zarządza sesjami ControlMaster.

## Klient roboczy — `slave.py`

Slave to samodzielny skrypt (~535 linii) z dwoma trybami uruchomienia:

- **CLI**: `python3 slave.py --master X --token-rejestracji Y`
- **Curl bootstrap**: `curl -s http://master:8080/join/<kod> | python3`

Argumenty mają fallback na zmienne środowiskowe (`SLAVE_MASTER`, `SLAVE_TOKEN`,
`SLAVE_NAME`, `SLAVE_HTTPS`), co umożliwia bootstrap przez preambułę env vars.

Po rejestracji stan zapisywany w `slave_state/<nazwa>.json` — ponowne uruchomienie
nie wymaga tokenu.

### Obsługa poleceń

Polecenia od Mastera obsługiwane przez dekorator `@polecenie`:
`exec` (uruchom program), `makro` (sekwencja z .sh), `pauza`, `wznow`, `status`, `stop`.

## Panel — `master_center.html`

Jednostronicowa aplikacja z dwoma zakładkami:

### Zakładka Slaves
- KPI: slave'y, aktywne, zadania/s, czas pracy
- Tabela slave'ów z kontrolkami (pauza/wznów/stop)
- Terminal poleceń z selektorem makr
- Jednorazowy kod startowy z curl bootstrap i tradycyjnym poleceniem

### Zakładka SSH
- KPI: hosty, aktywne sesje, polecenia
- Tabela hostów z sesjami (połączony/rozłączony/łączenie/błąd)
- Dodawanie/usuwanie hostów, masowe łączenie/rozłączanie
- Terminal SSH z selectorem hosta
- Feed poleceń z pauzą

## SSH ControlMaster

Sesje SSH używają `ssh -MNf -o ControlMaster=yes` z socketem w `/tmp/master_ssh/`.
Kolejne polecenia do tego samego hosta reużywają socket (~5ms vs sekundy).
Stany sesji: `rozłączony` → `łączenie` → `połączony` / `błąd`.

## Curl bootstrap — `/join/<kod>`

Mechanizm jednolinijkowego uruchomienia slave'a:

1. Panel generuje jednorazowy 6-znakowy kod (POST `/api/token-jednorazowy`)
2. Kod mapuje do konfiguracji: master, token, nazwa, https
3. `GET /join/<kod>` zwraca `text/x-python`: preambułę env vars + pełny slave.py
4. Kod jest jednorazowy — drugie pobranie zwraca 404
5. Kod wygasa po 10 minutach

## Tokeny i autoryzacja

| Token | Cel | Ważność |
|-------|-----|---------|
| Token rejestracji | Slave dołącza do Mastera | Stały (plik/env var) |
| Token bearer | Identyfikacja zarejestrowanego slave'a | Sesja |
| Token panelu | Chronione endpointy API | Stały (plik/env var) |
| Token jednorazowy | Pojedyncze dołączenie slave'a | 10 minut, jednorazowy |
| Kod startowy | Skrót do curl bootstrap | 10 minut, jednorazowy |

## Konteneryzacja

`Dockerfile` buduje obraz Mastera z pakietem `master/`, `master_center.html`
i `macros/`. `compose.yaml` montuje volume na dane trwałe i mapuje porty 9000
(slave'y) i 8080 (panel).
