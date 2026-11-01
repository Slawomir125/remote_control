# Architektura projektu

## Przegląd

System składa się z trzech warstw: **koordynator** (Master), **klienci roboczi**
(Slaves) i **panel webowy** (Master Center). Komunikacja Master↔Slave odbywa się
po HTTP/HTTPS; każdy Slave po pierwszej rejestracji dostaje bearer token i jest
widziany jako sesja — tak samo jak host SSH. Panel łączy się z Masterem przez
lokalne API HTTP (bez logowania — używaj tunelu SSH do zdalnego dostępu).

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
| `stan.py` | Dataclassy: `Stan`, `Slave` (z polem `sesja`), `Polecenie` |
| `baza.py` | SQLite (WAL), migracje (w tym kolumna `sesja`), persystencja |
| `logika.py` | Rejestracja, sesje per Slave (`polacz_slave`/`rozlacz_slave`), polecenia, makra |
| `panel.py` | Serwer HTTP panelu, endpointy API, routing (bez tokenu panelu) |
| `serwer_slaves.py` | asyncio TCP serwer — komunikacja ze slave'ami + monitor sesji |
| `ssh.py` | SSH ControlMaster — sesje, endpointy `/api/ssh/*` |
| `konsola.py` | Interaktywna konsola (`polec`, `polacz`, `rozlacz`, `makro`, …) |
| `widoki.py` | Serializacja stanu do JSON (snapshoty dla panelu) |
| `zadania.py` | Ocena odpowiedzi, okno czasowe, tempo, statystyki |

Transfer plików przecina kilka modułów:
- `panel.py` — endpointy `/api/slave/wyslij,pobierz` i `/api/ssh/wyslij,pobierz`
  (strumieniowanie przez `@endpoint(strumien=True)` z surowym `rfile`/`wfile`).
- `serwer_slaves.py` — dodatkowe routes `/plik/wyslij,pobierz` dla slave'ów
  (surowe bajty, `Authorization: Bearer <token slave'a>`).
- `ssh.py` — `wyslij_ssh`/`pobierz_ssh` używają `ssh -S <ControlPath> cel 'cat …'`.
- `stan.py` — staging `pliki_do_slave[cmd_id]` i `pliki_od_slave[cmd_id]`
  z `threading.Event` do synchronizacji operator ↔ slave.

### Przepływ danych

```
Slave ──HTTP/HTTPS──► serwer_slaves.py ──► logika.py ──► baza.py (SQLite)
                           │                     │
                           │                     ▼
                           │              stan.slaves[*].sesja
                           ▼                     │
                   gating sesji (503              │
                   gdy rozłączony)                │
                                                 ▼
Panel ◄──HTTP──── panel.py ◄──── widoki.py ◄── stan.py (pamięć)
                     │
                     ▼
               ssh.py ──SSH ControlMaster──► zdalne hosty
```

1. Slave łączy się z `serwer_slaves.py`, przedstawia token.
2. `logika.przetworz_prace` sprawdza `slave.sesja`:
   - `rozłączony` → zwraca sentinel; serwer odpowiada `503`.
   - `łączenie`/`błąd` → przejście do `połączony`, odpowiedź normalna.
   - `połączony` → `ostatnio = teraz`, odpowiedź normalna.
3. `baza.py` zapisuje wyniki do SQLite; `stan.py` trzyma stan w pamięci.
4. `serwer_slaves.raporter` co 2 s: Slave z sesją `połączony` i `teraz - ostatnio
   > AKTYWNY_DO_S` → sesja = `łączenie` (zgubiliśmy kontakt, ale sesja wciąż
   otwarta — czeka na powrót).
5. Panel odpytuje `widoki.snapshot` co sekundę przez `/api/stan`.
6. SSH działa niezależnie — `ssh.py` zarządza sesjami ControlMaster.

### Sesje per Slave

Pole `Slave.sesja ∈ {"rozłączony", "łączenie", "połączony", "błąd"}` — ten sam
cykl życia co w `HostSSH.sesja`. Przechowywane w pamięci; kolumna `sesja` w SQLite
jest aktualizowana tylko na explicitne akcje admina (`Połącz`/`Rozłącz`), więc
intencja admina przeżywa restart Mastera.

**Master trzyma sesję za wszelką cenę.** Sesja ląduje w `rozłączony` wyłącznie
po explicitnej akcji admina (`/api/slave/rozlacz` lub konsola `rozlacz`); ta
decyzja persystowana jest do DB i przeżywa restart. Wszystkie inne stany
(`połączony`/`łączenie`/`błąd`) po restarcie startują jako `łączenie` — sesja
otwarta, czeka na pakiet od Slave'a. Nawet błąd tokena nie zamyka sesji —
pierwszy udany pakiet przywraca `połączony`.

**Przejścia:**

| Zdarzenie | Przejście |
|-----------|-----------|
| Rejestracja nowego Slave | → `połączony` |
| Pakiet, sesja == `rozłączony` | 503, bez zmiany sesji |
| Pakiet, sesja ∈ {`łączenie`, `błąd`} | → `połączony` |
| Admin „Rozłącz" (persystowane do DB) | → `rozłączony` |
| Admin „Połącz" (persystowane do DB) | → `łączenie` (czekamy na pakiet) |
| Timeout `AKTYWNY_DO_S` z `połączony` | → `łączenie` |
| Nieprawidłowy token Slave'a | → `błąd`, 401 (sesja wciąż otwarta) |
| Start Mastera (odtwarzanie z DB) | `rozłączony` zostaje; każdy inny stan → `łączenie` |

## Klient roboczy — `slave.py`

Slave to samodzielny skrypt z CLI:
`python3 slave.py --master X --token-rejestracji Y`

Argumenty mają fallback na zmienne środowiskowe (`SLAVE_MASTER`, `SLAVE_TOKEN`,
`SLAVE_NAME`, `SLAVE_HTTPS`). Po rejestracji stan zapisywany
w `slave_state/<nazwa>.json` — ponowne uruchomienie nie wymaga tokenu.

### Obsługa poleceń

Polecenia od Mastera obsługiwane przez dekorator `@polecenie`:
`exec` (uruchom program), `makro` (sekwencja z .sh), `pauza`, `wznow`, `status`, `stop`.

## Panel — `master_center.html`

Jednostronicowa aplikacja z trzema zakładkami. **Panel nie wymaga tokenu
ani logowania** — zabezpiecz go po stronie sieci (localhost + tunel SSH).

### Zakładka Slaves
- KPI: sesje połączone, zadania/s, czas pracy, skuteczność, śr. czas
- Tabela Slaves z kolumną Sesja i przyciskami Połącz / Rozłącz / Historia per wiersz
- Terminal poleceń z selektorem makr (cel: ID albo zakres `1-12`)
- Sekcja „Dodaj Slave'a" generuje gotowe polecenie `python3 slave.py …`

### Zakładka SSH
- KPI: hosty, aktywne sesje, polecenia
- Tabela hostów z sesjami (połączony/rozłączony/łączenie/błąd)
- Dodawanie/usuwanie hostów, masowe łączenie/rozłączanie
- Terminal SSH z selectorem hosta
- Feed poleceń z pauzą

### Zakładka Znane hosty
- Unifikowany widok Slaves + SSH, sortowanie, status aktywności

## SSH ControlMaster

Sesje SSH używają `ssh -MNf -o ControlMaster=yes` z socketem w `/tmp/master_ssh/`.
Kolejne polecenia do tego samego hosta reużywają socket (~5ms vs sekundy).
Stany sesji: `rozłączony` → `łączenie` → `połączony` / `błąd`.

## Transfer plików — dwa kanały

| Cecha           | SSH tab                              | Slaves tab                          |
|-----------------|--------------------------------------|-------------------------------------|
| Wymóg           | aktywna sesja ControlMaster          | aktywna sesja Slave                 |
| Transport       | `ssh -S socket cel 'cat'`            | HTTP `POST/GET /plik/*`             |
| Chunk           | streaming (3 MiB bufor)              | 3 MiB (mieści się w `MAX_LINIA`)    |
| Format          | surowe bajty na stdin/stdout         | surowe bajty w ciele HTTP           |
| Weryfikacja     | brak (SSH gwarantuje spójność)       | sha256 (operator ↔ slave)           |
| Kolejkowanie    | synchroniczne (per request)          | asynchroniczne (polecenie w kolejce)|
| Max rozmiar     | `PLIKI_MAX_B` (env `…_MAX_MB`)       | `PLIKI_MAX_B` (env `…_MAX_MB`)      |

Przepływ Slaves-tab download (operator ← slave):
```
operator GET /api/slave/pobierz   ──► master kolejkuje `wyslij <path>` dla slave'a
                                      ────────────────────────────────────────┐
operator ◄──────── odpowiedź HTTP ◄── master czeka na pliki_od_slave[id].event│
                                                                              │
slave POST /plik/wyslij?pol=X&off=N …   (chunk po chunku)         ◄───────────┘
                              …&final=1                            event.set()
```

Przepływ Slaves-tab upload (operator → slave) — symetrycznie, przez
`pliki_do_slave[id]` i polecenie `pobierz` z sha256+rozmiarem.

## Tokeny i autoryzacja

| Token | Cel | Ważność |
|-------|-----|---------|
| Token rejestracji | Slave dołącza do Mastera | Stały (plik/env var) |
| Token bearer | Identyfikacja zarejestrowanego Slave'a | Sesja |

## Konteneryzacja

`Dockerfile` buduje obraz Mastera z pakietem `master/`, `master_center.html`
i `macros/`. `compose.yaml` montuje volume na dane trwałe i mapuje porty 9000
(slave'y) i 8080 (panel).
