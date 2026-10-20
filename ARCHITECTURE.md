# Architektura projektu

## Zastrzeżenie dotyczące obecnej implementacji

Poniższy opis przedstawia komponenty i przepływ danych na poziomie ogólnym.
Obecny projekt nie jest tylko wizualną symulacją: klient może wykonywać
rzeczywiste polecenia i uruchamiać procesy na swoim hoście. Używaj go wyłącznie
w kontrolowanym środowisku, na własnych maszynach. Sieć lokalna nie gwarantuje,
że wszystkie znajdujące się w niej urządzenia są Twoje ani że można na nich
bezpiecznie testować.

## Komponenty

### Koordynator

`master.py` utrzymuje stan klientów i zadań, przyjmuje ich zgłoszenia,
przetwarza odpowiedzi oraz zapisuje wybrane dane w SQLite. Udostępnia osobną
warstwę komunikacji dla klientów i warstwę panelu administracyjnego. Konsola
pozwala obserwować stan oraz zarządzać zadaniami.

### Klient roboczy

`slave.py` przechowuje lokalny stan sesji, okresowo komunikuje się
z koordynatorem, odbiera pracę lub polecenia i odsyła wyniki. Obsługa poleceń
obejmuje akcje systemowe, dlatego klient należy traktować jako program
wykonujący zdalnie zlecone działania, a nie jako nieszkodliwy generator
telemetrii. `slave/slave.py` jest obecnie identyczną kopią tego pliku.

### Panel

`master_center.html` prezentuje stan i historię zdarzeń oraz udostępnia
interfejs sterujący. Panel korzysta z API koordynatora. Dostęp do panelu
powinien być ograniczony do zaufanego środowiska; nie należy wystawiać go
publicznie.

### Uruchamianie i dane

`run_slaves.py` służy do uruchamiania wielu instancji klientów w testach.
`Dockerfile` i `compose.yaml` opisują kontener dla koordynatora. Dane trwałe,
pliki stanu, bazy SQLite i poświadczenia są przechowywane lokalnie i nie
powinny trafiać do repozytorium ani do publicznych obrazów.

## Ogólny przepływ danych

1. Klient nawiązuje komunikację z koordynatorem i przedstawia swoją sesję.
2. Koordynator aktualizuje stan klienta i, jeśli ma do wykonania pracę,
   przekazuje ją klientowi.
3. Klient przetwarza otrzymane dane, a następnie odsyła wynik i aktualizację
   swojego stanu.
4. Koordynator zapisuje wybrane informacje i udostępnia je konsoli oraz
   panelowi.
5. Panel odświeża widok na podstawie danych udostępnianych przez koordynator.

To opis logiczny, a nie specyfikacja protokołu. Nie należy traktować go jako
instrukcji wdrażania usługi ani podłączania klientów do urządzeń.

## Granice zaufania i ryzyka

- Klient wykonujący polecenia ma wpływ na system, na którym działa.
- Poświadczenia i lokalny stan klienta są wrażliwymi danymi.
- Kanał sieciowy i panel wymagają ochrony; samo użycie prywatnego adresu IP
  nie zapewnia uwierzytelnienia ani autoryzacji urządzeń.
- Pliki SQLite, klucze, tokeny i kopie stanu nie powinny być commitowane.
- `slave.py` i `slave/slave.py` są zduplikowane; zmianę funkcjonalną łatwo
  zastosować tylko w jednej kopii.

## Bezpieczny wariant edukacyjny

Docelowy symulator powinien zastąpić klientów syntetycznymi obiektami w pamięci,
a polecenia — zamkniętym katalogiem fikcyjnych zdarzeń. Powinien pokazywać
kolejkę, heartbeat, statusy i wyniki scenariuszy, ale nie uruchamiać procesów,
nie wykonywać makr, nie zapisywać poświadczeń klientów i nie komunikować się
z rzeczywistymi hostami. Obecna implementacja tych ograniczeń nie zapewnia.
