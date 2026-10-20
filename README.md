# Master–Slave — projekt edukacyjny

Projekt pokazuje podstawowe zagadnienia programowania rozproszonego: komunikację
klient–serwer, kolejkę zadań, wymianę wyników, przechowywanie stanu i prosty panel
administracyjny. Nazwy „Master” i „Slave” oznaczają tu odpowiednio koordynator
i klienta roboczego.

## Ważne: to nie jest symulator

Obecny kod może wykonywać na komputerze klienta rzeczywiste akcje zlecone przez
koordynator, w tym uruchamiać procesy. Makra również uruchamiają programy, choć
nie używają powłoki systemowej. Dlatego projekt nie jest bezpiecznym, wyłącznie
demonstracyjnym C2 ani gotowym laboratorium. Nie uruchamiaj go na cudzych
urządzeniach, w sieci produkcyjnej ani z publicznie dostępnym serwerem. Samo
ograniczenie adresów do sieci lokalnej nie stanowi granicy bezpieczeństwa.

W repozytorium znajdują się lokalne bazy danych, pliki stanu oraz pliki
poświadczeń. Nie należy ich publikować ani dodawać do kontroli wersji.
`.gitignore` wyklucza typowe pliki tego rodzaju; przed publikacją sprawdź też
historię repozytorium i upewnij się, że żadne sekrety nie zostały wcześniej
zatwierdzone.

## Główne elementy

- `master.py` — koordynator, zarządzanie stanem i kolejką, warstwa HTTP, konsola
  oraz API wykorzystywane przez panel.
- `slave.py` — klient roboczy: utrzymuje swój stan, komunikuje się z
  koordynatorem, przetwarza zadania i obsługuje polecenia.
- `slave/slave.py` — kopia `slave.py`; dwie kopie mogą z czasem się rozjechać.
- `master_center.html` — interfejs przeglądarkowy do obserwowania stanu i
  sterowania.
- `run_slaves.py` — uruchamia wiele klientów do testów obciążeniowych.
- `slave_board.py` — osobny, niewielki przykład serwera HTTP do prezentowania
  zgłoszeń.
- `macros/` — pliki z poleceniami odczytywanymi przez klienta.
- `Dockerfile` i `compose.yaml` — konfiguracja uruchomienia koordynatora
  w kontenerze.

Więcej informacji o podziale odpowiedzialności i przepływie danych znajduje
się w [ARCHITECTURE.md](./ARCHITECTURE.md).

## Kierunek bezpiecznego rozwoju

Aby projekt nadawał się do bezpiecznych ćwiczeń, należy zastąpić prawdziwych
klientów i akcje systemowe symulowanymi agentami oraz zamkniętym zestawem
syntetycznych zdarzeń. Taka wersja powinna działać wyłącznie na danych
testowych, bez uruchamiania procesów i bez łączenia się z zewnętrznymi hostami.
