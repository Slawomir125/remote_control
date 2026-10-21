"""Czyste funkcje: ocena odpowiedzi, okna czasowe. Bez stanu, bez I/O."""


def rowne(a, b):
    if type(a) is int and type(b) is int:
        return a == b
    try:
        return abs(float(a) - float(b)) < 1e-9
    except (TypeError, ValueError):
        return False


def ocen_odpowiedz(zadanie, dane, teraz):
    """Porównuje odpowiedź Slave z zadaniem w toku.
    Zwraca (werdykt, szczegoly): werdykt = None (pierwsza prośba),
    "ok", "zle" albo "nieaktualne" (odpowiedź na inne zadanie)."""
    odp = dane.get("odpowiedz")
    if odp is None:
        return None, {}
    if not zadanie or dane.get("zadanie_id") != zadanie["id"]:
        return "nieaktualne", {}
    ok = rowne(odp, zadanie["wynik"])
    return ("ok" if ok else "zle"), {"ok": ok, "czas": teraz - zadanie["t0"], "odp_txt": str(odp)[:30]}


# ─── okna czasowe ────────────────────────────────────────────────────────────
# Liczenie "ile w ostatniej minucie" bez trzymania całej historii w pamięci.

def przytnij_okno(timestampy, teraz, okno=60):
    """Usuwa z początku kolejki znaczniki starsze niż `okno` s."""
    while timestampy and teraz - timestampy[0] > okno:
        timestampy.popleft()


def ile_w_oknie(timestampy, teraz, okno=60):
    """Ile znaczników mieści się w ostatnich `okno` s."""
    n = 0
    for t in reversed(timestampy):
        if teraz - t > okno:
            break
        n += 1
    return n


def zlicz_sekunde(sekundy, teraz, zachowaj=120):
    """Globalny licznik tempa: kolejka [sekunda, ile]. Pamięć stała."""
    s = int(teraz)
    if sekundy and sekundy[-1][0] == s:
        sekundy[-1][1] += 1
    else:
        sekundy.append([s, 1])
    while sekundy and sekundy[0][0] < s - zachowaj:
        sekundy.popleft()


def seria_tempa(sekundy, teraz, okno=60):
    """Ile zadań ukończono w każdej z ostatnich `okno` sekund (najstarsza pierwsza)."""
    seria, s = [0] * okno, int(teraz)
    for sek, ile in reversed(sekundy):
        wiek = s - sek
        if wiek >= okno:
            break
        if wiek >= 0:
            seria[okno - 1 - wiek] += ile
    return seria
