"""Serwer HTTP dla Slave'ów (asyncio) + raporter zbiorczy."""

import asyncio
import hmac
import json
import threading
import time
import urllib.parse
from collections import Counter

from master.config import (
    HOST, PORT, MAX_LINIA, BACKLOG, CISZA_ROZLACZ_S, RAPORT_CO_S, typ_pakietu,
)
from master.stan import log
from master.logika import zarejestruj, przetworz_prace, odrzuc, token_jednorazowy_zuzyj


# ─── obsługa pakietów od Slave'ów ───────────────────────────────────────────

@typ_pakietu("rejestracja", anonimowy=True)
def pakiet_rejestracja(stan, slave_id, dane):
    nazwa = str(dane.get("nazwa", "?"))[:12]
    nowe_id, token = zarejestruj(stan, nazwa)
    return {"id": nowe_id, "token": token}


@typ_pakietu("praca")
def pakiet_praca(stan, slave_id, dane):
    return przetworz_prace(stan, slave_id, dane)


def token_rowny(a, b):
    return hmac.compare_digest(str(a).encode("utf-8"), str(b).encode("utf-8"))


def obsluz_zadanie_http(stan, sciezka, authorization, dane):
    """Obsługuje endpointy rejestracji i pracy; TLS zapewnia Cloudflare Tunnel/proxy."""
    token = authorization.removeprefix("Bearer ").strip() if authorization.startswith("Bearer ") else ""
    if sciezka == "/api/slave/register":
        if token_rowny(token, stan.token_rejestracji):
            return 200, pakiet_rejestracja(stan, "0", dane)
        if token and token_jednorazowy_zuzyj(stan, token):
            return 200, pakiet_rejestracja(stan, "0", dane)
        return 401, {"blad": "nieprawidłowy token rejestracji"}

    prefix = "/api/slave/"
    if not sciezka.startswith(prefix):
        return 404, {"blad": "nie znaleziono"}
    slave_id = urllib.parse.unquote(sciezka[len(prefix):]).strip()
    with stan.lock:
        slave = stan.slaves.get(slave_id)
        if slave is None:
            odrzuc(stan, "nieznane ID")
            return 401, {"blad": "nieznany Slave"}
        if not token_rowny(token, slave.token):
            odrzuc(stan, "zły token Slaves")
            return 401, {"blad": "nieprawidłowy token Slaves"}
        if not stan.pracuje:
            return 503, {"blad": "Master wstrzymany"}
    if not isinstance(dane, dict) or dane.get("typ") != "praca":
        return 400, {"blad": "nieprawidłowy pakiet pracy"}
    return 200, pakiet_praca(stan, slave_id, dane)


# ─── serwer asyncio ─────────────────────────────────────────────────────────

async def obsluz_polaczenie(stan, reader, writer):
    stan.polaczenia += 1
    status, response = 400, {"blad": "nieprawidłowe żądanie"}
    try:
        request_line = await asyncio.wait_for(reader.readline(), CISZA_ROZLACZ_S)
        method, target, _ = request_line.decode("ascii").strip().split(" ", 2)
        naglowki = {}
        rozmiar_naglowkow = 0
        while True:
            line = await asyncio.wait_for(reader.readline(), CISZA_ROZLACZ_S)
            rozmiar_naglowkow += len(line)
            if rozmiar_naglowkow > 65536:
                status, response = 431, {"blad": "nagłówki za duże"}
                break
            if line in (b"\r\n", b"\n", b""):
                break
            key, separator, value = line.decode("latin-1").partition(":")
            if separator:
                naglowki[key.strip().lower()] = value.strip()
        if status == 400:
            if method != "POST":
                status, response = 405, {"blad": "wymagana metoda POST"}
            elif naglowki.get("transfer-encoding", "").lower() == "chunked":
                status, response = 400, {"blad": "chunked nieobsługiwany"}
            else:
                dl = int(naglowki.get("content-length", "0"))
                if dl <= 0 or dl > MAX_LINIA:
                    status, response = 413, {"blad": "nieprawidłowy rozmiar żądania"}
                else:
                    body = await asyncio.wait_for(reader.readexactly(dl), CISZA_ROZLACZ_S)
                    dane = json.loads(body)
                    if not isinstance(dane, dict):
                        status, response = 400, {"blad": "oczekiwano obiektu JSON"}
                    else:
                        status, response = obsluz_zadanie_http(
                            stan, urllib.parse.urlsplit(target).path,
                            naglowki.get("authorization", ""), dane,
                        )
    except (asyncio.TimeoutError, asyncio.IncompleteReadError, UnicodeError, ValueError, json.JSONDecodeError):
        status, response = 400, {"blad": "nieprawidłowe żądanie HTTP/JSON"}
    except (ConnectionError, OSError):
        pass
    except Exception as e:
        log(stan, f"Błąd obsługi żądania Slaves: {e}", True)
        status, response = 500, {"blad": "wewnętrzny błąd Master"}
    finally:
        stan.polaczenia -= 1
        try:
            body = json.dumps(response, ensure_ascii=False).encode("utf-8")
            reason = {200: "OK", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found",
                      405: "Method Not Allowed", 413: "Payload Too Large",
                      431: "Request Header Fields Too Large",
                      500: "Internal Server Error",
                      503: "Service Unavailable"}.get(status, "Error")
            writer.write(f"HTTP/1.1 {status} {reason}\r\n".encode("ascii"))
            writer.write(b"Content-Type: application/json; charset=utf-8\r\n")
            writer.write(f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode("ascii"))
            writer.write(body)
            await writer.drain()
            writer.close()
        except Exception:
            pass


def uruchom_serwer_slaves(stan):
    """Startuje endpoint HTTP w osobnym wątku z własną pętlą asyncio."""
    petla = asyncio.new_event_loop()
    gotowe, wynik = threading.Event(), {}

    def dzialaj():
        asyncio.set_event_loop(petla)
        try:
            wynik["serwer"] = petla.run_until_complete(asyncio.start_server(
                lambda r, w: obsluz_polaczenie(stan, r, w), HOST, PORT,
                backlog=BACKLOG, limit=MAX_LINIA))
        except Exception as e:
            wynik["blad"] = e
            gotowe.set()
            return
        gotowe.set()
        petla.run_forever()
        wynik["serwer"].close()

    threading.Thread(target=dzialaj, daemon=True).start()
    gotowe.wait()
    if "blad" in wynik:
        raise wynik["blad"]

    async def koniec():
        wynik["serwer"].close()
        zadania = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for t in zadania:
            t.cancel()
        await asyncio.gather(*zadania, return_exceptions=True)
        petla.stop()

    def zatrzymaj():
        try:
            asyncio.run_coroutine_threadsafe(koniec(), petla).result(timeout=5)
        except Exception:
            pass
    return zatrzymaj


def podnies_limit_plikow():
    """Każde połączenie to deskryptor pliku. Podnosi miękki limit (ulimit -n)."""
    try:
        import resource
    except ImportError:                             # Windows
        return None
    miekki, twardy = resource.getrlimit(resource.RLIMIT_NOFILE)
    gora = 1_048_576 if twardy == resource.RLIM_INFINITY else twardy
    for cel in (gora, 1_048_576, 262_144, 65_536, 32_768, 10_240):
        if miekki < cel <= gora:
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (cel, twardy))
                return cel
            except (ValueError, OSError):
                continue
    return miekki


def raporter(stan):
    """Co RAPORT_CO_S zbiera rejestracje i odrzucone pakiety w jedną linię."""
    while True:
        time.sleep(RAPORT_CO_S)
        with stan.lock:
            nowi, stan.nowi = stan.nowi, []
            odrzucone, stan.odrzucone = stan.odrzucone, Counter()
            razem = len(stan.slaves)
        if 0 < len(nowi) <= 5:
            for slave_id, nazwa in nowi:
                log(stan, f"+ #{slave_id} {nazwa} zarejestrowany", True)
        elif nowi:
            log(stan, f"+ {len(nowi)} nowych Slaves (razem {razem})", True)
        if odrzucone:
            log(stan, "! odrzucone pakiety: " + ", ".join(f"{p}: {n}" for p, n in odrzucone.items()), True)
