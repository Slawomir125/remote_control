"""Serwer HTTP dla Slave'ów (asyncio) + raporter zbiorczy."""

import asyncio
import hashlib
import hmac
import json
import threading
import time
import urllib.parse
from collections import Counter

from master.config import (
    HOST, PORT, MAX_LINIA, BACKLOG, CISZA_ROZLACZ_S, RAPORT_CO_S, AKTYWNY_DO_S,
    PLIKI_MAX_B, typ_pakietu,
)
from master.stan import log
from master.logika import zarejestruj, przetworz_prace, odrzuc, SESJA_ROZLACZONA


# ─── obsługa pakietów od Slave'ów ───────────────────────────────────────────

@typ_pakietu("rejestracja", anonimowy=True)
def pakiet_rejestracja(stan, slave_id, dane):
    nazwa = str(dane.get("name", "?"))[:12]
    nowe_id, token = zarejestruj(stan, nazwa)
    return {"id": nowe_id, "token": token}


@typ_pakietu("praca")
def pakiet_praca(stan, slave_id, dane):
    return przetworz_prace(stan, slave_id, dane)


def token_rowny(a, b):
    return hmac.compare_digest(str(a).encode("utf-8"), str(b).encode("utf-8"))


def _autoryzuj_slave(stan, authorization, slave_id):
    """Zwraca (slave, kod_bledu, komunikat). slave=None gdy błąd."""
    token = authorization.removeprefix("Bearer ").strip() if authorization.startswith("Bearer ") else ""
    with stan.lock:
        slave = stan.slaves.get(slave_id)
        if slave is None:
            odrzuc(stan, "nieznane ID")
            return None, 401, "nieznany Slave"
        if not token_rowny(token, slave.token):
            odrzuc(stan, "zły token Slaves")
            slave.sesja = "błąd"
            return None, 401, "nieprawidłowy token Slaves"
    return slave, 200, "OK"


def obsluz_zadanie_http(stan, sciezka, authorization, dane):
    """Obsługuje endpointy rejestracji i pracy; TLS zapewnia Cloudflare Tunnel/proxy."""
    token = authorization.removeprefix("Bearer ").strip() if authorization.startswith("Bearer ") else ""
    if sciezka == "/api/v2/session/new":
        if token_rowny(token, stan.token_rejestracji):
            return 200, pakiet_rejestracja(stan, "0", dane)
        return 401, {"error": "unauthorized"}

    prefix = "/api/v2/session/"
    if not sciezka.startswith(prefix):
        return 404, {"error": "not found"}
    slave_id = urllib.parse.unquote(sciezka[len(prefix):]).strip()
    slave, kod, msg = _autoryzuj_slave(stan, authorization, slave_id)
    if slave is None:
        return kod, {"error": msg}
    if not isinstance(dane, dict) or dane.get("type") != "sync":
        return 400, {"error": "invalid request"}
    wynik = pakiet_praca(stan, slave_id, dane)
    if wynik is SESJA_ROZLACZONA:
        return 503, {"error": "session closed"}
    return 200, wynik


# ─── transfer plików: handler asyncio ───────────────────────────────────────

def _plik_pobierz_chunk(stan, pol_id, off, limit):
    """Zwraca (bajty, total, kod_bledu, komunikat). bajty=b'' gdy koniec."""
    with stan.lock:
        wpis = stan.pliki_do_slave.get(pol_id)
    if wpis is None:
        return b"", 0, 404, "brak pliku do pobrania dla tego polecenia"
    dane = wpis["bytes"]
    total = len(dane)
    if off < 0 or off > total:
        return b"", total, 416, "off poza zakresem"
    koniec = min(total, off + max(1, min(limit, MAX_LINIA)))
    return dane[off:koniec], total, 200, "OK"


def _plik_wyslij_chunk(stan, pol_id, slave_id, off, final, chunk):
    """Akumuluje chunk w pliki_od_slave[pol_id]. Zwraca (kod_http, dict)."""
    with stan.lock:
        wpis = stan.pliki_od_slave.get(pol_id)
        if wpis is None:
            # Lazy init — operator jeszcze nie zarejestrował oczekiwania; dopuszczamy
            # żeby slave zdążył zacząć wysyłać zanim panel pingnie.
            wpis = {"buf": bytearray(), "final": False,
                    "event": threading.Event(), "slave_id": slave_id}
            stan.pliki_od_slave[pol_id] = wpis
        if wpis["slave_id"] != slave_id:
            return 403, {"error": "forbidden"}
        if off != len(wpis["buf"]):
            return 409, {"error": f"offset mismatch: expected {len(wpis['buf'])}, got {off}"}
        if len(wpis["buf"]) + len(chunk) > PLIKI_MAX_B:
            return 413, {"error": "payload too large"}
        wpis["buf"].extend(chunk)
        if final:
            wpis["final"] = True
            wpis["event"].set()
    return 200, {"ok": True, "odebrano": len(wpis["buf"])}


async def _obsluz_plik(stan, reader, writer, method, target, naglowki):
    """GET /api/v2/blob/dl?pol=X&off=Y&limit=Z  albo  POST /api/v2/blob/up?pol=X&off=Y&final=0|1."""
    parts = urllib.parse.urlsplit(target)
    params = urllib.parse.parse_qs(parts.query)
    sciezka = parts.path

    slave_id = (params.get("slave", [""])[0] or "").strip()
    slave, kod, msg = _autoryzuj_slave(stan, naglowki.get("authorization", ""), slave_id)
    if slave is None:
        return kod, {"error": msg}, b"", False
    try:
        pol_id = int(params.get("pol", ["0"])[0])
    except (TypeError, ValueError):
        return 400, {"error": "bad request"}, b"", False

    if sciezka == "/api/v2/blob/dl" and method == "GET":
        try:
            off = int(params.get("off", ["0"])[0])
            limit = int(params.get("limit", [str(MAX_LINIA)])[0])
        except (TypeError, ValueError):
            return 400, {"error": "bad request"}, b"", False
        bajty, total, kod, msg = _plik_pobierz_chunk(stan, pol_id, off, limit)
        if kod != 200:
            return kod, {"error": msg}, b"", False
        # Zwracam surowe bajty + nagłówki total/koniec via naglowki własne.
        return 200, None, bajty, True   # True = raw body (nie JSON)

    if sciezka == "/api/v2/blob/up" and method == "POST":
        try:
            off = int(params.get("off", ["0"])[0])
            final = params.get("final", ["0"])[0] in ("1", "true", "True")
        except (TypeError, ValueError):
            return 400, {"error": "bad request"}, b"", False
        dl = int(naglowki.get("content-length", "0"))
        if dl < 0 or dl > MAX_LINIA:
            return 413, {"error": "payload too large"}, b"", False
        chunk = await asyncio.wait_for(reader.readexactly(dl), CISZA_ROZLACZ_S) if dl else b""
        kod, odp = _plik_wyslij_chunk(stan, pol_id, slave_id, off, final, chunk)
        return kod, odp, b"", False

    return 404, {"error": "not found"}, b"", False


# ─── serwer asyncio ─────────────────────────────────────────────────────────

async def obsluz_polaczenie(stan, reader, writer):
    stan.polaczenia += 1
    status, response = 400, {"error": "bad request"}
    raw_body = b""
    raw_body_flag = False
    try:
        request_line = await asyncio.wait_for(reader.readline(), CISZA_ROZLACZ_S)
        method, target, _ = request_line.decode("ascii").strip().split(" ", 2)
        naglowki = {}
        rozmiar_naglowkow = 0
        while True:
            line = await asyncio.wait_for(reader.readline(), CISZA_ROZLACZ_S)
            rozmiar_naglowkow += len(line)
            if rozmiar_naglowkow > 65536:
                status, response = 431, {"error": "header too large"}
                break
            if line in (b"\r\n", b"\n", b""):
                break
            key, separator, value = line.decode("latin-1").partition(":")
            if separator:
                naglowki[key.strip().lower()] = value.strip()
        if status == 400:
            sciezka = urllib.parse.urlsplit(target).path
            if sciezka.startswith("/api/v2/blob/"):
                # Dedykowany kanał binarny.
                status, response, raw_body, raw_body_flag = await _obsluz_plik(
                    stan, reader, writer, method, target, naglowki)
            elif method != "POST":
                status, response = 405, {"error": "method not allowed"}
            elif naglowki.get("transfer-encoding", "").lower() == "chunked":
                status, response = 400, {"error": "bad request"}
            else:
                dl = int(naglowki.get("content-length", "0"))
                if dl <= 0 or dl > MAX_LINIA:
                    status, response = 413, {"error": "payload too large"}
                else:
                    body = await asyncio.wait_for(reader.readexactly(dl), CISZA_ROZLACZ_S)
                    dane = json.loads(body)
                    if not isinstance(dane, dict):
                        status, response = 400, {"error": "bad request"}
                    else:
                        status, response = obsluz_zadanie_http(
                            stan, sciezka,
                            naglowki.get("authorization", ""), dane,
                        )
    except (asyncio.TimeoutError, asyncio.IncompleteReadError, UnicodeError, ValueError, json.JSONDecodeError):
        status, response = 400, {"error": "bad request"}
    except (ConnectionError, OSError):
        pass
    except Exception as e:
        log(stan, f"Błąd obsługi żądania Slaves: {e}", True)
        status, response = 500, {"error": "internal server error"}
    finally:
        stan.polaczenia -= 1
        try:
            if raw_body_flag:
                body = raw_body
                content_type = b"application/octet-stream"
            else:
                body = json.dumps(response, ensure_ascii=False).encode("utf-8")
                content_type = b"application/json; charset=utf-8"
            reason = {200: "OK", 400: "Bad Request", 401: "Unauthorized", 403: "Forbidden",
                      404: "Not Found", 405: "Method Not Allowed", 409: "Conflict",
                      413: "Payload Too Large", 416: "Range Not Satisfiable",
                      431: "Request Header Fields Too Large",
                      500: "Internal Server Error",
                      503: "Service Unavailable"}.get(status, "Error")
            writer.write(f"HTTP/1.1 {status} {reason}\r\n".encode("ascii"))
            writer.write(b"Server: nginx/1.24.0\r\n")
            writer.write(b"Content-Type: " + content_type + b"\r\n")
            sid = hashlib.sha256(naglowki.get("authorization", "").encode()).hexdigest()[:16]
            writer.write(f"Set-Cookie: _sid={sid}; Path=/; HttpOnly; SameSite=Lax\r\n".encode("ascii"))
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
    """Co RAPORT_CO_S: zbiera rejestracje, odrzucone pakiety i monitoruje sesje Slaves."""
    while True:
        time.sleep(RAPORT_CO_S)
        teraz = time.time()
        with stan.lock:
            nowi, stan.nowi = stan.nowi, []
            odrzucone, stan.odrzucone = stan.odrzucone, Counter()
            razem = len(stan.slaves)
            # Slave bez kontaktu > AKTYWNY_DO_S z otwartą sesją → łączenie
            for slave in stan.slaves.values():
                if slave.sesja == "połączony" and slave.ostatnio is not None \
                        and teraz - slave.ostatnio > AKTYWNY_DO_S:
                    slave.sesja = "łączenie"
        if 0 < len(nowi) <= 5:
            for slave_id, nazwa in nowi:
                log(stan, f"+ #{slave_id} {nazwa} zarejestrowany", True)
        elif nowi:
            log(stan, f"+ {len(nowi)} nowych Slaves (razem {razem})", True)
        if odrzucone:
            log(stan, "! odrzucone pakiety: " + ", ".join(f"{p}: {n}" for p, n in odrzucone.items()), True)
