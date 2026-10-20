"""Strona /krzycz - przyjmuje krzyki (POST) i wyświetla listę (GET, odświeżana co 2s).

Uruchom:  python strona.py  [port]      (domyślnie 8080)
"""
import html
import json
import sys
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

KRZYKI = []
LOCK = threading.Lock()


def render() -> bytes:
    with LOCK:
        wiersze = "".join(
            f"<tr><td>{html.escape(k['czas'])}</td><td>#{html.escape(str(k['id']))}</td>"
            f"<td>{html.escape(k['nazwa'])}</td><td>{html.escape(k['wiadomosc'])}</td></tr>"
            for k in reversed(KRZYKI)
        ) or "<tr><td colspan=4>Cisza... Slaves jeszcze nie krzyczą.</td></tr>"
    return f"""<!doctype html><html lang="pl"><head><meta charset="utf-8">
<meta http-equiv="refresh" content="2"><title>Krzyki Slaves</title>
<style>body{{font-family:sans-serif;background:#111;color:#eee;margin:2rem}}
table{{border-collapse:collapse;width:100%}}td,th{{border-bottom:1px solid #333;padding:.5rem;text-align:left}}
th{{color:#fc0}}</style></head><body><h1>📣 Krzyki Slaves ({len(KRZYKI)})</h1>
<table><tr><th>Czas</th><th>ID</th><th>Slave</th><th>Wiadomość</th></tr>{wiersze}</table>
</body></html>""".encode()


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.split("?")[0] not in ("/", "/krzycz"):
            self.send_error(404)
            return
        tresc = render()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(tresc)))
        self.end_headers()
        self.wfile.write(tresc)

    def do_POST(self):
        if self.path != "/krzycz":
            self.send_error(404)
            return
        try:
            n = int(self.headers.get("Content-Length", 0))
            if not 0 < n <= 10_000:
                raise ValueError
            d = json.loads(self.rfile.read(n))
            wpis = {
                "czas": datetime.now().strftime("%H:%M:%S"),
                "id": str(d.get("id", "?"))[:20],
                "nazwa": str(d.get("nazwa", "?"))[:50],
                "wiadomosc": str(d["wiadomosc"])[:1000],
            }
        except (ValueError, KeyError, json.JSONDecodeError):
            self.send_error(400)
            return
        with LOCK:
            KRZYKI.append(wpis)
        print(f"[/krzycz] #{wpis['id']} {wpis['nazwa']}: {wpis['wiadomosc']}")
        self.send_response(201)
        self.send_header("Content-Length", "0")
        self.end_headers()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    print(f"Strona: http://0.0.0.0:{port}/krzycz")
    ThreadingHTTPServer(("0.0.0.0", port), H).serve_forever()
