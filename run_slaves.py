"""Uruchamia N Slaves naraz (symulacja).

    python run_slaves.py 5 --domena master.example.com
    python run_slaves.py 5000 --watki --cicho --domena master.example.com
Opcje: --master host[:port]  --domena domena  --https/--no-https  --token-rejestracji token.
"""
import argparse
import importlib.util
import subprocess
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit

ap = argparse.ArgumentParser()
ap.add_argument("n", nargs="?", type=int, default=3, help="ile Slaves")
ap.add_argument("--master", default="localhost")
ap.add_argument("--domena", help="publiczna domena Master")
ap.add_argument("--port", type=int, help="port HTTP(S); domyślnie 443/80 lub port z --master")
ap.add_argument("--https", action=argparse.BooleanOptionalAction,
                default=True)
ap.add_argument("--token-rejestracji")
ap.add_argument("--opoznienie", type=float, default=1.0)
ap.add_argument("--watki", action="store_true", help="wątki zamiast procesów (lżejsze przy setkach)")
ap.add_argument("--cicho", action="store_true")
a = ap.parse_args()

katalog = Path(__file__).parent
parsed = urlsplit("//" + a.master)
host = parsed.hostname or a.master
domena = a.domena or host
port = a.port or (None if a.domena else parsed.port) or (443 if a.https else 80)

if a.watki:
    spec = importlib.util.spec_from_file_location("slave_worker", katalog / "slave.py")
    slave = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = slave
    spec.loader.exec_module(slave)

    threading.stack_size(256 * 1024)             # mały stos: 20 000 wątków mieści się w pamięci
    stop = threading.Event()
    watki = [threading.Thread(target=slave.praca, daemon=True,
                        args=(f"slave{i}", host, port, a.opoznienie, a.cicho, stop,
                            a.token_rejestracji, domena, a.https))
             for i in range(1, a.n + 1)]
    for w in watki:
        w.start()
        time.sleep(0.002)                        # lekkie rozłożenie rejestracji
    print(f"Uruchomiono {a.n} Slaves (wątki). Ctrl+C kończy.", flush=True)
    try:
        while any(w.is_alive() for w in watki):
            time.sleep(0.5)
    except KeyboardInterrupt:
        stop.set()
        for w in watki:
            w.join(timeout=3)
        print(f"Zatrzymano {a.n} Slaves.", flush=True)
else:
    dodatkowe = (["--cicho"] if a.cicho else [])
    if a.domena:
        dodatkowe += ["--domena", a.domena]
    if a.port:
        dodatkowe += ["--port", str(a.port)]
    dodatkowe += ["--https" if a.https else "--no-https"]
    if a.token_rejestracji:
        dodatkowe += ["--token-rejestracji", a.token_rejestracji]
    procesy = [
        subprocess.Popen([sys.executable, str(katalog / "slave.py"), "--nazwa", f"slave{i}",
                          "--master", a.master, "--opoznienie", str(a.opoznienie), *dodatkowe])
        for i in range(1, a.n + 1)
    ]
    try:
        for p in procesy:
            p.wait()
    except KeyboardInterrupt:
        for p in procesy:
            p.terminate()
