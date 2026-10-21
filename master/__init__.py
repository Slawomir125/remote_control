"""Master v7 — pakiet koordynatora.

Punkty rozbudowy (rejestry wypełniane dekoratorami):

    OPERACJE.add("%")                 nowy symbol działania (dodaj też w slave.py)
    @typ_pakietu("telemetria")        nowy typ wiadomości od Slaves
    @komenda("nazwa", "opis")         nowa komenda w konsoli Master
    @endpoint("/api/cos")             nowy adres w panelu (HTTP)
    POLECENIA_SLAVES["nazwa"] = ...   nowe polecenie dla Slaves (+ @polecenie w slave.py)
"""
