# Loxone MQTT Subscriber & Publisher

Zwei eigenständige **Programmierbausteine** (User-Defined Function Blocks) für
Loxone Config, mit denen sich ein MQTT-Broker per rohem TCP-Socket ansprechen
lässt – ganz ohne Zusatz-Hardware oder Miniserver-Erweiterung. Getestet gegen
MQTT 3.1.1 (Mosquitto).

- `mqtt_subscriber.c` – abonniert mehrere Topics gleichzeitig (inkl. Wildcards)
  und gibt empfangene Werte als Text aus.
- `mqtt_publisher.c` – sendet auf Trigger ein Payload an ein Topic (QoS 0/1/2,
  Retain).

Beide Bausteine sind bewusst als eine einzige `.c`-Datei gehalten, wie es
Loxone Config für Programmierbausteine erwartet, und kommen ohne externe
Libraries aus.

> ⚠️ **Kein offizieller Loxone-Baustein.** Das Ganze basiert auf rohem
> `stream_create`/`stream_read`/`stream_write` und einer minimalen, selbst
> gebauten MQTT-Implementierung. Es deckt das ab, was für einfache
> Sensor-/Steuer-Topics nötig ist (CONNECT, SUBSCRIBE, PUBLISH inkl. QoS 0–2,
> PING), aber keinen vollständigen MQTT-Stack (z.B. kein TLS, kein Will-Message,
> keine Session-Persistenz).

## Voraussetzungen

- Loxone Config (mit Unterstützung für Programmierbausteine / `STREAM`-Funktionen)
- Ein Miniserver, der ausgehende TCP-Verbindungen zum Broker zulässt
- Ein erreichbarer MQTT-Broker mit Benutzername/Passwort-Auth (z.B. Mosquitto)
- Kein TLS/MQTTS – die Skripte sprechen unverschlüsseltes MQTT auf Port 1883.
  Für eine Verbindung übers Internet daher nur per VPN, nicht direkt exponieren.

## Installation in Loxone Config

1. Im Loxone Config-Baumeditor einen **Programmierbaustein** anlegen (bzw. den
   Editor für einen bestehenden Baustein öffnen).
2. Den Inhalt von `mqtt_subscriber.c` bzw. `mqtt_publisher.c` in den Code-Editor
   des Bausteins einfügen.
3. Am Kopf der Datei die Konfigurationskonstanten anpassen:

   ```c
   #define BROKER_IP   "192.168.1.10"
   #define BROKER_PORT 1883
   #define CLIENT_ID   "Loxone_Subscriber"   // im Netzwerk eindeutig halten!
   #define MQTT_USER   "mqtt_user"
   #define MQTT_PASS   "mqtt_password"
   ```

   Für Subscriber und Publisher unbedingt unterschiedliche `CLIENT_ID` vergeben
   – die meisten Broker trennen bei doppelter Client-ID die ältere Verbindung.
4. Baustein kompilieren/übernehmen und die Ein-/Ausgänge im Baumeditor mit den
   gewünschten Loxone-Objekten (virtuelle Ein-/Ausgänge, Schalter, Textbausteine
   etc.) verbinden – siehe Tabellen unten.
5. Programm auf den Miniserver übertragen.

## Subscriber – Ein-/Ausgänge

| I/O | Typ    | Bedeutung |
|-----|--------|-----------|
| I1  | Digital | Start-Trigger (steigende Flanke startet die Verbindung) |
| I13 | Digital | Enable (1 = aktiv, 0 = trennt die Verbindung) |
| T1  | Text    | Topic-Liste, siehe Format unten |
| O1  | Digital | Heartbeat, togglet ~alle 5 s solange I13=1 |
| O2  | Digital | 1 = mit Broker verbunden, 0 = sonst |
| T1  | Text    | Statustext |
| T2  | Text    | empfangene Werte |
| T3  | Text    | Diagnose (Topic-Parsing, SUBACK-Codes) |

**Topic-Liste (Eingang T1), Format:**

```
wohnzimmer/temperatur;kueche/temperatur|1;garten/sensoren/#|1
```

- `;` trennt einzelne Topics
- `|1` bzw. `|2` direkt nach einem Topic setzt dessen QoS (Default: QoS 0)
- Ein Topic, das mit `#` endet, wird als Wildcard behandelt

**Empfangene Werte (Ausgang T2), Format:**

```
S1:23.4;S3:19.8;
```

`S<n>` bezieht sich auf die **Position** des Topics in der Eingabeliste
(1-basiert), nicht auf den Topic-Namen selbst. Pro Topic-Index empfiehlt sich
in Loxone ein eigener Textbaustein/eine Regex, um `S<n>:...;` wieder in den
reinen Wert zu zerlegen.

**Diagnose (Ausgang T3):** Direkt nach dem Start zeigt T3 z.B.

```
InLen=87 Count=3 Last[2]=garten/sensoren/#
```

Nach Eintreffen aller SUBACK-Antworten wird T3 einmalig überschrieben mit
einer Zusammenfassung je Topic-Index:

```
T1=0 T2=1 T3=128
```

`0`/`1`/`2` = vom Broker akzeptiert mit der jeweiligen QoS, `128` = vom Broker
abgelehnt (z.B. fehlende ACL-Berechtigung für dieses Topic).

## Publisher – Ein-/Ausgänge

| I/O | Typ    | Bedeutung |
|-----|--------|-----------|
| I1  | Digital | Trigger (steigende Flanke löst einen Publish aus) |
| I2  | Analog  | QoS (0, 1 oder 2) |
| I3  | Analog  | Retain-Flag (0 oder 1) |
| I13 | Digital | Enable |
| T1  | Text    | Ziel-Topic |
| T2  | Text    | Payload (wird 1:1 gesendet) |
| AQ1 | Digital | Heartbeat, togglet ~alle 5 s solange I13=1 |
| AQ2 | Digital | 1 = mit Broker verbunden, 0 = sonst |
| TQ1 | Text    | Status-/Fehlertext |

Ablauf: T1 (Topic) und T2 (Payload) vor dem Trigger setzen, dann I1 kurz auf 1
pulsen. Ein neuer Publish wird nur bei einer **steigenden Flanke** auf I1
ausgelöst.

## Verbindungsverhalten (beide Bausteine)

- **Reconnect automatisch:** Der Subscriber merkt sich nach dem ersten
  Start-Trigger nur "Verbindung gewünscht" und verbindet danach immer wieder
  automatisch neu, sobald keine Verbindung besteht – auch nach einem
  Broker-Neustart, ohne dass I1 erneut gepulst werden muss. Der Publisher
  verbindet ohnehin bei jedem Zyklus neu, wenn `pMqttStream == NULL` ist.
- **PINGRESP-Watchdog (nur Subscriber):** Kommt innerhalb eines Ping-Zyklus
  keine Antwort vom Broker, gilt die Verbindung als tot und wird aktiv
  geschlossen – nötig, weil ein sauberes Schließen durch den Broker vom
  Loxone-Stream nicht immer als Lesefehler erkannt wird.
- **O2/AQ2** sind stabile 1/0-Signale ("verbunden ja/nein"), keine Pulse –
  lassen sich direkt an einen Status- oder Alarmbaustein anschließen.

## Bekannte Einschränkungen

- Kein TLS (MQTTS), keine Client-Zertifikate.
- Kein "Last Will and Testament".
- Maximal `MAX_TOPICS` (Subscriber, Default 16) Topics gleichzeitig.
- Payload-/Topic-Puffer sind fest dimensioniert (siehe `buf[1024]`,
  `values[512]`); sehr große Payloads werden abgeschnitten bzw. das
  entsprechende Paket wird verworfen, statt den Baustein zum Absturz zu
  bringen.
- Wildcard-Unterstützung nur für ein `#` am Ende eines Topics, kein `+`.

## Lizenz

Kein Lizenz-Header enthalten – ergänzt selbst eine Lizenz eurer Wahl (z.B.
MIT), bevor ihr das Repository veröffentlicht.
