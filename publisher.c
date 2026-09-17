/*
 * MQTT Publisher für Loxone (Programmierbaustein)
 * ==================================================
 *
 * Sendet auf eine steigende Flanke hin ein Text-Payload an ein MQTT-Topic
 * (mit optionalem QoS/Retain) und meldet Status und Verbindung zurück.
 *
 * INPUTS
 *   I1  (Index 0)  Digital - Trigger. Steigende Flanke löst genau einen
 *                  Publish mit dem aktuellen Inhalt von T1/T2 aus.
 *   I2  (Index 1)  Analog  - QoS (0, 1 oder 2).
 *   I3  (Index 2)  Analog  - Retain-Flag (0 oder 1).
 *   I13 (Index 12) Digital - Enable. 1 = aktiv, 0 = getrennt/inaktiv.
 *   T1  (Index 0)  Text - Ziel-Topic.
 *   T2  (Index 1)  Text - Payload (wird 1:1 als Text gesendet, kein
 *                  Parsing/Escaping durch dieses Modul).
 *
 * OUTPUTS
 *   AQ1 (Index 0) Digital - Heartbeat, togglet ca. alle 5s solange I13=1.
 *                 Zeigt "der Baustein läuft", unabhängig vom Broker.
 *   AQ2 (Index 1) Digital - Verbindungsstatus: 1 = aktuell mit Broker
 *                 verbunden, 0 = alles andere (nicht gestartet,
 *                 reconnecting, disabled).
 *   TQ1 (Index 0) Text - Status-/Fehlertext, u.a.:
 *                 "Connected to Broker", "Send OK",
 *                 "Error: Publish Write Failed", "Error: QoS ACK Timeout",
 *                 "Error: Topic missing", "Error: Payload too large",
 *                 "TCP Connection Error", "Connection Lost",
 *                 "Socket Read Error", "Disconnected (Enable Off)".
 *
 * FEATURES
 *   - QoS 0/1/2 inkl. vollständigem PUBACK/PUBREC/PUBREL/PUBCOMP-Handshake
 *     für QoS 1/2, mit einem Retry-Versuch (DUP-Flag) bei Timeout.
 *   - Erkennt einen fehlgeschlagenen stream_write() beim Publish sofort als
 *     Fehler (statt stillschweigend weiterzulaufen) und schließt/öffnet die
 *     Verbindung neu.
 *   - TQ1 wird bei jedem Übergang enabled -> disabled genau einmal gesetzt,
 *     unabhängig davon, ob zu diesem Zeitpunkt noch eine Verbindung bestand.
 *   - AQ1 (Heartbeat) läuft unabhängig vom Verbindungsstatus.
 *   - AQ2 ist ein stabiles Verbunden-Flag statt eines kurzen Pulses -
 *     direkt in einen Status-/Alarmbaustein einspeisbar.
 *
 * KONFIGURATION
 *   Siehe Konstanten unten. Bitte vor dem Einsatz an die eigene Umgebung
 *   anpassen. Aus Sicherheitsgründen keine echten Zugangsdaten in ein
 *   öffentliches Repository committen.
 */

#define BROKER_IP   "192.168.1.10"      // TODO: IP/Hostname eures MQTT-Brokers
#define BROKER_PORT 1883
#define CLIENT_ID   "Loxone_Publisher"  // TODO: im Netzwerk eindeutige Client-ID
#define MQTT_USER   "mqtt_user"         // TODO: MQTT-Benutzername
#define MQTT_PASS   "mqtt_password"     // TODO: MQTT-Passwort

STREAM* pMqttStream = NULL;
int lastPing = 0;
int packetIdCounter = 1;
int lastHeartbeat = -1;
float lastTrigger = 0.0;
char packetBuf[1024];
int lastEnaState = -1; // für den sauberen Disable-Text, unabhängig vom Verbindungsstatus
int lastConnFlag = -1; // für das stabile Verbunden-Flag auf AQ2

// --- HILFSFUNKTIONEN ---

void safe_mqtt_connect(STREAM* s) {
    int uL, pL, cL, rL;
    char h[12]; char head[2];
    uL = strlen(MQTT_USER); pL = strlen(MQTT_PASS); cL = strlen(CLIENT_ID);
    rL = 10 + 2 + cL + 2 + uL + 2 + pL;
    h[0]=0x10; h[1]=rL; h[2]=0x00; h[3]=0x04; h[4]='M'; h[5]='Q'; h[6]='T'; h[7]='T';
    h[8]=0x04; h[9]=0xC2; h[10]=0x00; h[11]=0x3C;
    stream_write(s, h, 12);
    head[0]=(cL>>8)&0xFF; head[1]=cL&0xFF; stream_write(s, head, 2); stream_write(s, CLIENT_ID, cL);
    head[0]=(uL>>8)&0xFF; head[1]=uL&0xFF; stream_write(s, head, 2); stream_write(s, MQTT_USER, uL);
    head[0]=(pL>>8)&0xFF; head[1]=pL&0xFF; stream_write(s, head, 2); stream_write(s, MQTT_PASS, pL);
    stream_flush(s);
}

int wait_for_ack(STREAM* s, int expectedType, int expectedId) {
    char r[4]; int timeout = 0; int rid;
    while (timeout < 50) {
        if (stream_read(s, r, 1, 10) > 0) {
            if ((r[0] & 0xF0) == (expectedType & 0xF0)) {
                stream_read(s, &r[1], 3, 50);
                rid = ((r[2] & 0xFF) << 8) | (r[3] & 0xFF);
                if (rid == expectedId) return 1;
            }
        }
        timeout = timeout + 1;
    }
    return 0;
}

// Gibt 1 bei Erfolg zurück, 0 bei Verbindungsabbruch/Fehler.
int safe_mqtt_publish(STREAM* s, char* topic, char* payload, int qos, int retain) {
    int tLen, pLen, remLen, curId, j, idx, attempt, success;
    tLen = strlen(topic);
    if (tLen == 0) { setoutputtext(0, "Error: Topic missing"); return 1; } // 1, da kein Socket-Fehler

    pLen = strlen(payload);
    remLen = 2 + tLen + pLen;
    curId = 0;
    if (qos > 0) {
        remLen = remLen + 2;
        packetIdCounter = packetIdCounter + 1;
        if (packetIdCounter > 65000) packetIdCounter = 1;
        curId = packetIdCounter;
    }

    if ((remLen + 5) > 1024) { setoutputtext(0, "Error: Payload too large"); return 1; } // 1, da kein Socket-Fehler

    attempt = 0; success = 0;
    while (attempt < 2 && success == 0) {
        idx = 0; packetBuf[idx] = 0x30;
        if (qos == 1) packetBuf[idx] = packetBuf[idx] | 0x02;
        if (qos == 2) packetBuf[idx] = packetBuf[idx] | 0x04;
        if (retain == 1) packetBuf[idx] = packetBuf[idx] | 0x01;
        if (attempt > 0) packetBuf[idx] = packetBuf[idx] | 0x08; // DUP-Flag beim Retry
        idx = idx + 1;
        if (remLen < 128) { packetBuf[idx++] = remLen; }
        else { packetBuf[idx++] = (remLen & 127) | 128; packetBuf[idx++] = (remLen >> 7); }
        packetBuf[idx++] = (tLen >> 8) & 0xFF; packetBuf[idx++] = tLen & 0xFF;
        for(j=0; j<tLen; j++) packetBuf[idx++] = topic[j];
        if (qos > 0) { packetBuf[idx++] = (curId >> 8) & 0xFF; packetBuf[idx++] = (curId & 0xFF); }
        for(j=0; j<pLen; j++) packetBuf[idx++] = payload[j];

        // Schreibfehler auf dem Socket sofort als Fehler behandeln, statt
        // stillschweigend weiterzulaufen.
        if (stream_write(s, packetBuf, idx) <= 0) {
            setoutputtext(0, "Error: Publish Write Failed");
            return 0;
        }
        stream_flush(s);

        if (qos == 0) { success = 1; }
        else if (qos == 1) { success = wait_for_ack(s, 0x40, curId); }
        else if (qos == 2) {
            if (wait_for_ack(s, 0x50, curId)) {
                char rel[4]; rel[0] = 0x62; rel[1] = 0x02; rel[2] = (curId >> 8) & 0xFF; rel[3] = curId & 0xFF;
                stream_write(s, rel, 4); stream_flush(s);
                if (wait_for_ack(s, 0x70, curId)) success = 1;
            }
        }
        if (success == 0) attempt = attempt + 1;
    }

    if (success == 0) {
        setoutputtext(0, "Error: QoS ACK Timeout");
        return 0; // Rückmeldung an Hauptschleife: Verbindung ist wahrscheinlich tot
    } else {
        setoutputtext(0, "Send OK");
        return 1;
    }
}

// --- HAUPTSCHLEIFE ---

while(1) {
    float ena = getinput(12);
    float trg = getinput(0);
    int qosIn = (int)(getinput(1) + 0.1);
    int retIn = (int)(getinput(2) + 0.1);

    if (ena > 0.5) {
        lastEnaState = 1; // merkt "war zuletzt enabled", für den sauberen Disable-Übergang

        // Heartbeat läuft unabhängig vom Verbindungsstatus, solange I13=1 -
        // zeigt "der Loop lebt", nicht "der Broker ist erreichbar".
        int curHb = (getcurrenttime() / 5) % 2;
        if (curHb != lastHeartbeat) { setoutput(0, curHb); lastHeartbeat = curHb; }

        if (pMqttStream == NULL) {
            char url[64]; sprintf(url, "/dev/tcp/%s/%d", BROKER_IP, BROKER_PORT);
            pMqttStream = stream_create(url, 0, 0);
            if (pMqttStream != NULL) {
                safe_mqtt_connect(pMqttStream);
                lastPing = getcurrenttime();
                setoutputtext(0, "Connected to Broker");
            } else {
                setoutputtext(0, "TCP Connection Error");
                sleep(5000);
            }
        }

        if (pMqttStream != NULL) {
            if (trg > 0.5 && lastTrigger <= 0.5) {
                char* tTopic = getinputtext(0);
                char* tPayload = getinputtext(1);
                if (tTopic != NULL && tPayload != NULL) {
                    // Wenn der Publish fehlschlägt, Stream sofort schließen -
                    // der nächste Zyklus verbindet automatisch neu.
                    if (safe_mqtt_publish(pMqttStream, tTopic, tPayload, qosIn, retIn) == 0) {
                        stream_close(pMqttStream);
                        pMqttStream = NULL;
                    }
                }
            }
            lastTrigger = trg;

            // Ping-Check
            if (pMqttStream != NULL && getcurrenttime() - lastPing > 30) {
                char p[2]; p[0]=0xC0; p[1]=0x00;
                if (stream_write(pMqttStream, p, 2) <= 0) {
                    setoutputtext(0, "Connection Lost");
                    stream_close(pMqttStream); pMqttStream = NULL;
                } else {
                    stream_flush(pMqttStream);
                    lastPing = getcurrenttime();
                }
            }

            // Lese-Puffer leeren und auf Socket-Fehler (-1) prüfen
            if (pMqttStream != NULL) {
                char dummy[128];
                int readRes = stream_read(pMqttStream, dummy, 128, 1);
                if (readRes < 0) {
                    setoutputtext(0, "Socket Read Error");
                    stream_close(pMqttStream); pMqttStream = NULL;
                } else {
                    while (stream_read(pMqttStream, dummy, 128, 1) > 0);
                }
            }
        }
    } else {
        // Text wird bei JEDEM Übergang enabled -> disabled genau einmal
        // gesetzt, unabhängig davon ob pMqttStream noch offen war oder schon
        // vorher (z.B. gescheiterter Reconnect) auf NULL stand.
        if (pMqttStream != NULL) {
            stream_close(pMqttStream); pMqttStream = NULL; lastHeartbeat = -1;
        }
        if (lastEnaState != 0) {
            setoutputtext(0, "Disconnected (Enable Off)");
            lastEnaState = 0;
        }
    }

    // AQ2: stabiles Signal, 1 nur wenn tatsächlich verbunden, sonst 0.
    // Zentral am Ende des Zyklus gesetzt, damit auch ein Verbindungsabbruch
    // mitten im Zyklus (Publish-Fehler, Ping-Fehler, Lesefehler) korrekt
    // erfasst wird.
    if (pMqttStream != NULL) {
        if (lastConnFlag != 1) { setoutput(1, 1); lastConnFlag = 1; }
    } else {
        if (lastConnFlag != 0) { setoutput(1, 0); lastConnFlag = 0; }
    }
    sleep(100);
}
