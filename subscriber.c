/*
 * MQTT Multi-Topic Subscriber für Loxone (Programmierbaustein)
 * ==============================================================
 *
 * Abonniert mehrere MQTT-Topics (inkl. Wildcard '#' am Ende) bei einem
 * Broker und gibt empfangene Werte sowie den Verbindungsstatus als
 * Loxone-Ein-/Ausgänge aus.
 *
 * INPUTS
 *   I1  (Index 0)  Digital - Start-Trigger. Eine steigende Flanke markiert
 *                  "Verbindung gewünscht"; ob tatsächlich (re-)connectet
 *                  wird, hängt danach nur noch davon ab, ob gerade keine
 *                  Verbindung besteht (siehe "Reconnect-Verhalten" unten).
 *   I13 (Index 12) Digital - Enable. 1 = aktiv, 0 = getrennt/inaktiv.
 *   T1  (Index 0)  Text - Topic-Liste, Format:
 *                  "topic/eins;topic/zwei|1;topic/drei|2;sensors/#"
 *                  ";" trennt Topics, "|1" bzw. "|2" direkt nach einem
 *                  Topic setzt dessen QoS (Default QoS 0, kein Suffix nötig).
 *
 * OUTPUTS
 *   O1 (Index 0)  Digital - Heartbeat, togglet ca. alle 5s solange I13=1.
 *                 Zeigt "der Baustein läuft", unabhängig vom Broker.
 *   O2 (Index 1)  Digital - Verbindungsstatus: 1 = aktuell mit Broker
 *                 verbunden, 0 = alles andere (nicht gestartet,
 *                 reconnecting, Timeout, disabled).
 *   T1 (Index 0)  Text - Statustext ("Ready", "Subscribed",
 *                 "Connection Lost", "Ping Timeout - Reconnecting", ...).
 *   T2 (Index 1)  Text - empfangene Werte in einem Rutsch, Format:
 *                 "S<Topic-Index+1>:<Wert>;S<Topic-Index+1>:<Wert>;..."
 *                 Der Topic-Index bezieht sich auf die Reihenfolge in T1
 *                 (Eingang), 1-basiert. Empfehlung: pro Topic-Index einen
 *                 Loxone-Textbaustein/Regex zum Extrahieren des Werts.
 *   T3 (Index 2)  Text - Diagnose: nach dem Parsen der Topic-Liste zeigt
 *                 dieser Ausgang Eingabelänge/Anzahl erkannter Topics,
 *                 nach Abschluss aller SUBACKs zusätzlich einmalig den
 *                 Broker-Return-Code je Topic (0/1/2 = OK mit der jeweiligen
 *                 QoS, 128 = vom Broker abgelehnt, z.B. wegen ACL).
 *
 * FEATURES
 *   - Automatischer Reconnect: läuft immer, wenn "gestartet" wurde und
 *     aktuell keine Verbindung besteht - unabhängig vom aktuellen Pegel
 *     von I1 (siehe unten).
 *   - PINGRESP-Watchdog: erkennt tote Verbindungen aktiv, falls der
 *     Loxone-Stream ein sauberes Schließen durch den Broker nicht selbst
 *     über einen negativen stream_read()-Rückgabewert meldet.
 *   - Bounds-Checks beim Parsen des MQTT-Byte-Streams, damit fragmentierte
 *     oder unerwartet große Pakete (z.B. lange JSON-Payloads) nicht zu
 *     Buffer-Overruns/Abstürzen führen, sondern sauber verworfen werden.
 *   - Debug-Ausgabe der geparsten Topic-Liste und der SUBACK-Ergebnisse
 *     je Topic (siehe T3), um z.B. abgeschnittene Eingabetexte oder vom
 *     Broker abgelehnte Subscriptions schnell zu erkennen.
 *
 * RECONNECT-VERHALTEN
 *   I1 setzt nur ein internes Flag ("Verbindung gewünscht"). Der eigentliche
 *   (Re-)Connect erfolgt danach automatisch, sobald keine Verbindung besteht
 *   - auch nach einem späteren Verbindungsabbruch (z.B. Broker-Neustart),
 *   ohne dass I1 erneut gepulst werden muss.
 *
 * KONFIGURATION
 *   Siehe Konstanten unten. Bitte vor dem Einsatz an die eigene Umgebung
 *   anpassen. Aus Sicherheitsgründen keine echten Zugangsdaten in ein
 *   öffentliches Repository committen.
 */

#define BROKER_IP   "192.168.1.10"        // TODO: IP/Hostname eures MQTT-Brokers
#define BROKER_PORT 1883
#define CLIENT_ID   "Loxone_Subscriber"   // TODO: im Netzwerk eindeutige Client-ID
#define MQTT_USER   "mqtt_user"           // TODO: MQTT-Benutzername
#define MQTT_PASS   "mqtt_password"       // TODO: MQTT-Passwort
#define MAX_TOPICS 16
#define RECONNECT_INTERVAL 5

STREAM* pMqttStream = NULL;
char topics[2048];
int topicQos[MAX_TOPICS];
char values[512];
char buf[1024];
char rcvTop[128];
char fOut[1024];
char outBuf[2048];
char response[4];
char dbgOut[256];
char sumOut[600];
int subAckCode[MAX_TOPICS];  // gespeicherter Return-Code je Topic (-1 = keine Antwort erhalten)
int subAckCount = 0;
int subAckReported = 0;      // verhindert Mehrfachausgabe der Zusammenfassung auf T3
int pid, sIdx;                // Packet-ID / Topic-Index aus dem SUBACK

// Cache für die Topic-Erkennung (Länge, Wildcard-Flag)
int tLenCache[MAX_TOPICS];
int tIsWild[MAX_TOPICS];
int isMatch;

int i, k, n, t, c, initialized = 0;
int lastPing = 0;
int topicCount = 0;
int lastHeartbeat = -1;
int lastConnFlag = -1;
int currentHb = 0;
float lastStartTrigger = 0.0;
int bIdx, type, qosFlag, msgPos, remLen, mult, totalLen, tLen, pStart, pLen;
int copyLen, outPos, fLen, rid;
int inLen;

int mqttStarted = 0;
int lastReconnectAttempt = -1000;
int pingAwaitingResponse = 0; // für den PINGRESP-Watchdog

// Variablen für Connect & Subscribe
int uL, pL, cL, rL, tL;
char h[12]; char head[2];
char subH[6]; char qos;

// Variablen für die Hauptschleife
float enable;
float startTrg;
int now;
char* tInput;
int pQos;
char url[64];
char p[2];

// --- HILFSFUNKTIONEN ---

void safe_mqtt_connect(STREAM* s) {
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

// Erhält eine eigene Packet-ID pro Subscribe (statt fix 0x0001), damit die
// spätere SUBACK-Antwort (spiegelt dieselbe Packet-ID zurück) eindeutig dem
// passenden Topic-Index zugeordnet werden kann.
void safe_mqtt_subscribe(STREAM* s, char* topic, int qosLvl, int packetId) {
    tL = strlen(topic); if (tL == 0) return;
    rL = 2 + 2 + tL + 1;
    qos = qosLvl & 0x03;
    subH[0]=0x82; subH[1]=rL; subH[2]=(packetId>>8)&0xFF; subH[3]=packetId&0xFF;
    subH[4]=(tL>>8)&0xFF; subH[5]=tL&0xFF;
    stream_write(s, subH, 6); stream_write(s, topic, tL); stream_write(s, &qos, 1);
    stream_flush(s);
}

// --- HAUPTSCHLEIFE ---

while(1) {
    enable = getinput(12); // I13
    startTrg = getinput(0); // I1
    now = getcurrenttime();
    currentHb = (now / 5) % 2; // auch im getrennten Zustand berechnet, damit O1 immer pulst

    if (enable > 0.5) {
        // Heartbeat läuft unabhängig vom Verbindungsstatus, solange I13=1 -
        // zeigt "der Loop lebt", nicht "der Broker ist erreichbar".
        if (currentHb != lastHeartbeat) { setoutput(0, currentHb); lastHeartbeat = currentHb; }

        if (initialized == 0) {
            tInput = getinputtext(0);
            topics[0] = '\0';
            inLen = 0;
            if (tInput != NULL) {
                inLen = strlen(tInput); // tatsächliche Länge des Eingabetexts, für Debug
                pQos = 0; i = 0; t = 0; c = 0;
                while((tInput[i] != '\0') && (t < MAX_TOPICS)) {
                    if(tInput[i] == ';') {
                        topics[t * 128 + c] = '\0';
                        if (pQos == 0) { topicQos[t] = 0; }
                        t++; c = 0; pQos = 0;
                    } else if(tInput[i] == '|') {
                        topics[t * 128 + c] = '\0'; pQos = 1;
                    } else if(((unsigned char)tInput[i]) > 32) { // unsigned Vergleich wegen UTF-8
                        if (pQos == 0) { if(c < 127) { topics[t * 128 + c] = tInput[i]; c++; } }
                        else { if (tInput[i] == '1') topicQos[t] = 1; else if (tInput[i] == '2') topicQos[t] = 2; }
                    }
                    i++;
                }
                if (c > 0) { topicCount = t + 1; } else { topicCount = t; }
            }

            for (i = 0; i < topicCount; i++) {
                tLenCache[i] = strlen(&topics[i * 128]);
                tIsWild[i] = 0;
                if (tLenCache[i] > 0) {
                   if (topics[i * 128 + tLenCache[i] - 1] == '#') tIsWild[i] = 1;
                }
            }

            // Debug-Ausgabe: Eingabelänge, erkannte Topic-Anzahl und letztes
            // erkanntes Topic - hilft z.B. zu erkennen, ob getinputtext(0)
            // durch eine zu kurze max. Textlänge im Eingabefeld abgeschnitten
            // wurde, wodurch hintere Topics nie subscribed würden.
            if (topicCount > 0) {
                sprintf(dbgOut, "InLen=%d Count=%d Last[%d]=%s", inLen, topicCount, topicCount - 1, &topics[(topicCount - 1) * 128]);
            } else {
                sprintf(dbgOut, "InLen=%d Count=0 (keine Topics erkannt!)", inLen);
            }
            setoutputtext(2, dbgOut);

            initialized = 1;
            setoutputtext(0, "Ready - Waiting for Trigger");
        }

        // I1 markiert nur "Start gewünscht", verbindet nicht direkt selbst.
        if ((startTrg > 0.5) && (lastStartTrigger <= 0.5)) {
            mqttStarted = 1;
        }
        lastStartTrigger = startTrg;

        // (Re-)Connect läuft, sobald mqttStarted=1 und aktuell keine
        // Verbindung besteht - unabhängig vom aktuellen Pegel von I1.
        if ((mqttStarted == 1) && (pMqttStream == NULL) && ((now - lastReconnectAttempt) >= RECONNECT_INTERVAL)) {
            lastReconnectAttempt = now;
            sprintf(url, "/dev/tcp/%s/%d", BROKER_IP, BROKER_PORT);
            pMqttStream = stream_create(url, 0, 0);
            if (pMqttStream != NULL) {
                safe_mqtt_connect(pMqttStream);
                lastPing = now;
                pingAwaitingResponse = 0;
                // SUBACK-Tracking für diesen (Re-)Connect zurücksetzen
                subAckCount = 0; subAckReported = 0;
                for(i=0; i < MAX_TOPICS; i++) subAckCode[i] = -1;
                // Jedes Topic bekommt eine eigene Packet-ID (i+1), damit die
                // SUBACK-Antwort später eindeutig zuordenbar ist.
                for(i=0; i < topicCount; i++) safe_mqtt_subscribe(pMqttStream, &topics[i * 128], topicQos[i], i + 1);
                setoutputtext(0, "Subscribed");
            } else {
                setoutputtext(0, "Reconnecting...");
            }
        }

        if (pMqttStream != NULL) {
            if ((now - lastPing) > 30) {
                if (pingAwaitingResponse == 1) {
                    // Kein PINGRESP seit dem letzten Zyklus -> Verbindung gilt
                    // als tot, unabhängig davon ob stream_read() selbst einen
                    // Fehler meldet.
                    setoutputtext(0, "Ping Timeout - Reconnecting");
                    stream_close(pMqttStream);
                    pMqttStream = NULL;
                } else {
                    p[0]=0xC0; p[1]=0x00;
                    stream_write(pMqttStream, p, 2); stream_flush(pMqttStream);
                    lastPing = now;
                    pingAwaitingResponse = 1;
                }
            }

            // Falls der Watchdog gerade geschlossen hat, in diesem Zyklus
            // nicht mehr lesen - der nächste Durchlauf reconnectet automatisch.
            if (pMqttStream != NULL) {
                outPos = 0; outBuf[0] = '\0';

                while ((n = stream_read(pMqttStream, buf, 1024, 10)) > 0) {
                    bIdx = 0;
                    while (bIdx < n) {
                        // Mindestens Header-Byte + 1 Length-Byte muss im
                        // Puffer sein, bevor auf buf[bIdx+1] zugegriffen wird.
                        if ((bIdx + 1) >= n) { break; }

                        type = buf[bIdx] & 0xF0;
                        qosFlag = (buf[bIdx] >> 1) & 0x03;
                        msgPos = bIdx + 1; remLen = 0; mult = 1;
                        while (msgPos < n) {
                            remLen = remLen + ((buf[msgPos] & 127) * mult);
                            if ((buf[msgPos] & 128) == 0) { msgPos++; break; }
                            mult = mult * 128; msgPos++;
                        }

                        // Bricht die Remaining-Length-Schleife am Ende von n
                        // ab, ohne dass das Fortsetzungs-Bit (0x80) sauber
                        // terminiert wurde, ist der Header selbst fragmentiert
                        // (Paket geht über den nächsten stream_read() hinaus)
                        // -> remLen/msgPos sind dann nicht vertrauenswürdig.
                        if ((msgPos > 0) && (msgPos <= n) && (msgPos > bIdx) && ((buf[msgPos - 1] & 128) != 0)) {
                            break; // Length-Byte-Kette nicht terminiert -> Header unvollständig
                        }
                        if (msgPos >= n && remLen == 0 && mult > 1) {
                            break; // Sicherheitsnetz für den gleichen Fall
                        }

                        totalLen = (msgPos - bIdx) + remLen;

                        // Plausibilitäts-/Bounds-Check: schützt vor Buffer-
                        // Overrun bei fragmentierten, unvollständigen oder
                        // übergroßen Paketen (z.B. lange JSON-Payloads), die
                        // über mehrere stream_read()-Aufrufe verteilt
                        // ankommen. Ohne diesen Check könnte totalLen durch
                        // Integer-Überlauf negativ werden und die Prüfung
                        // "(bIdx+totalLen)<=n" trotzdem bestehen.
                        if ((totalLen <= 0) || (totalLen > 1024) || ((bIdx + totalLen) > n)) {
                            break; // Rest dieses Reads verwerfen; nächster Read/Reconnect fängt weiter
                        }

                        if ((type == 0x30) && ((bIdx + totalLen) <= n)) {
                            // Topic-Length-Header (2 Byte) muss vollständig im Puffer liegen
                            if ((msgPos + 2) > n) { bIdx = bIdx + totalLen; continue; }

                            tLen = (buf[msgPos] << 8) | (buf[msgPos+1] & 0xFF);

                            // tLen plausibilisieren, bevor damit weitergerechnet wird
                            if ((tLen < 0) || (tLen > 127) || ((msgPos + 2 + tLen) > n)) {
                                bIdx = bIdx + totalLen;
                                continue;
                            }

                            pStart = msgPos + 2 + tLen;
                            if (qosFlag > 0) {
                                // Packet-ID (2 Byte) muss im Puffer liegen, bevor gelesen wird
                                if ((pStart + 1) >= n) { bIdx = bIdx + totalLen; continue; }
                                if (qosFlag == 1) { response[0] = 0x40; response[1] = 0x02; response[2] = buf[pStart]; response[3] = buf[pStart+1]; stream_write(pMqttStream, response, 4); stream_flush(pMqttStream); }
                                else if (qosFlag == 2) { response[0] = 0x50; response[1] = 0x02; response[2] = buf[pStart]; response[3] = buf[pStart+1]; stream_write(pMqttStream, response, 4); stream_flush(pMqttStream); }
                                pStart = pStart + 2;
                            }

                            pLen = (bIdx + totalLen) - pStart;

                            // pLen/pStart gegen den tatsächlichen Puffer
                            // prüfen, bevor aus buf[] gelesen wird.
                            if ((pLen < 0) || (pStart < 0) || ((pStart + pLen) > n)) {
                                bIdx = bIdx + totalLen;
                                continue;
                            }

                            tL = tLen;
                            if (tL > 127) tL = 127;
                            strncpy(rcvTop, &buf[msgPos+2], tL);
                            rcvTop[tL] = '\0';

                            for(k=0; k < topicCount; k++) {
                                isMatch = 0;
                                if (tIsWild[k]) {
                                    if (strncmp(rcvTop, &topics[k * 128], tLenCache[k] - 1) == 0) isMatch = 1;
                                }
                                else {
                                    if (strcmp(rcvTop, &topics[k * 128]) == 0) isMatch = 1;
                                }

                                if(isMatch) {
                                    copyLen = pLen;
                                    if (copyLen > 510) copyLen = 510;

                                    strncpy(values, &buf[pStart], copyLen);
                                    values[copyLen] = '\0';

                                    // Topic-Name selbst wird nicht mit ausgegeben, nur der Index
                                    sprintf(fOut, "S%d:%s;", k + 1, values);

                                    fLen = strlen(fOut);
                                    if ((outPos + fLen) < 2000) { strcpy(&outBuf[outPos], fOut); outPos = outPos + fLen; }
                                    break;
                                }
                            }
                        }
                        else if (type == 0x60) {
                            // Packet-ID (2 Byte) muss im Puffer liegen
                            if ((msgPos + 1) >= n) { bIdx = bIdx + totalLen; continue; }
                            response[0] = 0x70; response[1] = 0x02;
                            response[2] = buf[msgPos]; response[3] = buf[msgPos+1];
                            stream_write(pMqttStream, response, 4); stream_flush(pMqttStream);
                        }
                        else if (type == 0xD0) {
                            // PINGRESP vom Broker -> Verbindung nachweislich lebendig
                            pingAwaitingResponse = 0;
                        }
                        else if (type == 0x90) {
                            // SUBACK vom Broker. Packet-ID (2 Bytes) verweist
                            // auf den Topic-Index (packetId = Index+1). Direkt
                            // danach folgt pro angefragtem Topic 1 Byte
                            // Return-Code:
                            //   0x00/0x01/0x02 = akzeptiert mit QoS 0/1/2
                            //   0x80           = Ablehnung (z.B. ACL/Rechte/Broker-Limit)
                            // 3 Byte (Packet-ID + Return-Code) müssen im Puffer liegen
                            if ((msgPos + 2) >= n) { bIdx = bIdx + totalLen; continue; }
                            pid = ((buf[msgPos] & 0xFF) << 8) | (buf[msgPos + 1] & 0xFF);
                            sIdx = pid - 1;
                            if ((sIdx >= 0) && (sIdx < MAX_TOPICS)) {
                                if (subAckCode[sIdx] == -1) subAckCount++;
                                subAckCode[sIdx] = buf[msgPos + 2] & 0xFF;
                            }
                        }
                        bIdx = bIdx + totalLen;
                    }
                }
                if (outPos > 0) setoutputtext(1, outBuf);

                // Sobald für alle Topics eine SUBACK-Antwort da ist, einmalig
                // eine Zusammenfassung auf T3 ausgeben - zeigt pro Topic-Index
                // den Broker-Return-Code, z.B. ob ein Topic (0x80) abgelehnt
                // statt (0x00/0x01/0x02) akzeptiert wurde.
                if ((subAckReported == 0) && (subAckCount >= topicCount) && (topicCount > 0)) {
                    outPos = 0; sumOut[0] = '\0';
                    for (i = 0; i < topicCount; i++) {
                        // Loxones sprintf unterstützt %X/%x nicht zuverlässig,
                        // daher Dezimalwert statt Hex.
                        // 0=QoS0 OK, 1=QoS1 OK, 2=QoS2 OK, 128=vom Broker abgelehnt
                        sprintf(fOut, "T%d=%d ", i + 1, subAckCode[i] & 0xFF);
                        fLen = strlen(fOut);
                        if ((outPos + fLen) < 590) { strcpy(&sumOut[outPos], fOut); outPos = outPos + fLen; }
                    }
                    setoutputtext(2, sumOut);
                    subAckReported = 1;
                }

                if ((pMqttStream != NULL) && (n < 0)) { setoutputtext(0, "Connection Lost"); stream_close(pMqttStream); pMqttStream = NULL; }
            }
        }

        // O2: stabiles Signal, 1 nur wenn tatsächlich verbunden, sonst 0.
        // Wird zentral am Ende des Zyklus gesetzt, damit auch ein
        // Verbindungsabbruch mitten im Zyklus (z.B. durch den Ping-Watchdog)
        // korrekt erfasst wird.
        if (pMqttStream != NULL) {
            if (lastConnFlag != 1) { setoutput(1, 1); lastConnFlag = 1; }
        } else {
            if (lastConnFlag != 0) { setoutput(1, 0); lastConnFlag = 0; }
        }
        sleep(10);
    } else {
        setoutput(0, 0);
        if (lastConnFlag != 0) { setoutput(1, 0); lastConnFlag = 0; } // nicht verbunden während deaktiviert
        if(pMqttStream != NULL) { setoutputtext(0, "Disconnected"); stream_close(pMqttStream); pMqttStream = NULL; }
        initialized = 0; lastStartTrigger = 0.0;
        mqttStarted = 0;
        pingAwaitingResponse = 0;
        sleep(500);
    }
}
