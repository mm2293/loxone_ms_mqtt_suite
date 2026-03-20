// MQTT Multi-Topic Subscriber - EXTREME SPEED TUNED
// TQ1: Status | TQ2: Daten | I13: Enable | I1: Start Trigger

#define BROKER_IP "x.x.x.x"
#define BROKER_PORT 1883
#define CLIENT_ID "Lox1_SUB"
#define MQTT_USER "username"
#define MQTT_PASS "password"
#define MAX_TOPICS 16

STREAM* pMqttStream = NULL;
char topics[2048];  
int topicQos[MAX_TOPICS]; 
char values[512]; 
char buf[1024];
char rcvTop[128];
char fOut[600];
char outBuf[800]; 
char response[4];   

// --- GLOBALE VARIABLEN FÜR MAXIMALE PERFORMANCE ---
int i, k, n, t, c, initialized = 0; 
int lastPing = 0;
int topicCount = 0;
int lastHeartbeat = -1;
int currentHb = 0;
float lastStartTrigger = 0.0; 
int bIdx, type, qosFlag, msgPos, remLen, mult, totalLen, tLen, pStart, pLen;
int copyLen, outPos, fLen, rid; 

// Variablen für Connect & Subscribe
int uL, pL, cL, rL, tL; // <--- HIER: tL hinzugefügt
char h[12]; char head[2];
char subH[6]; char qos;

// Variablen für match_topic
int sL;

// Variablen für Main-Loop
float enable;
float startTrg;
int now;
char* tInput;
int pQos;
char url[64];
char p[2];
// --------------------------------------------------

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

void safe_mqtt_subscribe(STREAM* s, char* topic, int qosLvl) {
    tL = strlen(topic); if (tL == 0) return;
    rL = 2 + 2 + tL + 1;
    qos = qosLvl & 0x03; 
    subH[0]=0x82; subH[1]=rL; subH[2]=0x00; subH[3]=0x01;
    subH[4]=(tL>>8)&0xFF; subH[5]=tL&0xFF;
    stream_write(s, subH, 6); stream_write(s, topic, tL); stream_write(s, &qos, 1);
    stream_flush(s);
}

int match_topic(char* rTop, char* search) {
    sL = strlen(search); 
    if (sL <= 0) return 0;
    if (search[sL - 1] == '#') {
        if (strncmp(rTop, search, sL - 1) == 0) return 1;
    } else {
        if (strcmp(rTop, search) == 0) return 1;
    }
    return 0;
}

// --- HAUPTSCHLEIFE ---

while(1) {
    enable = getinput(12); // I13
    startTrg = getinput(0); // I1
    now = getcurrenttime();

    if (enable > 0.5) {
        if (initialized == 0) {
            tInput = getinputtext(0);
            topics[0] = '\0'; 
            if (tInput != NULL) {
                pQos = 0; i = 0; t = 0; c = 0;
                while((tInput[i] != '\0') && (t < MAX_TOPICS)) {
                    if(tInput[i] == ';') { 
                        topics[t * 128 + c] = '\0'; 
                        if (pQos == 0) { topicQos[t] = 0; }
                        t++; c = 0; pQos = 0; 
                    } else if(tInput[i] == '|') {
                        topics[t * 128 + c] = '\0'; pQos = 1; 
                    } else if(tInput[i] > 32) {
                        if (pQos == 0) { if(c < 127) { topics[t * 128 + c] = tInput[i]; c++; } }
                        else { if (tInput[i] == '1') topicQos[t] = 1; else if (tInput[i] == '2') topicQos[t] = 2; }
                    }
                    i++;
                }
                if (c > 0) { topicCount = t + 1; } else { topicCount = t; }
            }
            initialized = 1; 
            setoutputtext(0, "Ready - Waiting for Trigger");
        }

        if ((startTrg > 0.5) && (lastStartTrigger <= 0.5) && (pMqttStream == NULL)) {
            setoutputtext(0, "Connecting...");
            sprintf(url, "/dev/tcp/%s/%d", BROKER_IP, BROKER_PORT);
            pMqttStream = stream_create(url, 0, 0);
            if (pMqttStream != NULL) {
                safe_mqtt_connect(pMqttStream);
                setoutputtext(0, "Connected");
                lastPing = now;
                for(i=0; i < topicCount; i++) { 
                    safe_mqtt_subscribe(pMqttStream, &topics[i * 128], topicQos[i]); 
                }
                setoutputtext(0, "Subscribed");
            } else { 
                setoutputtext(0, "TCP Error");
            }
        }
        lastStartTrigger = startTrg;

        if (pMqttStream != NULL) {
            currentHb = (now / 5) % 2; 
            if (currentHb != lastHeartbeat) { setoutput(0, currentHb); lastHeartbeat = currentHb; }

            if ((now - lastPing) > 30) {
                p[0]=0xC0; p[1]=0x00; 
                if (stream_write(pMqttStream, p, 2) <= 0) {
                    setoutputtext(0, "Ping Failed");
                }
                stream_flush(pMqttStream);
                lastPing = now;
            }

            outPos = 0; outBuf[0] = '\0';

            while ((n = stream_read(pMqttStream, buf, 1024, 10)) > 0) { 
                bIdx = 0;
                while (bIdx < n) {
                    type = buf[bIdx] & 0xF0;
                    qosFlag = (buf[bIdx] >> 1) & 0x03;
                    msgPos = bIdx + 1; remLen = 0; mult = 1;
                    
                    while (msgPos < n) {
                        remLen = remLen + ((buf[msgPos] & 127) * mult);
                        if ((buf[msgPos] & 128) == 0) { msgPos++; break; }
                        mult = mult * 128; msgPos++;
                    }
                    totalLen = (msgPos - bIdx) + remLen;

                    if ((type == 0x30) && ((bIdx + totalLen) <= n)) {
                        tLen = (buf[msgPos] << 8) | (buf[msgPos+1] & 0xFF);
                        pStart = msgPos + 2 + tLen;
                        
                        if (qosFlag > 0) {
                            rid = (buf[pStart] << 8) | (buf[pStart+1] & 0xFF);
                            if (qosFlag == 1) { 
                                response[0] = 0x40; response[1] = 0x02; 
                                response[2] = buf[pStart]; response[3] = buf[pStart+1];
                                stream_write(pMqttStream, response, 4); stream_flush(pMqttStream);
                            } else if (qosFlag == 2) { 
                                response[0] = 0x50; response[1] = 0x02; 
                                response[2] = buf[pStart]; response[3] = buf[pStart+1];
                                stream_write(pMqttStream, response, 4); stream_flush(pMqttStream);
                            }
                            pStart = pStart + 2; 
                        }
                        
                        pLen = (bIdx + totalLen) - pStart;
                        if (tLen > 127) tLen = 127;
                        strncpy(rcvTop, &buf[msgPos+2], tLen); rcvTop[tLen] = '\0';

                        for(k=0; k < topicCount; k++) {
                            if(match_topic(rcvTop, &topics[k * 128])) {
                                if (pLen > 510) { copyLen = 510; } else { copyLen = pLen; }
                                strncpy(values, &buf[pStart], copyLen); values[copyLen] = '\0';
                                sprintf(fOut, "S%d:[%s|%s];", k + 1, rcvTop, values);
                                fLen = strlen(fOut);
                                if ((outPos + fLen) < 790) { strcpy(&outBuf[outPos], fOut); outPos = outPos + fLen; }
                                break; 
                            }
                        }
                    } 
                    else if (type == 0x60) {
                        response[0] = 0x70; response[1] = 0x02; 
                        response[2] = buf[msgPos]; response[3] = buf[msgPos+1];
                        stream_write(pMqttStream, response, 4); stream_flush(pMqttStream);
                    }
                    bIdx = bIdx + totalLen;
                }
            }
            if (outPos > 0) { setoutputtext(1, outBuf); } 
            if (n < 0) { 
                setoutputtext(0, "Connection Lost");
                stream_close(pMqttStream); pMqttStream = NULL; 
            }
        }
        sleep(10); 
    } else {
        setoutput(0, 0); 
        if(pMqttStream != NULL) { 
            setoutputtext(0, "Disconnected");
            stream_close(pMqttStream); pMqttStream = NULL; 
        }
        initialized = 0; 
        lastStartTrigger = 0.0;
        sleep(500); 
    }
}
