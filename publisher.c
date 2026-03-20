// MQTT Publisher - ZERO-PARSING - WITH DIAGNOSTICS
// T1=Topic, T2=Payload, I13=Enable, I1=Trigger, I2=QoS, I3=Retain, I13=Enable
// AQ1 (Index 0) = Heartbeat | AQ2 (Index 1) = Error Pulse
// TQ1 (Index 0) = Status & Error Text

#define BROKER_IP "x.x.x.x"
#define BROKER_PORT 1883
#define CLIENT_ID "Lox1_PUB"
#define MQTT_USER "username"
#define MQTT_PASS "password"

STREAM* pMqttStream = NULL;
int lastPing = 0;
int packetIdCounter = 1; 
int lastHeartbeat = -1;
float lastTrigger = 0.0;
char packetBuf[1024];

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

void safe_mqtt_publish(STREAM* s, char* topic, char* payload, int qos, int retain) {
    int tLen, pLen, remLen, curId, j, idx, attempt, success;
    tLen = strlen(topic); 
    if (tLen == 0) { setoutputtext(0, "Error: Topic missing"); return; }
    
    pLen = strlen(payload);
    remLen = 2 + tLen + pLen;
    curId = 0;
    if (qos > 0) { 
        remLen = remLen + 2; 
        packetIdCounter = packetIdCounter + 1;
        if (packetIdCounter > 65000) packetIdCounter = 1; 
        curId = packetIdCounter; 
    }
    
    if ((remLen + 5) > 1024) { setoutputtext(0, "Error: Payload too large"); return; }
    
    attempt = 0; success = 0;
    while (attempt < 2 && success == 0) {
        idx = 0; packetBuf[idx] = 0x30; 
        if (qos == 1) packetBuf[idx] = packetBuf[idx] | 0x02;
        if (qos == 2) packetBuf[idx] = packetBuf[idx] | 0x04;
        if (retain == 1) packetBuf[idx] = packetBuf[idx] | 0x01;
        if (attempt > 0) packetBuf[idx] = packetBuf[idx] | 0x08;
        idx = idx + 1;
        if (remLen < 128) { packetBuf[idx++] = remLen; } 
        else { packetBuf[idx++] = (remLen & 127) | 128; packetBuf[idx++] = (remLen >> 7); }
        packetBuf[idx++] = (tLen >> 8) & 0xFF; packetBuf[idx++] = tLen & 0xFF;
        for(j=0; j<tLen; j++) packetBuf[idx++] = topic[j];
        if (qos > 0) { packetBuf[idx++] = (curId >> 8) & 0xFF; packetBuf[idx++] = (curId & 0xFF); }
        for(j=0; j<pLen; j++) packetBuf[idx++] = payload[j];
        
        stream_write(s, packetBuf, idx); stream_flush(s);
        
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
        setoutput(1, 1); sleep(50); setoutput(1, 0); 
    } else {
        setoutputtext(0, "Send OK");
    }
}

// --- HAUPTSCHLEIFE ---

while(1) {
    float ena = getinput(12); 
    float trg = getinput(0);
    int qosIn = (int)(getinput(1) + 0.1); 
    int retIn = (int)(getinput(2) + 0.1);
    
    if (ena > 0.5) {
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
            int curHb = (getcurrenttime() / 5) % 2;
            if (curHb != lastHeartbeat) { setoutput(0, curHb); lastHeartbeat = curHb; }
            
            if (trg > 0.5 && lastTrigger <= 0.5) {
                char* tTopic = getinputtext(0); 
                char* tPayload = getinputtext(1);
                if (tTopic != NULL && tPayload != NULL) {
                    safe_mqtt_publish(pMqttStream, tTopic, tPayload, qosIn, retIn);
                }
            }
            lastTrigger = trg;

            if (getcurrenttime() - lastPing > 30) {
                char p[2]; p[0]=0xC0; p[1]=0x00; 
                if (stream_write(pMqttStream, p, 2) <= 0) {
                    setoutputtext(0, "Connection Lost");
                    stream_close(pMqttStream); pMqttStream = NULL;
                } else {
                    stream_flush(pMqttStream);
                    lastPing = getcurrenttime();
                }
            }
            char dummy[128]; while (stream_read(pMqttStream, dummy, 128, 1) > 0);
        }
    } else if (pMqttStream != NULL) {
        setoutputtext(0, "Disconnected (Enable Off)");
        stream_close(pMqttStream); pMqttStream = NULL; lastHeartbeat = -1;
    }
    sleep(100);
}
