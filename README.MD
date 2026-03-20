<h1 align="center">Loxone Gen 1 - PicoC MQTT Suite (Sub & Pub)</h1>

<p align="center">
  <strong>Native, hochperformante MQTT-Implementierung für den Loxone Miniserver Gen 1.</strong><br>
  Optimiert für minimale CPU-Last durch <i>Zero-Parsing</i> Architektur und direkte TCP-Kommunikation.
</p>

<hr>

<h2>📖 System-Konzept</h2>
<p>
  Diese Suite ermöglicht es dem Miniserver Gen 1, ohne externe Gateways direkt mit MQTT-Brokern zu kommunizieren. 
  Um die begrenzte CPU-Leistung des Gen 1 zu schonen, nutzen beide Bausteine das <b>Zero-Parsing-Prinzip</b>: 
  Daten werden getrennt an den Eingängen übergeben, anstatt lange Texte mühsam im Skript zu zerlegen.
</p>

<hr>

<h2>⚙️ Globale Konfiguration (Header)</h2>
<p>Bevor du die Skripte in Loxone Config einfügst, passe die Zugangsdaten im Kopfbereich (Header) des jeweiligen Codes an:</p>
<pre><code>
#define BROKER_IP "X.X.X.X"   // IP deines MQTT-Brokers
#define BROKER_PORT 1883          // Standard Port: 1883
#define MQTT_USER "username"          // Benutzername
#define MQTT_PASS "password"    // Passwort
</code></pre>

<p><b>Master-Schalter (I13):</b> Beide Bausteine werden über <b>I13</b> (intern Index 12) aktiviert. Steht dieser auf "0", wird die TCP-Verbindung sofort getrennt und das Skript pausiert (0% Last).</p>

<hr>

<h2>1. Setup: MQTT Subscriber (Lox1_SUB)</h2>
<p>Empfängt Daten von bis zu 16 Topics gleichzeitig.</p>

<h3>Pin-Belegung (Eingänge)</h3>
<table width="100%" border="1" cellpadding="5">
  <tr bgcolor="#f2f2f2"><th>Eingang</th><th>Index</th><th>Funktion</th></tr>
  <tr><td><b>I13</b></td><td>12</td><td><b>Enable:</b> Programm aktivieren / TCP Standby.</td></tr>
  <tr><td><b>I1</b></td><td>0</td><td><b>Start Trigger:</b> Flanke (0 auf 1) startet Verbindung & Subscription.</td></tr>
  <tr><td><b>T1</b></td><td>-</td><td><b>Topic-Liste:</b> Format <code>Topic|QoS;</code> (z.B. <code>temp/1|0;batterie/1|1;</code>).</td></tr>
</table>

<h3>Pin-Belegung (Ausgänge)</h3>
<table width="100%" border="1" cellpadding="5">
  <tr bgcolor="#f2f2f2"><th>Ausgang</th><th>Index</th><th>Funktion</th></tr>
  <tr><td><b>AQ1</b></td><td>0</td><td><b>Heartbeat:</b> Taktet alle 5 Sek. (0/1). Überwachung (Watchdog).</td></tr>
  <tr><td><b>TQ1</b></td><td>0</td><td><b>Status:</b> Diagnose (z.B. <i>Connected</i>, <i>Subscribed</i>).</td></tr>
  <tr><td><b>TQ2</b></td><td>1</td><td><b>Daten:</b> Empfangene Werte im Format <code>S1:[topic|value];</code>.</td></tr>
</table>

<p><i><b>Tipp:</b> Nutze für die Daten an TQ2 einen "Virtuellen Status" mit der Befehlserkennung <code>S1:[\v]</code> (für den ersten Sensor).</i></p>

<hr>

<h2>2. Setup: MQTT Publisher (Lox1_PUB)</h2>
<p>Sendet Nachrichten sofort bei Trigger-Impuls (inkl. QoS 1 & 2 Bestätigung).</p>

<h3>Pin-Belegung (Eingänge)</h3>
<table width="100%" border="1" cellpadding="5">
  <tr bgcolor="#f2f2f2"><th>Eingang</th><th>Index</th><th>Funktion</th></tr>
  <tr><td><b>I13</b></td><td>12</td><td><b>Enable:</b> Hält die TCP-Verbindung zum Broker offen.</td></tr>
  <tr><td><b>I1</b></td><td>0</td><td><b>Send Trigger:</b> Positive Flanke sendet das aktuelle Paket.</td></tr>
  <tr><td><b>I2</b></td><td>1</td><td><b>QoS:</b> Quality of Service (0, 1 oder 2).</td></tr>
  <tr><td><b>I3</b></td><td>2</td><td><b>Retain:</b> Nachricht beim Broker speichern (0 oder 1).</td></tr>
  <tr><td><b>T1 / T2</b></td><td>-</td><td><b>Topic / Payload:</b> Zielpfad und Inhalt (z.B. <code>licht/1</code> und <code>ON</code>).</td></tr>
</table>

<h3>Pin-Belegung (Ausgänge)</h3>
<table width="100%" border="1" cellpadding="5">
  <tr bgcolor="#f2f2f2"><th>Ausgang</th><th>Index</th><th>Funktion</th></tr>
  <tr><td><b>AQ1</b></td><td>0</td><td><b>Heartbeat:</b> Taktet alle 5 Sek. (0/1).</td></tr>
  <tr><td><b>AQ2</b></td><td>1</td><td><b>Error Pulse:</b> Kurzer Impuls bei Sende-Fehlern (ACK-Timeout).</td></tr>
  <tr><td><b>TQ1</b></td><td>0</td><td><b>Status:</b> Feedback (z.B. <i>Send OK</i>, <i>Error: Timeout</i>).</td></tr>
</table>

<hr>

<h2>🛠 Installation in Loxone Config</h2>
<ol>
  <li>Erstelle einen neuen Baustein vom Typ <b>"Programm"</b>.</li>
  <li>Kopiere den Quellcode des gewünschten Skripts in den Editor.</li>
  <li>Verbinde die digitalen und analogen Ein-/Ausgänge laut den obigen Tabellen.</li>
  <li><b>Wichtig für Publisher:</b> Nutze vor dem Sende-Trigger (I1) einen <i>Verzögerten Impuls (0.1s)</i>. Damit stellst du sicher, dass die Texte an T1/T2 bereits stabil im RAM liegen, bevor das Skript feuert.</li>
</ol>

<hr>

<p align="center">
  <i>Maximale MQTT-Power für den Loxone Gen 1. Schlank, stabil und direkt.</i>
</p>
