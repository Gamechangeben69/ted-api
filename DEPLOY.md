# Deployment – TED IT Tenders API

## 1. Server einrichten (Hetzner CX22 reicht)

```bash
apt update && apt install -y python3 python3-pip python3-venv git cron
git clone https://github.com/Gamechangeben69/ted-api.git /opt/ted-api
cd /opt/ted-api
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
# Fuer PostgreSQL zusaetzlich:
# pip install psycopg2-binary
cp .env.example .env
# .env nach Bedarf anpassen (DATABASE_URL, API_KEYS)
```

## 2. Datenbank initial befuellen

```bash
# Letzte 30 Tage Deutschland (erster Lauf, ~10-15 Min)
python scraper.py --days 30 --land DEU --xml-limit 200

# Alle EU-Laender letzte 14 Tage
python scraper.py --days 14 --land DEU,FRA,POL,ITA,ESP,NLD,BEL,AUT,SWE,DNK --xml-limit 500

# Historischer Backfill 1 Jahr (nur Deutschland, ~2-3 Std)
python scraper.py --historisch --days 365 --land DEU

# Historischer Backfill alle EU (viele Stunden, fuer Produktion)
python scraper.py --historisch --days 365 --land DEU,FRA,POL,ITA,ESP,NLD,BEL,AUT,SWE,DNK,FIN,PRT,CZE,SVK,HUN,ROU,BGR,HRV,SVN,EST,LVA,LTU,LUX,MLT,CYP,IRL,GRC,NOR,CHE
```

## 3. API-Server starten

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

Swagger-Doku: http://DEINE-IP:8000/

## 4. Als Systemd-Service einrichten

```bash
cat > /etc/systemd/system/ted-api.service << EOF
[Unit]
Description=TED IT Tenders API
After=network.target

[Service]
User=root
WorkingDirectory=/opt/ted-api
EnvironmentFile=/opt/ted-api/.env
ExecStart=/opt/ted-api/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable ted-api
systemctl start ted-api
systemctl status ted-api
```

## 5. Cron-Jobs einrichten

```bash
# Taeglich um 05:00 Uhr: letzte 3 Tage scrapen + Alerts pruefen
(crontab -l 2>/dev/null; echo "0 5 * * * cd /opt/ted-api && /opt/ted-api/venv/bin/python scraper.py --days 3 --land DEU,FRA,POL,ITA,ESP,NLD,BEL,AUT --xml-limit 200 >> /var/log/ted-scraper.log 2>&1") | crontab -

# Taeglich um 06:00 Uhr: Alerts pruefen und Webhooks senden
(crontab -l 2>/dev/null; echo "0 6 * * * cd /opt/ted-api && /opt/ted-api/venv/bin/python scraper.py --check-alerts --days 1 >> /var/log/ted-alerts.log 2>&1") | crontab -
```

## 6. PostgreSQL einrichten (empfohlen fuer Produktion)

```bash
apt install -y postgresql
sudo -u postgres psql -c "CREATE USER ted_user WITH PASSWORD 'password';"
sudo -u postgres psql -c "CREATE DATABASE ted_tenders OWNER ted_user;"
# In .env setzen:
# DATABASE_URL=postgresql://ted_user:password@localhost:5432/ted_tenders
pip install psycopg2-binary
```

## 7. RapidAPI einrichten

1. Account anlegen auf https://rapidapi.com/provider
2. Neue API anlegen → "Add an API"
3. Base URL eintragen: `http://DEINE-IP:8000` (oder mit Nginx + HTTPS)
4. Endpunkte eintragen (alle GET-Endpunkte)
5. Pricing-Plaene:
   - **Free**: 50 Requests/Tag, kostenlos
   - **Basic**: 500 Requests/Tag, z.B. 9 EUR/Monat
   - **Pro**: Unlimited, z.B. 49 EUR/Monat

## 8. Optional: Nginx als Reverse Proxy + HTTPS

```bash
apt install -y nginx certbot python3-certbot-nginx
cat > /etc/nginx/sites-available/ted-api << EOF
server {
    server_name api.deine-domain.de;
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
    }
}
EOF
ln -s /etc/nginx/sites-available/ted-api /etc/nginx/sites-enabled/
certbot --nginx -d api.deine-domain.de
systemctl restart nginx
```

## API-Key-Verwaltung

API-Keys und Tiers in `.env` eintragen:
```
API_KEYS=key1:basic;key2:pro;key3:basic
```

## Alle Endpunkte

| Endpunkt | Beschreibung |
|---|---|
| `GET /` | Swagger-Dokumentation |
| `GET /tenders` | Ausschreibungen suchen (country, cpv, keyword, active, days, date_from, date_to) |
| `GET /tenders/{id}` | Einzelne Ausschreibung mit Losen + Zuschlaegen (lazy XML enrichment) |
| `GET /awards` | Zuschlaege suchen (country, days, min_value) |
| `GET /suppliers` | Lieferanten nach Win-Count und Gesamtwert |
| `GET /suppliers/{id}/awards` | Alle Zuschlaege eines Lieferanten |
| `POST /alerts` | Gespeichertes Suchprofil anlegen |
| `GET /alerts` | Alle aktiven Alerts auflisten |
| `GET /alerts/{id}/check` | Alert manuell ausfuehren |
| `DELETE /alerts/{id}` | Alert deaktivieren |
| `GET /stats` | Statistiken nach Land, Kategorie, Typ |
| `GET /health` | Health-Check |

## Beispielanfragen

```bash
# Aktive IT-Ausschreibungen in Deutschland
curl "http://localhost:8000/tenders?country=DEU&active=true"

# Software-Ausschreibungen EU-weit, letzte 7 Tage
curl "http://localhost:8000/tenders?cpv=48&days=7&active=false&page_size=50"

# Stichwortsuche (sucht in Titel + Beschreibung)
curl "http://localhost:8000/tenders?keyword=cybersecurity&active=true"

# Zuschlaege ueber 500.000 EUR
curl "http://localhost:8000/awards?min_value=500000&days=30"

# Top-Lieferanten nach Wins
curl "http://localhost:8000/suppliers?page_size=20"

# Alert anlegen: alle neuen IT-Ausschreibungen in Deutschland
curl -X POST "http://localhost:8000/alerts?name=IT+Germany&country=DEU&cpv_prefix=72&webhook_url=https://hooks.example.com/ted"

# Alert manuell pruefen
curl "http://localhost:8000/alerts/1/check?days=7"

# Statistiken
curl "http://localhost:8000/stats"
```
