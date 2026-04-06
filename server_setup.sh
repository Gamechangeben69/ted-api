#!/bin/bash
# ============================================================
# TED API Server Setup
# Einmalig auf einem frischen Ubuntu 24.04 Server ausfuehren:
#   curl -sSL https://raw.githubusercontent.com/DEIN-USER/ted-api/main/server_setup.sh | bash
# ODER nach git clone:
#   bash server_setup.sh
# ============================================================

set -e  # Abbruch bei Fehler

REPO_URL="${1:-https://github.com/Gamechangeben69/ted-api.git}"
APP_DIR="/opt/ted-api"
APP_USER="tedapi"
DB_NAME="ted_tenders"
DB_USER="ted_user"
DB_PASS="$(openssl rand -base64 20 | tr -d '/+=' | head -c 24)"

echo "============================================================"
echo " TED API Server Setup"
echo " Repo: $REPO_URL"
echo "============================================================"
echo ""

# 1. System-Updates
echo "[1/9] System-Updates..."
apt-get update -qq
apt-get install -y -qq python3 python3-pip python3-venv git cron curl nginx postgresql

# 2. App-User anlegen
echo "[2/9] App-User anlegen..."
id "$APP_USER" &>/dev/null || useradd -r -m -d "$APP_DIR" -s /bin/bash "$APP_USER"

# 3. Code deployen
echo "[3/9] Code deployen..."
if [ -d "$APP_DIR/.git" ]; then
    echo "  Repository existiert bereits, pull..."
    sudo -u "$APP_USER" git -C "$APP_DIR" pull
else
    sudo -u "$APP_USER" git clone "$REPO_URL" "$APP_DIR"
fi

# 4. Python venv + Pakete
echo "[4/9] Python-Umgebung einrichten..."
sudo -u "$APP_USER" python3 -m venv "$APP_DIR/venv"
sudo -u "$APP_USER" "$APP_DIR/venv/bin/pip" install -q -r "$APP_DIR/requirements.txt"
sudo -u "$APP_USER" "$APP_DIR/venv/bin/pip" install -q psycopg2-binary

# 5. PostgreSQL einrichten
echo "[5/9] PostgreSQL einrichten..."
sudo -u postgres psql -c "SELECT 1 FROM pg_roles WHERE rolname='$DB_USER'" | grep -q 1 || \
    sudo -u postgres psql -c "CREATE USER $DB_USER WITH PASSWORD '$DB_PASS';"
sudo -u postgres psql -c "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'" | grep -q 1 || \
    sudo -u postgres psql -c "CREATE DATABASE $DB_NAME OWNER $DB_USER;"

# 6. .env anlegen
echo "[6/9] Konfiguration anlegen..."
cat > "$APP_DIR/.env" << ENV
DATABASE_URL=postgresql://$DB_USER:$DB_PASS@localhost:5432/$DB_NAME
API_KEYS=
ENV
chown "$APP_USER:$APP_USER" "$APP_DIR/.env"
chmod 600 "$APP_DIR/.env"

# 7. Systemd-Service
echo "[7/9] Systemd-Service einrichten..."
cat > /etc/systemd/system/ted-api.service << SERVICE
[Unit]
Description=TED IT Tenders API
After=network.target postgresql.service

[Service]
User=$APP_USER
WorkingDirectory=$APP_DIR
EnvironmentFile=$APP_DIR/.env
ExecStart=$APP_DIR/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000 --workers 2
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
SERVICE

systemctl daemon-reload
systemctl enable ted-api

# 8. Cron-Jobs
echo "[8/9] Cron-Jobs einrichten..."
CRON_SCRAPE="0 5 * * * cd $APP_DIR && $APP_DIR/venv/bin/python scraper.py --days 3 --land DEU,FRA,POL,ITA,ESP,NLD,BEL,AUT --xml-limit 200 >> /var/log/ted-scraper.log 2>&1"
CRON_ALERTS="0 6 * * * cd $APP_DIR && $APP_DIR/venv/bin/python scraper.py --check-alerts --days 1 >> /var/log/ted-alerts.log 2>&1"
(crontab -u "$APP_USER" -l 2>/dev/null; echo "$CRON_SCRAPE"; echo "$CRON_ALERTS") | crontab -u "$APP_USER" -

# 9. Firewall (UFW)
echo "[9/9] Firewall konfigurieren..."
ufw allow 22/tcp  >/dev/null 2>&1 || true
ufw allow 8000/tcp >/dev/null 2>&1 || true
ufw --force enable >/dev/null 2>&1 || true

echo ""
echo "============================================================"
echo " Setup abgeschlossen!"
echo ""
echo " DB-Passwort (sicher aufbewahren!): $DB_PASS"
echo ""
echo " Naechste Schritte:"
echo " 1. Ersten Datensatz laden:"
echo "    sudo -u $APP_USER $APP_DIR/venv/bin/python $APP_DIR/scraper.py --days 30 --land DEU --xml-limit 100"
echo ""
echo " 2. Server starten:"
echo "    systemctl start ted-api"
echo "    systemctl status ted-api"
echo ""
echo " 3. API testen:"
echo "    curl http://localhost:8000/health"
echo "    curl http://localhost:8000/stats"
echo ""
echo " 4. Von aussen erreichbar unter:"
echo "    http://$(curl -s ifconfig.me):8000"
echo "============================================================"
