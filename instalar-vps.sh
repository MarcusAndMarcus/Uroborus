#!/usr/bin/env bash
# Instala o UROBOROS numa VPS Debian/Ubuntu limpa, atrás do Caddy com TLS.
# Rode como root:  bash instalar-vps.sh uroboros.seudominio.com.br
set -euo pipefail

DOMINIO="${1:-}"
[ -z "$DOMINIO" ] && { echo "uso: bash instalar-vps.sh SEU.DOMINIO"; exit 1; }
REPO="${REPO:-}"                     # opcional: URL git; sem isso, envie os arquivos por scp/rsync
DIR=/opt/uroboros

echo "==> pacotes"
apt-get update -qq
apt-get install -y -qq curl ca-certificates gnupg ufw rsync

if ! command -v node >/dev/null || [ "$(node -p 'process.versions.node.split(".")[0]')" -lt 20 ]; then
  echo "==> node 22"
  curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
  apt-get install -y -qq nodejs
fi

if ! command -v caddy >/dev/null; then
  echo "==> caddy"
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' > /etc/apt/sources.list.d/caddy-stable.list
  apt-get update -qq && apt-get install -y -qq caddy
fi

echo "==> usuário e diretórios"
id -u uroboros >/dev/null 2>&1 || useradd --system --home "$DIR" --shell /usr/sbin/nologin uroboros
mkdir -p "$DIR"/{jobs,corpus}

if [ -n "$REPO" ]; then
  echo "==> código de $REPO"
  apt-get install -y -qq git
  [ -d "$DIR/.git" ] && git -C "$DIR" pull --ff-only || git clone --depth 1 "$REPO" "$DIR"
else
  echo "==> envie server.js e public/ para $DIR (scp/rsync) se ainda não estiverem lá"
fi
chown -R uroboros:uroboros "$DIR"

if [ ! -f /etc/uroboros.env ]; then
  echo "==> ambiente"
  TOKEN=$(head -c 24 /dev/urandom | base64 | tr -d '/+=' | head -c 32)
  cat > /etc/uroboros.env <<EOF
ANTHROPIC_API_KEY=
GEMINI_API_KEY=
ACCESS_TOKEN=$TOKEN
HOST=127.0.0.1
PORT=8080
CLUSTERS=10
CONCURRENCY=4
JOBS_DIR=$DIR/jobs
CORPUS_DIR=$DIR/corpus
EOF
  chmod 600 /etc/uroboros.env
  echo "    token de acesso gerado: $TOKEN"
  echo "    PREENCHA as chaves de API em /etc/uroboros.env antes de usar"
fi

echo "==> systemd"
install -m 644 "$DIR/deploy/uroboros.service" /etc/systemd/system/uroboros.service 2>/dev/null || true
systemctl daemon-reload
systemctl enable --now uroboros

echo "==> caddy"
sed "s/uroboros.seudominio.com.br/$DOMINIO/" "$DIR/deploy/Caddyfile" > /etc/caddy/Caddyfile
systemctl reload caddy || systemctl restart caddy

echo "==> firewall"
ufw allow OpenSSH >/dev/null
ufw allow 80/tcp >/dev/null
ufw allow 443/tcp >/dev/null
ufw --force enable >/dev/null

echo
echo "pronto: https://$DOMINIO"
echo "logs:   journalctl -u uroboros -f"
echo "saúde:  curl -s https://$DOMINIO/api/health"
