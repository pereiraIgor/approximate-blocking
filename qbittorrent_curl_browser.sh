#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   QBIT_HOST="http://localhost:8080" QBIT_USER="admin" QBIT_PASS="..." ./qbittorrent_curl_browser.sh

QBIT_HOST="${QBIT_HOST:-http://localhost:8080}"
QBIT_USER="${QBIT_USER:-admin}"
QBIT_PASS="${QBIT_PASS:-}"

if [[ -z "$QBIT_PASS" ]]; then
  echo "Erro: defina QBIT_PASS antes de executar."
  exit 1
fi

COOKIE_FILE="$(mktemp)"
trap 'rm -f "$COOKIE_FILE"' EXIT

echo "[1/4] Testando conexão com qBittorrent..."

# Tenta descobrir o host correto
HOSTS_TO_TRY=("${QBIT_HOST}" "http://localhost:8080" "http://127.0.0.1:8080" "http://qbittorrent:8080")
WORKING_HOST=""

for HOST in "${HOSTS_TO_TRY[@]}"; do
  echo "  Tentando: $HOST"
  if VERSION=$(curl -sS -f -m 3 "${HOST}/api/v2/app/version" 2>/dev/null); then
    WORKING_HOST="$HOST"
    echo "  ✓ Conectado! Versão: $VERSION"
    break
  fi
done

if [[ -z "$WORKING_HOST" ]]; then
  echo ""
  echo "ERRO: Nenhum host respondeu. Diagnóstico:"
  echo "  1. Container rodando: $(docker ps -q -f name=qbittorrent &>/dev/null && echo 'SIM' || echo 'NÃO')"
  echo "  2. Teste manual: curl -v http://localhost:8080/api/v2/app/version"
  echo "  3. Logs do container: docker logs qbittorrent --tail 50"
  exit 1
fi

# Atualiza para usar o host que funcionou
QBIT_HOST="$WORKING_HOST"

echo "[2/4] Autenticando no qBittorrent API..."
LOGIN_RESP="$(curl -sS -w "\nHTTP_CODE:%{http_code}" -c "$COOKIE_FILE" -X POST \
  -d "username=${QBIT_USER}&password=${QBIT_PASS}" \
  "${QBIT_HOST}/api/v2/auth/login")"

HTTP_CODE="$(echo "$LOGIN_RESP" | grep -oP 'HTTP_CODE:\K\d+')"
LOGIN_MSG="$(echo "$LOGIN_RESP" | grep -v "HTTP_CODE:")"

if [[ "$LOGIN_MSG" != "Ok." ]]; then
  echo "Falha no login. HTTP: $HTTP_CODE | Resposta: '$LOGIN_MSG'"
  echo "Verifique usuário/senha. Para ver senha temporária: docker logs qbittorrent 2>&1 | grep -i password"
  exit 1
fi
echo "Login OK"

echo "[3/4] Aplicando preferencias de gerenciamento de torrents..."
PREFERENCES_JSON='{"auto_tmm_enabled":true,"torrent_changed_tmm_enabled":true,"save_path_changed_tmm_enabled":true,"category_changed_tmm_enabled":true,"save_path":"/downloads"}'

curl -sS -b "$COOKIE_FILE" -X POST \
  --data-urlencode "json=${PREFERENCES_JSON}" \
  "${QBIT_HOST}/api/v2/app/setPreferences" >/dev/null
echo "Preferencias aplicadas"

echo "[4/4] Validando preferencias aplicadas..."
curl -sS -b "$COOKIE_FILE" "${QBIT_HOST}/api/v2/app/preferences" | \
  grep -E '"auto_tmm_enabled"|"torrent_changed_tmm_enabled"|"save_path_changed_tmm_enabled"|"category_changed_tmm_enabled"|"save_path"' || true

echo ""
echo "✓ Concluido. Configuracao aplicada com sucesso!"
