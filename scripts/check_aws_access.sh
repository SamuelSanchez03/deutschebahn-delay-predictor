#!/usr/bin/env bash
# Verifica que los dos usuarios IAM tengan EXACTAMENTE los permisos esperados.
# Prueba lo que debe funcionar y también lo que debe fallar (mínimo privilegio).
#
# Uso: scripts/check_aws_access.sh <bucket>
# Requiere los perfiles de AWS CLI "db-gh-actions" y "db-databricks"
# (aws configure --profile <nombre>).
set -u

BUCKET="${1:?Uso: $0 <bucket>}"
TMP="$(mktemp)"
echo "prueba de permisos $(date -u +%FT%TZ)" > "$TMP"
FAILS=0

# check <perfil> <esperado: ok|deny> <descripción> <comando aws...>
check() {
  local profile="$1" expected="$2" desc="$3"; shift 3
  if out=$(aws --profile "$profile" "$@" 2>&1); then got=ok; else got=deny; fi
  if [[ "$got" == "$expected" ]]; then
    printf '  ✅ %-58s (%s, esperado)\n' "$desc" "$got"
  else
    printf '  ❌ %-58s (%s, se esperaba %s)\n' "$desc" "$got" "$expected"
    printf '     %s\n' "$(echo "$out" | tail -1)"
    FAILS=$((FAILS + 1))
  fi
}

for p in db-gh-actions db-databricks; do
  echo
  echo "== Perfil $p =="
  check "$p" ok "Identidad (sts get-caller-identity)" sts get-caller-identity --query Arn --output text
  check "$p" ok "Listar el bucket" s3 ls "s3://$BUCKET"
done

echo
echo "== db-gh-actions: escribe landing/, solo lee exports/ =="
check db-gh-actions ok   "Subir a landing/"           s3 cp "$TMP" "s3://$BUCKET/landing/_check.txt"
check db-gh-actions ok   "Leer de landing/"           s3 cp "s3://$BUCKET/landing/_check.txt" -
check db-gh-actions deny "Subir a exports/ (prohibido)" s3 cp "$TMP" "s3://$BUCKET/exports/_check.txt"

echo
echo "== db-databricks: lee landing/, escribe exports/ =="
check db-databricks ok   "Leer de landing/"             s3 cp "s3://$BUCKET/landing/_check.txt" -
check db-databricks deny "Subir a landing/ (prohibido)" s3 cp "$TMP" "s3://$BUCKET/landing/_check2.txt"
check db-databricks ok   "Subir a exports/"             s3 cp "$TMP" "s3://$BUCKET/exports/_check.txt"
check db-databricks ok   "Borrar de exports/"           s3 rm "s3://$BUCKET/exports/_check.txt"

echo
echo "== Seguridad del bucket =="
check db-gh-actions deny "Borrar de landing/ (prohibido para ambos)" s3 rm "s3://$BUCKET/landing/_check.txt"

# Limpieza: ningún usuario puede borrar en landing/, así que el archivo de prueba
# se queda ahí (pesa unos bytes). Bórralo desde la consola o con tu usuario administrador.
rm -f "$TMP"
echo
if [[ $FAILS -eq 0 ]]; then
  echo "🎉 Todo correcto: los permisos coinciden con las políticas."
  echo "   (Queda s3://$BUCKET/landing/_check.txt; bórralo desde la consola de S3.)"
else
  echo "⚠️  $FAILS prueba(s) no coinciden. Revisa las políticas en docs/aws/."
  exit 1
fi
