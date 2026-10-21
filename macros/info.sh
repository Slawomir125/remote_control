# Informacje o systemie — demonstruje potoki, przekierowania i zmienne shella
whoami
uname -a
echo "Procesy: $(ps aux | wc -l)"
df -h / | tail -1
echo "Pamięć:" && free -h | grep Mem
ls -la /tmp | head -5
