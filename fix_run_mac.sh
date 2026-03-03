#!/bin/bash
# Убирает ошибку «повреждён» на Mac: карантин и окончания строк CRLF

cd "$(dirname "$0")"

# Снимаем карантин с файлов (исправляет «повреждён» при запуске)
xattr -cr run.command run_bot.command 2>/dev/null || true

# Убираем CRLF-окончания строк у .command-скриптов
sed -i '' $'s/\r$//' run.command 2>/dev/null || true
sed -i '' $'s/\r$//' run_bot.command 2>/dev/null || true

# Делаем файлы исполняемыми
chmod +x run.command run_bot.command 2>/dev/null || true

echo "Готово. Теперь можно дважды щёлкнуть по run.command и run_bot.command"
