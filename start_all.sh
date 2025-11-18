#!/bin/bash

# Скрипт для запуска всех компонентов MetaFetcher в tmux
# Использование: ./start_all.sh

set -e

# Получаем директорию скрипта
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Имя tmux сессии
TMUX_SESSION="metafetcher"

echo "=== Запуск MetaFetcher ==="
echo "Рабочая директория: $SCRIPT_DIR"

# Шаг 1: Создание директории .results
echo ""
echo "1. Создание директории .results..."
mkdir -p .results
echo "✓ Директория .results создана"

# Шаг 2: Установка cloudflared
echo ""
echo "2. Запуск установки cloudflared..."
if [ -f "./install_cloudflared.sh" ]; then
    chmod +x ./install_cloudflared.sh
    ./install_cloudflared.sh
    echo "✓ cloudflared установлен"
else
    echo "⚠ Файл install_cloudflared.sh не найден, пропускаем установку"
fi

# Проверка наличия tmux
if ! command -v tmux &> /dev/null; then
    echo "❌ Ошибка: tmux не установлен. Установите tmux для продолжения."
    exit 1
fi

# Проверяем, существует ли уже сессия
if tmux has-session -t "$TMUX_SESSION" 2>/dev/null; then
    echo ""
    echo "⚠ Tmux сессия '$TMUX_SESSION' уже существует."
    read -p "Убить существующую сессию и создать новую? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        tmux kill-session -t "$TMUX_SESSION"
    else
        echo "Используйте: tmux attach -t $TMUX_SESSION"
        exit 0
    fi
fi

# Создаем новую tmux сессию
echo ""
echo "3. Создание tmux сессии '$TMUX_SESSION'..."
tmux new-session -d -s "$TMUX_SESSION" -c "$SCRIPT_DIR"

# Функция для создания нового окна и запуска команды
create_window() {
    local window_num=$1
    local window_name=$2
    local command=$3
    
    if [ $window_num -eq 1 ]; then
        # Первое окно уже создано, переименовываем его
        tmux rename-window -t "$TMUX_SESSION:0" "$window_name"
        tmux send-keys -t "$TMUX_SESSION:0" "$command" C-m
    else
        # Создаем новое окно
        tmux new-window -t "$TMUX_SESSION" -n "$window_name" -c "$SCRIPT_DIR"
        tmux send-keys -t "$TMUX_SESSION:$window_name" "$command" C-m
    fi
}

# Шаг 3: Окно 1 - fetcher2.py
echo "4. Запуск fetcher2.py в окне 1..."
create_window 1 "fetcher2" "python3 fetcher2.py"
sleep 1

# Шаг 4: Окно 2 - _yt_dlp/main_yt_dlp.py
echo "5. Запуск _yt_dlp/main_yt_dlp.py в окне 2..."
create_window 2 "yt_dlp" "python3 _yt_dlp/main_yt_dlp.py"
sleep 1

# Шаг 5: Окно 3 - main_hf.py
echo "6. Запуск main_hf.py в окне 3..."
create_window 3 "main_hf" "python3 main_hf.py"
sleep 1

# Шаг 6: Окно 4 - downloader.py
echo "7. Запуск downloader.py в окне 4..."
create_window 4 "downloader" "python3 downloader.py"
sleep 1

# Шаг 7: Окно 5 - metrics.py
echo "8. Запуск metrics.py в окне 5..."
create_window 5 "metrics" "python3 metrics.py"
sleep 1

# Шаг 8: Окно 6 - prometheus
echo "9. Запуск prometheus в окне 6..."
# Проверяем наличие prometheus
if command -v prometheus &> /dev/null; then
    PROMETHEUS_CMD="prometheus --config.file=\"$SCRIPT_DIR/prometheus.yml\" --web.listen-address=\":9090\""
elif [ -f "./prometheus" ]; then
    PROMETHEUS_CMD="./prometheus --config.file=\"$SCRIPT_DIR/prometheus.yml\" --web.listen-address=\":9090\""
else
    echo "⚠ prometheus не найден, используем команду 'prometheus' (может не работать)"
    PROMETHEUS_CMD="prometheus --config.file=\"$SCRIPT_DIR/prometheus.yml\" --web.listen-address=\":9090\""
fi
create_window 6 "prometheus" "$PROMETHEUS_CMD"
sleep 1

# Шаг 9: Окно 7 - cloudflared
echo "10. Запуск cloudflared в окне 7..."
if command -v cloudflared &> /dev/null; then
    create_window 7 "cloudflared" "cloudflared tunnel --url http://localhost:9090"
else
    echo "⚠ cloudflared не найден в PATH"
    create_window 7 "cloudflared" "cloudflared tunnel --url http://localhost:9090"
fi
sleep 1

echo ""
echo "=== Все компоненты запущены! ==="
echo ""
echo "Для подключения к tmux сессии используйте:"
echo "  tmux attach -t $TMUX_SESSION"
echo ""
echo "Для просмотра списка окон:"
echo "  tmux list-windows -t $TMUX_SESSION"
echo ""
echo "Для переключения между окнами (внутри tmux):"
echo "  Ctrl+b затем номер окна (0-6)"
echo ""
echo "Для выхода из tmux (не останавливая процессы):"
echo "  Ctrl+b затем d"
echo ""

