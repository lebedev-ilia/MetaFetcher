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
echo "1. Создание директории .results и .tmp"
mkdir -p .results
mkdir -p .tmp
echo "✓ Директория .results и .tmp созданы"

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
create_window 1 "fetcher2" ".venv/bin/python fetcher2.py"
sleep 1

# Шаг 4: Окно 2 - _yt_dlp/main_yt_dlp.py
echo "5. Запуск _yt_dlp/main_yt_dlp.py в окне 2..."
create_window 2 "yt_dlp" ".venv/bin/python _yt_dlp/main_yt_dlp.py"
sleep 1

# Шаг 5: Окно 3 - main_hf.py
echo "6. Запуск main_hf.py в окне 3..."
create_window 3 "main_hf" ".venv/bin/python main_hf.py"
sleep 1

# Шаг 6: Окно 4 - downloader.py
echo "7. Запуск downloader.py в окне 4..."
create_window 4 "downloader" ".venv/bin/python downloader.py"
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

