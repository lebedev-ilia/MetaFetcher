#!/bin/bash

# Скрипт установки cloudflared на Linux
# Использование: ./install_cloudflared.sh

set -e

echo "Установка cloudflared..."

# Проверяем, установлен ли уже cloudflared
if command -v cloudflared &> /dev/null; then
    echo "cloudflared уже установлен:"
    cloudflared --version
    read -p "Переустановить? (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 0
    fi
fi

# Определяем архитектуру
ARCH=$(uname -m)
if [ "$ARCH" = "x86_64" ]; then
    ARCH="amd64"
elif [ "$ARCH" = "aarch64" ]; then
    ARCH="arm64"
else
    echo "Неподдерживаемая архитектура: $ARCH"
    exit 1
fi

# Метод 1: Попытка установки через официальный репозиторий Cloudflare
install_via_repo() {
    echo "Попытка установки через официальный репозиторий Cloudflare..."
    
    # Определяем версию Ubuntu/Debian
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        CODENAME=$VERSION_CODENAME
        if [ -z "$CODENAME" ]; then
            # Пытаемся определить по другим полям
            if [[ "$ID" == "ubuntu" ]]; then
                CODENAME=$(echo $VERSION | cut -d' ' -f1 | cut -d'.' -f1,2)
                case $CODENAME in
                    "20.04") CODENAME="focal" ;;
                    "22.04") CODENAME="jammy" ;;
                    "24.04") CODENAME="noble" ;;
                    *) CODENAME="jammy" ;; # fallback
                esac
            elif [[ "$ID" == "debian" ]]; then
                CODENAME="bookworm"
            fi
        fi
    else
        CODENAME="jammy" # fallback
    fi
    
    echo "Используется кодовое имя: $CODENAME"
    
    # Добавляем GPG ключ
    curl -fsSL https://pkg.cloudflare.com/cloudflare-main.gpg | sudo tee /usr/share/keyrings/cloudflare-main.gpg > /dev/null
    
    # Добавляем репозиторий
    echo "deb [signed-by=/usr/share/keyrings/cloudflare-main.gpg] https://pkg.cloudflare.com/cloudflared $CODENAME main" | sudo tee /etc/apt/sources.list.d/cloudflared.list > /dev/null
    
    # Обновляем и устанавливаем
    sudo apt update
    sudo apt install -y cloudflared
    
    return $?
}

# Метод 2: Прямая установка .deb пакета
install_via_deb() {
    echo "Попытка установки через прямой .deb пакет..."
    
    TEMP_DIR=$(mktemp -d)
    cd "$TEMP_DIR"
    
    # Определяем URL для скачивания
    if [ "$ARCH" = "amd64" ]; then
        DEB_URL="https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb"
    elif [ "$ARCH" = "arm64" ]; then
        DEB_URL="https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64.deb"
    else
        echo "Неподдерживаемая архитектура для .deb пакета"
        return 1
    fi
    
    echo "Скачивание cloudflared из: $DEB_URL"
    wget -q "$DEB_URL" -O cloudflared.deb || {
        echo "Ошибка при скачивании"
        return 1
    }
    
    echo "Установка пакета..."
    sudo dpkg -i cloudflared.deb || {
        echo "Исправление зависимостей..."
        sudo apt-get install -f -y
        sudo dpkg -i cloudflared.deb
    }
    
    cd -
    rm -rf "$TEMP_DIR"
    return $?
}

# Метод 3: Прямая установка бинарника
install_via_binary() {
    echo "Попытка установки через прямой бинарник..."
    
    TEMP_DIR=$(mktemp -d)
    cd "$TEMP_DIR"
    
    BINARY_URL="https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${ARCH}"
    
    echo "Скачивание cloudflared из: $BINARY_URL"
    wget -q "$BINARY_URL" -O cloudflared || {
        echo "Ошибка при скачивании"
        return 1
    }
    
    chmod +x cloudflared
    sudo mv cloudflared /usr/local/bin/cloudflared
    
    cd -
    rm -rf "$TEMP_DIR"
    return $?
}

# Пытаемся установить разными методами
if install_via_repo; then
    echo "✓ cloudflared установлен через официальный репозиторий"
elif install_via_deb; then
    echo "✓ cloudflared установлен через .deb пакет"
elif install_via_binary; then
    echo "✓ cloudflared установлен через бинарник"
else
    echo "✗ Не удалось установить cloudflared"
    exit 1
fi

# Проверяем установку
if command -v cloudflared &> /dev/null; then
    echo ""
    echo "✓ Установка завершена успешно!"
    echo "Версия cloudflared:"
    cloudflared --version
else
    echo "✗ cloudflared не найден после установки"
    exit 1
fi

