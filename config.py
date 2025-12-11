# config.py
import os

DB_NAME = "vibecheck.db"
BOT_TOKEN = "8587296967:AAGehLlzXh6IAMJVzitSpMB4vTBMFMi2s5s"

# ID (int) -> Имя для графиков (str)
USER_MAPPING = {
    435819434:  "Настик Сон",
    500990987:  "Санно",
    1042749461: "Надюха",
    617790282:  "Дианик"
}

# Маппинг тегов (@username -> Имя)
# ВАЖНО: Имена справа должны совпадать с именами в USER_MAPPING, чтобы матрица была красивой
TAG_MAPPING = {
    '@narrinan': 'Надюха',
    '@ariesalexan': 'Санно',
    '@pryanik_dianik': 'Дианик',
    '@staandee': 'Настик Сон'
}

# ID ботов и сервисных аккаунтов
IGNORE_IDS = [
    777000,      # Telegram Service Notification
]