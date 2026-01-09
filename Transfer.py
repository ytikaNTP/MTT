import asyncio
import hashlib
import logging
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Set, Dict, Optional, List
from dataclasses import dataclass, field

from pymax import SocketMaxClient
from pymax.types import Message, PhotoAttach, VideoAttach, FileAttach
from pymax.payloads import UserAgentPayload
import aiohttp
from telegram import Bot
from telegram.constants import ParseMode

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class Config:
    """Конфигурация бота"""
    # Max настройки
    max_phone: str
    max_chat_ids: List[int]  # ID чатов для мониторинга
    
    # Telegram настройки
    telegram_token: str
    telegram_chat_id: int  # ID целевого чата/канала
    
    # Настройки обработки
    max_work_dir: str = "./max_sessions"
    download_path: str = "./downloads"
    state_file: str = "./bot_state.json"
    duplicate_cache_ttl_hours: int = 168  # 7 дней
    max_media_per_group: int = 10  # Макс. медиа в альбоме Telegram
    request_timeout: int = 30
    
    # Настройки SocketMaxClient
    device_type: str = "DESKTOP"  # DESKTOP, ANDROID, IOS
    max_headers: Optional[Dict] = None
    
    def __post_init__(self):
        self.max_headers = UserAgentPayload(device_type=self.device_type)

class StateManager:
    """Управление состоянием бота"""
    
    def __init__(self, state_file: str):
        self.state_file = Path(state_file)
        self.processed_messages: Set[str] = set()
        self.load()
    
    def load(self):
        """Загружает состояние из файла"""
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processed_messages = set(data.get('processed_messages', []))
                logger.info(f"Loaded {len(self.processed_messages)} processed messages")
        except Exception as e:
            logger.error(f"Error loading state: {e}")
            self.processed_messages = set()
    
    def save(self):
        """Сохраняет состояние в файл"""
        try:
            data = {
                'processed_messages': list(self.processed_messages),
                'last_save': datetime.now().isoformat()
            }
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"State saved: {len(self.processed_messages)} messages")
        except Exception as e:
            logger.error(f"Error saving state: {e}")
    
    def add_message(self, message_hash: str):
        """Добавляет сообщение в обработанные"""
        self.processed_messages.add(message_hash)
        # Автосохранение при изменении
        if len(self.processed_messages) % 10 == 0:
            self.save()
    
    def is_processed(self, message_hash: str) -> bool:
        """Проверяет, было ли сообщение обработано"""
        return message_hash in self.processed_messages



class MaxForwarderBot:
    """Основной бот для пересылки сообщений из Max в Telegram"""
    
    def __init__(self, config: Config):
        self.config = config
        self.state_manager = StateManager(config.state_file)
        
        # Инициализация клиента Max
        self.max_client = SocketMaxClient(
            phone=config.max_phone,
            work_dir=config.max_work_dir,
            headers=config.max_headers,
            registration=False  # Вход по существующему аккаунту
        )
        
        # Инициализация Telegram бота
        self.bot = Bot(token=config.telegram_token)
        self.chat_id = config.telegram_chat_id
        
        # Регистрация обработчиков
        self._register_handlers()
    
    def _register_handlers(self):
        """Регистрирует обработчики событий Max"""
        
        # Регистрация обработчика сообщений (временно без фильтра для отладки)
        self.max_client.on_message()(self.process_message)
        
        # Регистрация обработчика старта
        self.max_client.on_start(handler=self._on_start)
        
        # Регистрация периодической задачи
        self.max_client.task(seconds=3600)(self._periodic_save)  # 1 час = 3600 секунд
    
    async def _on_start(self):
        """Обработчик события старта Max клиента"""
        logger.info(f"Max client started! ID: {self.max_client.me.id}")
        logger.info(f"Monitoring chats: {self.config.max_chat_ids}")
    
    async def _periodic_save(self):
        """Периодическое сохранение состояния"""
        self.state_manager.save()
        logger.info("Periodic state save completed")
    
    def _generate_message_hash(self, message: Message) -> str:
        """Генерирует уникальный хэш для сообщения"""
        # Базовые данные
        hash_data = f"{message.chat_id}:{message.id}:{message.text or ''}"
        
        # Добавляем информацию о вложениях
        if message.attaches:
            for attach in message.attaches:
                if isinstance(attach, PhotoAttach):
                    hash_data += f":photo_{attach.photo_id}"
                elif isinstance(attach, VideoAttach):
                    hash_data += f":video_{attach.video_id}"
                elif isinstance(attach, FileAttach):
                    hash_data += f":file_{attach.file_id}"
                elif isinstance(attach, AudioAttach):
                    hash_data += f":audio_{attach.audio_id}"
        
        # Добавляем время сообщения (в секундах)
        hash_data += f":{message.time / 1000}"
        
        return hashlib.sha256(hash_data.encode()).hexdigest()
    
    def _format_message_text(self, message: Message) -> str:
        """Форматирует текст сообщения для Telegram"""
        text = message.text or ""
        
        # Форматируем текст: сначала текст сообщения, затем подпись
        formatted = ""
        
        if text:
            formatted += f"{text}\n"
        
        # Добавляем подпись
        formatted += "*сообщение обработано и отправлено ботом"
        
        return formatted
    
    async def process_message(self, message: Message):
        """Обрабатывает входящее сообщение из Max"""
        try:
            # Генерируем хэш сообщения
            message_hash = self._generate_message_hash(message)
            
            # Проверка на дублирование
            if self.state_manager.is_processed(message_hash):
                logger.info(f"Duplicate message ignored: {message.id}")
                return
            
            logger.info(f"New message from Max chat {message.chat_id} (ID: {message.id})")
            
            # Форматируем текст
            text = self._format_message_text(message)
            
            # Обрабатываем вложения
            media_links = []
            if message.attaches:
                media_links = await self._get_media_links(message)
            
            # Формируем итоговый текст с ссылками на медиа
            final_text = ""
            if media_links:
                # Сначала добавляем ссылки на медиа
                final_text += "\n".join(media_links) + "\n\n"
            
            # Добавляем основной текст сообщения
            final_text += text
            
            # Отправляем в Telegram
            try:
                # Разбиваем длинные сообщения (Telegram лимит 4096 символов)
                max_length = 4000
                if len(final_text) <= max_length:
                    await self.bot.send_message(
                        chat_id=self.chat_id,
                        text=final_text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True
                    )
                else:
                    # Разбиваем на части
                    parts = [final_text[i:i+max_length] for i in range(0, len(final_text), max_length)]
                    for i, part in enumerate(parts):
                        prefix = f"<i>(Часть {i+1}/{len(parts)})</i>\n\n" if len(parts) > 1 else ""
                        await self.bot.send_message(
                            chat_id=self.chat_id,
                            text=prefix + part,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True
                        )
                
                logger.info("Sent text message")
            except Exception as e:
                logger.error(f"Error sending text: {e}")
                raise
            
            # Помечаем как обработанное
            self.state_manager.add_message(message_hash)
            
            logger.info(f"Message {message.id} processed successfully")
            
        except Exception as e:
            logger.error(f"Error processing message {message.id}: {e}")
    
    async def _get_media_links(self, message: Message) -> List[str]:
        """Формирует HTML ссылки на вложения сообщения"""
        media_links = []
        
        for i, attach in enumerate(message.attaches):
            try:
                if isinstance(attach, PhotoAttach):
                    # Формируем HTML ссылку на фото
                    if attach.base_url:
                        media_links.append(f'<a href="{attach.base_url}">ФОТО #{i+1}</a>')
                
                elif isinstance(attach, VideoAttach):
                    # Формируем HTML ссылку на видео
                    video_request = await self.max_client.get_video_by_id(
                        chat_id=message.chat_id,
                        message_id=message.id,
                        video_id=attach.video_id
                    )
                    if video_request and video_request.url:
                        media_links.append(f'<a href="{video_request.url}">ВИДЕО #{i+1}</a>')
                
                elif isinstance(attach, FileAttach):
                    # Формируем HTML ссылку на файл
                    file_request = await self.max_client.get_file_by_id(
                        chat_id=message.chat_id,
                        message_id=message.id,
                        file_id=attach.file_id
                    )
                    if file_request and file_request.url:
                        media_links.append(f'<a href="{file_request.url}">ФАЙЛ #{i+1}</a>')
                
                else:
                    logger.warning(f"Unsupported attachment type: {type(attach)}")
                    continue
                    
            except Exception as e:
                logger.error(f"Error processing attachment: {e}")
        
        logger.info(f"Generated {len(media_links)} HTML links")
        return media_links
    
    async def start(self):
        """Запускает бота"""
        logger.info("Starting Max to Telegram forwarder bot...")
        logger.info(f"Max phone: {self.config.max_phone}")
        logger.info(f"Telegram chat ID: {self.config.telegram_chat_id}")
        logger.info(f"Monitoring Max chats: {self.config.max_chat_ids}")
        
        try:
            # Запускаем Max клиент
            await self.max_client.start()
            
        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
        except Exception as e:
            logger.error(f"Bot error: {e}")
            raise
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Очистка ресурсов"""
        logger.info("Cleaning up resources...")
        self.state_manager.save()
        
        if hasattr(self, 'max_client') and self.max_client:
            await self.max_client.close()
        
        logger.info("Cleanup completed")

def load_config_from_env() -> Config:
    """Загружает конфигурацию из .env файла"""
    # Читаем .env файл вручную
    env_vars = {}
    try:
        with open('.env', 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key.strip()] = value.strip()
    except FileNotFoundError:
        logger.warning(".env file not found, using environment variables")
        env_vars = os.environ
    
    # Парсим Max чаты (разделенные запятыми)
    max_chat_ids_str = env_vars.get("MAX_CHAT_IDS", "")
    max_chat_ids = []
    if max_chat_ids_str:
        try:
            max_chat_ids = [int(chat_id.strip()) for chat_id in max_chat_ids_str.split(",")]
        except ValueError as e:
            logger.error(f"Error parsing MAX_CHAT_IDS: {e}")
            raise ValueError("Invalid MAX_CHAT_IDS format in .env file")
    
    return Config(
        max_phone=env_vars.get("MAX_PHONE", ""),
        max_chat_ids=max_chat_ids,
        telegram_token=env_vars.get("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=int(env_vars.get("TELEGRAM_CHAT_ID", 0)),
        max_work_dir=env_vars.get("MAX_WORK_DIR", "./max_sessions"),
        download_path=env_vars.get("DOWNLOAD_PATH", "./downloads"),
        state_file=env_vars.get("STATE_FILE", "./bot_state.json"),
        duplicate_cache_ttl_hours=int(env_vars.get("DUPLICATE_CACHE_TTL_HOURS", 168)),
        max_media_per_group=int(env_vars.get("MAX_MEDIA_PER_GROUP", 10)),
        request_timeout=int(env_vars.get("REQUEST_TIMEOUT", 30)),
        device_type=env_vars.get("MAX_DEVICE_TYPE", "DESKTOP")
    )

def main():
    """Точка входа"""
    try:
        # Загружаем конфигурацию из .env файла
        config = load_config_from_env()
        
        # Проверяем обязательные параметры
        if not config.max_phone:
            raise ValueError("MAX_PHONE is required in .env file")
        if not config.telegram_token:
            raise ValueError("TELEGRAM_BOT_TOKEN is required in .env file")
        if config.telegram_chat_id == 0:
            raise ValueError("TELEGRAM_CHAT_ID is required in .env file")
        if not config.max_chat_ids:
            raise ValueError("MAX_CHAT_IDS is required in .env file")
        
        logger.info("Configuration loaded successfully from .env file")
        logger.info(f"Max phone: {config.max_phone}")
        logger.info(f"Telegram chat ID: {config.telegram_chat_id}")
        logger.info(f"Monitoring Max chats: {config.max_chat_ids}")
        
        # Запуск бота
        bot = MaxForwarderBot(config)
        asyncio.run(bot.start())
        
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()
