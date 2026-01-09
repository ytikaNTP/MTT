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
from pymax.filters import Filters
from pymax.types import Message, PhotoAttach, VideoAttach, FileAttach, AudioAttach
from pymax.payloads import UserAgentPayload
from pymax.files import Photo, Video, File
import aiohttp
from telegram import Bot, InputMediaPhoto, InputMediaVideo, InputMediaDocument
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

class MediaDownloader:
    """Загрузка медиафайлов из Max"""
    
    def __init__(self, download_path: str, timeout: int = 30):
        self.download_path = Path(download_path)
        self.download_path.mkdir(exist_ok=True)
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=10)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=self.timeout
        )
        return self
        
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
    
    async def download_file(self, url: str, filepath: Path) -> bool:
        """Скачивает файл по URL"""
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    # Скачиваем файл синхронно, так как aiofiles недоступен
                    content = await response.read()
                    with open(filepath, 'wb') as f:
                        f.write(content)
                    logger.info(f"Downloaded: {filepath.name}")
                    return True
                else:
                    logger.error(f"Failed to download {url}: HTTP {response.status}")
                    return False
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return False
    
    def get_unique_filename(self, base_name: str, ext: str) -> Path:
        """Генерирует уникальное имя файла"""
        counter = 0
        while True:
            if counter == 0:
                filename = f"{base_name}.{ext}"
            else:
                filename = f"{base_name}_{counter}.{ext}"
            
            filepath = self.download_path / filename
            if not filepath.exists():
                return filepath
            counter += 1

class MaxMediaHandler:
    """Обработчик медиа из Max"""
    
    def __init__(self, max_client: SocketMaxClient, downloader: MediaDownloader):
        self.max_client = max_client
        self.downloader = downloader
    
    async def download_photo(self, photo_attach: PhotoAttach) -> Optional[Path]:
        """Скачивает фото из Max"""
        try:
            # Получаем фото через клиент
            photo = Photo(
                photo_id=photo_attach.photo_id,
                token=photo_attach.photo_token
            )
            
            # Генерируем URL для скачивания
            # В реальности нужно использовать методы клиента для получения файла
            # Для примера используем base_url
            if photo_attach.base_url:
                filename = self.downloader.get_unique_filename(
                    f"photo_{photo_attach.photo_id}", 
                    "jpg"
                )
                if await self.downloader.download_file(photo_attach.base_url, filename):
                    return filename
        except Exception as e:
            logger.error(f"Error downloading photo {photo_attach.photo_id}: {e}")
        return None
    
    async def download_video(self, video_attach: VideoAttach, chat_id: int, message_id: int) -> Optional[Path]:
        """Скачивает видео из Max"""
        try:
            # Получаем видео через клиент
            video_request = await self.max_client.get_video_by_id(
                chat_id=chat_id,
                message_id=message_id,
                video_id=video_attach.video_id
            )
            
            if video_request and video_request.url:
                filename = self.downloader.get_unique_filename(
                    f"video_{video_attach.video_id}",
                    "mp4"
                )
                if await self.downloader.download_file(video_request.url, filename):
                    return filename
        except Exception as e:
            logger.error(f"Error downloading video {video_attach.video_id}: {e}")
        return None
    
    async def download_file(self, file_attach: FileAttach, chat_id: int, message_id: int) -> Optional[Path]:
        """Скачивает файл из Max"""
        try:
            # Получаем файл через клиент
            file_request = await self.max_client.get_file_by_id(
                chat_id=chat_id,
                message_id=message_id,
                file_id=file_attach.file_id
            )
            
            if file_request and file_request.url:
                # Определяем расширение из имени файла
                ext = file_attach.name.split('.')[-1] if '.' in file_attach.name else 'bin'
                filename = self.downloader.get_unique_filename(
                    f"file_{file_attach.file_id}",
                    ext
                )
                if await self.downloader.download_file(file_request.url, filename):
                    return filename
        except Exception as e:
            logger.error(f"Error downloading file {file_attach.file_id}: {e}")
        return None

class TelegramSender:
    """Отправка сообщений в Telegram"""
    
    def __init__(self, token: str, chat_id: int, max_media_per_group: int = 10):
        self.bot = Bot(token=token)
        self.chat_id = chat_id
        self.max_media_per_group = max_media_per_group
    
    async def send_message_with_media(self, text: str, media_files: List[Path]):
        """Отправляет сообщение с медиа в Telegram"""
        try:
            if not media_files:
                # Только текст
                await self.send_text(text)
                return
            
            # Группируем медиа по типам
            photos = [f for f in media_files if f.suffix.lower() in ['.jpg', '.jpeg', '.png', '.gif']]
            videos = [f for f in media_files if f.suffix.lower() in ['.mp4', '.mov', '.avi', '.mkv']]
            documents = [f for f in media_files if f not in photos + videos]
            
            # Отправляем фото (можно группировать)
            if photos:
                await self._send_photo_group(photos, text)
                text = ""  # Текст уже отправлен с фото
            
            # Отправляем видео (по одному, т.к. группировка видео не всегда работает)
            if videos:
                for video in videos:
                    await self._send_single_video(video, text if text else None)
                    text = ""  # Текст уже отправлен с первым видео
            
            # Отправляем документы
            if documents:
                for doc in documents:
                    await self._send_document(doc, text if text else None)
                    text = ""
            
            # Если остался текст и не было медиафайлов
            if text and not (photos or videos or documents):
                await self.send_text(text)
                
        except Exception as e:
            logger.error(f"Error sending message with media: {e}")
            raise
    
    async def _send_photo_group(self, photos: List[Path], caption: str = ""):
        """Отправляет группу фото"""
        media_group = []
        
        for i, photo_path in enumerate(photos[:self.max_media_per_group]):
            with open(photo_path, 'rb') as f:
                media = InputMediaPhoto(
                    media=f,
                    caption=caption if i == 0 else "",
                    parse_mode=ParseMode.HTML
                )
                media_group.append(media)
        
        if media_group:
            await self.bot.send_media_group(
                chat_id=self.chat_id,
                media=media_group
            )
            logger.info(f"Sent photo group with {len(media_group)} photos")
    
    async def _send_single_video(self, video_path: Path, caption: str = None):
        """Отправляет одно видео"""
        with open(video_path, 'rb') as f:
            await self.bot.send_video(
                chat_id=self.chat_id,
                video=f,
                caption=caption,
                parse_mode=ParseMode.HTML,
                supports_streaming=True
            )
            logger.info(f"Sent video: {video_path.name}")
    
    async def _send_document(self, doc_path: Path, caption: str = None):
        """Отправляет документ"""
        with open(doc_path, 'rb') as f:
            await self.bot.send_document(
                chat_id=self.chat_id,
                document=f,
                caption=caption,
                parse_mode=ParseMode.HTML
            )
            logger.info(f"Sent document: {doc_path.name}")
    
    async def send_text(self, text: str):
        """Отправляет текстовое сообщение"""
        try:
            # Разбиваем длинные сообщения (Telegram лимит 4096 символов)
            max_length = 4000
            if len(text) <= max_length:
                await self.bot.send_message(
                    chat_id=self.chat_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True
                )
            else:
                # Разбиваем на части
                parts = [text[i:i+max_length] for i in range(0, len(text), max_length)]
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
        
        # Инициализация компонентов
        self.downloader = MediaDownloader(config.download_path, config.request_timeout)
        self.media_handler = MaxMediaHandler(self.max_client, self.downloader)
        self.telegram_sender = TelegramSender(
            config.telegram_token,
            config.telegram_chat_id,
            config.max_media_per_group
        )
        
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
            media_files = []
            media_links = []
            if message.attaches:
                media_files, media_links = await self._download_attachments(message)
            
            # Формируем итоговый текст с ссылками на медиа
            final_text = ""
            if media_links:
                # Сначала добавляем ссылки на медиа
                final_text += "\n".join(media_links) + "\n\n"
            
            # Добавляем основной текст сообщения
            final_text += text
            
            # Отправляем в Telegram
            await self.telegram_sender.send_message_with_media(final_text, media_files)
            
            # Помечаем как обработанное
            self.state_manager.add_message(message_hash)
            
            # Очищаем временные файлы
            for filepath in media_files:
                try:
                    filepath.unlink()
                except Exception as e:
                    logger.warning(f"Failed to delete temp file {filepath}: {e}")
            
            logger.info(f"Message {message.id} processed successfully")
            
        except Exception as e:
            logger.error(f"Error processing message {message.id}: {e}")
    
    async def _download_attachments(self, message: Message) -> tuple[List[Path], List[str]]:
        """Скачивает все вложения сообщения и возвращает файлы и ссылки"""
        media_files = []
        media_links = []
        
        async with self.downloader:
            for i, attach in enumerate(message.attaches):
                try:
                    if isinstance(attach, PhotoAttach):
                        filepath = await self.media_handler.download_photo(attach)
                        if filepath:
                            media_files.append(filepath)
                        else:
                            # Если не удалось скачать, добавляем кликабельную ссылку
                            if attach.base_url:
                                media_links.append(f"[ФОТО #{i+1}]({attach.base_url})")
                    
                    elif isinstance(attach, VideoAttach):
                        filepath = await self.media_handler.download_video(
                            attach, message.chat_id, message.id
                        )
                        if filepath:
                            media_files.append(filepath)
                        else:
                            # Если не удалось скачать, добавляем кликабельную ссылку
                            video_request = await self.max_client.get_video_by_id(
                                chat_id=message.chat_id,
                                message_id=message.id,
                                video_id=attach.video_id
                            )
                            if video_request and video_request.url:
                                media_links.append(f"[ВИДЕО #{i+1}]({video_request.url})")
                    
                    elif isinstance(attach, FileAttach):
                        filepath = await self.media_handler.download_file(
                            attach, message.chat_id, message.id
                        )
                        if filepath:
                            media_files.append(filepath)
                        else:
                            # Если не удалось скачать, добавляем кликабельную ссылку
                            file_request = await self.max_client.get_file_by_id(
                                chat_id=message.chat_id,
                                message_id=message.id,
                                file_id=attach.file_id
                            )
                            if file_request and file_request.url:
                                media_links.append(f"[ФАЙЛ #{i+1}]({file_request.url})")
                    
                    else:
                        logger.warning(f"Unsupported attachment type: {type(attach)}")
                        continue
                        
                except Exception as e:
                    logger.error(f"Error processing attachment: {e}")
        
        logger.info(f"Downloaded {len(media_files)} attachments, {len(media_links)} links")
        return media_files, media_links
    
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
