import time
import base64

from nacl.public import PrivateKey, PublicKey, Box
from nacl.secret import SecretBox
from nacl.utils import random

from pydantic import BaseModel
from enum import Enum
from typing import Any

import asyncio

class MessageType(Enum):
    default = 'text'
    make_session = 'make_session'
    ack_session = 'ack_session'

class Session(BaseModel):
    shared_key: bytes
    last_activity: float
    writer: Any

class P2PMessage(BaseModel):
    sender_id: int          
    sender_pub_key: str     
    payload: str  
    type: str

class P2PClient:
    def __init__(self):
        self.private_key = PrivateKey.generate()
        self.public_key = self.private_key.public_key

        # {recipient_id: Session}
        self.sessions: dict[str, Session]= {}
        self.SESSION_TIMEOUT = 300

    def assymetric_box(self, sender_pub_key_bytes: bytes):
        return Box(self.private_key, PublicKey(sender_pub_key_bytes))
    
    def add_symmetric_session(self, recipient_id: str, writer: asyncio.StreamWriter, key: bytes = None):
        now = time.time()
        print(f"[*] Добавление новой симметричной сессии для {recipient_id}")
        if key is None: key = random(SecretBox.KEY_SIZE)
        print(key)
        self.sessions[recipient_id] = Session(
            shared_key=key, 
            last_activity=now,
            writer=writer
            )
        

    def encrypt_symmetric(self, recipient_id: str, pub_key_bytes: bytes, text: str):
        """Шифрование быстрым симметричным алгоритмом"""
        # Если сессии нет или она протухла — создаем новую
        now = time.time()
        if recipient_id not in self.sessions or (now - self.sessions[recipient_id].last_activity) > self.SESSION_TIMEOUT:
            self.add_symmetric_session(recipient_id, pub_key_bytes)
        
        session = self.sessions[recipient_id]
        session.last_activity = now # Обновляем время активности
        
        box = SecretBox(session.shared_key)
        encrypted = box.encrypt(text)
        
        return base64.b64encode(encrypted).decode()

    def decrypt_symmetric(self, sender_id: str, sender_pub_key_bytes: bytes, encrypted_b64: str):
        """Расшифровка симметричным ключом"""
        # Вычисляем ОБЩИЙ секрет (Diffie-Hellman)
        # Используем наш приватный ключ и ПУБЛИЧНЫЙ ключ того, кто прислал сообщение
        box_helper = Box(self.private_key, PublicKey(sender_pub_key_bytes))
        shared_key = box_helper.shared_key() 
        
        # Используем этот секрет в SecretBox
        secret_box = SecretBox(shared_key)
        
        encrypted_data = base64.b64decode(encrypted_b64)
        # Расшифровываем (nonce уже внутри encrypted_data, если ты юзал box.encrypt)
        return secret_box.decrypt(encrypted_data).decode('utf-8')
    
    def cleanup_sessions(self):
        """Метод для очистки старых сессий (запускать по таймеру)"""
        now = time.time()
        to_delete = [uid for uid, s in self.sessions.items() if now - s.last_activity > self.SESSION_TIMEOUT]
        for uid in to_delete:
            del self.sessions[uid]
            print(f"[!] Сессия с {uid} закрыта по таймауту. Ключи удалены.")