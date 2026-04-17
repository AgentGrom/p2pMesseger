import time
import base64

from nacl.public import PrivateKey, PublicKey, Box
from nacl.secret import SecretBox
from nacl.utils import random

from pydantic import BaseModel

class Session(BaseModel):
    shared_key: bytes
    last_activity: float

class P2PMessage(BaseModel):
    sender_id: str          
    sender_pub_key: str     
    encrypted_payload: str  
    type: str = "text"

class P2PClient:
    def __init__(self):
        self.private_key = PrivateKey.generate()
        self.public_key = self.private_key.public_key

        # {recipient_id: Session}
        self.sessions: dict[str, Session]= {}
        self.SESSION_TIMEOUT = 300

    def get_shared_key(self, recipient_pub_key_bytes: bytes):
        """Алгоритм Диффи-Хеллмана для получения общего ключа"""
        recipient_pub_key = PublicKey(recipient_pub_key_bytes)
        # Генерируем общий секрет на основе двух пар ключей
        shared_box = Box(self.private_key, recipient_pub_key)
        # shared_key — это производное от секрета, используем его для симметрии
        return shared_box.shared_key()
    
    def encrypt_symmetric(self, recipient_id: str, pub_key_bytes: bytes, text: str):
        """Шифрование быстрым симметричным алгоритмом"""
        # Если сессии нет или она протухла — создаем новую
        now = time.time()
        if recipient_id not in self.sessions or (now - self.sessions[recipient_id].last_activity) > self.SESSION_TIMEOUT:
            print(f"[*] Установка новой симметричной сессии для {recipient_id}")
            key = self.get_shared_key(pub_key_bytes)
            self.sessions[recipient_id] = Session(shared_key=key, last_activity=now)
        
        session = self.sessions[recipient_id]
        session.last_activity = now # Обновляем время активности
        
        box = SecretBox(session.shared_key)
        nonce = random(SecretBox.NONCE_SIZE)
        encrypted = box.encrypt(text.encode(), nonce)
        
        return base64.b64encode(encrypted).decode()

    def decrypt_symmetric(self, sender_id: str, sender_pub_key_bytes: bytes, encrypted_b64: str):
        """Расшифровка симметричным ключом"""
        key = self.get_shared_key(sender_pub_key_bytes) # Вычисляем тот же ключ
        box = SecretBox(key)
        
        encrypted_data = base64.b64decode(encrypted_b64)
        return box.decrypt(encrypted_data).decode()

    def cleanup_sessions(self):
        """Метод для очистки старых сессий (запускать по таймеру)"""
        now = time.time()
        to_delete = [uid for uid, s in self.sessions.items() if now - s.last_activity > self.SESSION_TIMEOUT]
        for uid in to_delete:
            del self.sessions[uid]
            print(f"[!] Сессия с {uid} закрыта по таймауту. Ключи удалены.")