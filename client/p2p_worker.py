import asyncio
import base64
import json
from client.core import P2PClient, P2PMessage 

class P2PWorker:
    def __init__(self, host: str, port: int, user_id: str):
        self.client = P2PClient()
        self.host = host
        self.port = port
        self.user_id = user_id
        
        # Активные сокеты: { "ip:port": (reader, writer) }
        self.active_connections = {}
        # Записная книжка: { "Никнейм": {"ip": "...", "port": ..., "pub_key": "..."} }
        self.contacts = {}

    async def start(self):
        server = await asyncio.start_server(self.handle_incoming, self.host, self.port)
        pub_key_str = base64.b64encode(self.client.public_key.encode()).decode()
        
        print(f"\n[SYSTEM] Узел {self.user_id} запущен на {self.host}:{self.port}")
        print(f"[SYSTEM] Твой публичный ключ: {pub_key_str}")
        
        async with server:
            await server.serve_forever()

    async def handle_incoming(self, reader, writer):
        peer = writer.get_extra_info('peername')
        peer_ip = peer[0]
        
        try:
            while True:
                data = await reader.read(8192)
                if not data: break
                
                raw_data = data.decode()
                msg_dict = json.loads(raw_data)
                
                # АВТО-КОНТАКТ: Если ника нет в базе — добавляем
                nickname = msg_dict.get("sender_id")
                if nickname and nickname not in self.contacts:
                    self.contacts[nickname] = {
                        "ip": peer_ip,
                        "port": peer[1], 
                        "pub_key": msg_dict.get("sender_pub_key")
                    }
                    print(f"\n[+] Контакт '{nickname}' добавлен автоматически ({peer_ip})")

                # Расшифровка текста
                if msg_dict.get("encrypted_payload"):
                    msg = P2PMessage(**msg_dict)
                    sender_pub_key = base64.b64decode(msg.sender_pub_key)
                    
                    decrypted_text = self.client.decrypt_symmetric(
                        msg.sender_id, sender_pub_key, msg.encrypted_payload
                    )
                    
                    print(f"\n[{msg.sender_id}]: {decrypted_text}")
                    print(f"[{self.user_id}] > ", end="", flush=True)

        except Exception as e:
            print(f"\n[!] Ошибка связи с {peer_ip}: {e}")
        finally:
            writer.close()

    async def send_to_contact(self, alias: str, text: str):
        """Отправка сообщения по никнейму из записной книжки"""
        if alias not in self.contacts:
            print(f"[!] Ошибка: Контакта '{alias}' нет в списке!")
            return
        
        c = self.contacts[alias]
        await self.send_message(c["ip"], c["port"], c["pub_key"], text)

    async def send_message(self, target_ip: str, target_port: int, target_pub_key_b64: str, text: str):
        """Низкоуровневая отправка по IP/Порту (используется для первого контакта)"""
        target_id = f"{target_ip}:{target_port}"
        
        # Если соединения еще нет — открываем
        if target_id not in self.active_connections:
            try:
                reader, writer = await asyncio.open_connection(target_ip, target_port)
                self.active_connections[target_id] = (reader, writer)
                # Запускаем фоновое прослушивание ответов от этого узла
                asyncio.create_task(self.handle_incoming(reader, writer))
            except Exception as e:
                print(f"[!] Не удалось подключиться к {target_id}: {e}")
                return

        _, writer = self.active_connections[target_id]

        # Шифруем сообщение
        pub_key_bytes = base64.b64decode(target_pub_key_b64)
        encrypted_data = self.client.encrypt_symmetric(target_id, pub_key_bytes, text)
        
        payload = P2PMessage(
            sender_id=self.user_id,
            sender_pub_key=base64.b64encode(self.client.public_key.encode()).decode(),
            encrypted_payload=encrypted_data,
            type="text"
        )

        try:
            writer.write(payload.model_dump_json().encode())
            await writer.drain()
        except Exception as e:
            print(f"[!] Ошибка отправки: {e}")
            self.active_connections.pop(target_id, None)