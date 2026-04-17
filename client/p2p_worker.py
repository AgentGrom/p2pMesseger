import asyncio
import base64
import json
import time
from client.core import P2PClient, P2PMessage 

class P2PWorker:
    def __init__(self, host: str, port: int, user_id: str):
        self.client = P2PClient()
        self.host = host
        self.port = port
        self.user_id = user_id
        self.active_connections = {} # { "ip:port": writer }
        self.contacts = {} # { "Nick": {"ip": "..", "port": .., "pub_key": "..", "last_seen": ..} }

    async def start(self):
        server = await asyncio.start_server(self.handle_incoming, self.host, self.port)
        asyncio.create_task(self.cleanup_contacts())
        pub_key_str = base64.b64encode(self.client.public_key.encode()).decode()
        print(f"\n[SYSTEM] Узел {self.user_id} на порту {self.port}")
        print(f"[SYSTEM] Ключ: {pub_key_str}")
        async with server:
            await server.serve_forever()

    async def cleanup_contacts(self):
        while True:
            await asyncio.sleep(60)
            now = time.time()
            to_delete = [n for n, info in self.contacts.items() if now - info['last_seen'] > 300]
            for n in to_delete:
                print(f"\n[SYSTEM] Контакт '{n}' удален по таймауту.")
                self.contacts.pop(n)

    def create_payload(self, text, target_id, target_pub_key, msg_type="text"):
        """Вспомогательная функция для сборки пакета"""
        encrypted = self.client.encrypt_symmetric(target_id, base64.b64decode(target_pub_key), text)
        payload = P2PMessage(
            sender_id=self.user_id,
            sender_pub_key=base64.b64encode(self.client.public_key.encode()).decode(),
            encrypted_payload=encrypted,
            type=msg_type
        ).model_dump()
        payload["sender_listen_port"] = self.port
        return json.dumps(payload).encode()

    async def handle_incoming(self, reader, writer):
        peer = writer.get_extra_info('peername')
        peer_id = f"{peer[0]}:{peer[1]}"
        
        # СОХРАНЯЕМ СОКЕТ, чтобы не открывать новый в ответ
        if peer_id not in self.active_connections:
            self.active_connections[peer_id] = writer

        try:
            while True:
                data = await reader.read(8192)
                if not data: break
                
                msg_dict = json.loads(data.decode())
                nickname = msg_dict.get("sender_id")
                
                if nickname:
                    # Убираем временный "pending", если он был
                    self.contacts.pop(f"pending_{peer[0]}", None)
                    
                    # Обновляем инфо о контакте
                    self.contacts[nickname] = {
                        "ip": peer[0],
                        "port": msg_dict.get("sender_listen_port", peer[1]),
                        "pub_key": msg_dict.get("sender_pub_key"),
                        "last_seen": time.time()
                    }

                    # ЕСЛИ ЭТО ПЕРВОЕ СООБЩЕНИЕ (Handshake), ШЛЕМ ОТВЕТ СО СВОИМ НИКОМ
                    if msg_dict.get("type") == "handshake":
                        response = self.create_payload("Handshake OK", nickname, msg_dict["sender_pub_key"], "handshake_reply")
                        writer.write(response)
                        await writer.drain()

                if msg_dict.get("type") == "text":
                    msg = P2PMessage(**msg_dict)
                    decrypted = self.client.decrypt_symmetric(
                        msg.sender_id, base64.b64decode(msg.sender_pub_key), msg.encrypted_payload
                    )
                    print(f"\n[{msg.sender_id}]: {decrypted}")
                    print(f"[{self.user_id}] > ", end="", flush=True)

        except Exception: pass
        finally:
            self.active_connections.pop(peer_id, None)
            writer.close()

    async def send_to_contact(self, alias: str, text: str):
        if alias not in self.contacts:
            print(f"[!] Ошибка: Контакт '{alias}' не найден!"); return
        c = self.contacts[alias]
        await self.send_message(c["ip"], c["port"], c["pub_key"], text)

    async def send_message(self, target_ip: str, target_port: int, target_pub_key_b64: str, text: str, is_handshake=True):
        target_id = f"{target_ip}:{target_port}"
        
        if target_id not in self.active_connections:
            try:
                reader, writer = await asyncio.open_connection(target_ip, target_port)
                self.active_connections[target_id] = writer
                asyncio.create_task(self.handle_incoming(reader, writer))
                self.contacts[f"pending_{target_ip}"] = {
                    "ip": target_ip, "port": target_port, "pub_key": target_pub_key_b64, "last_seen": time.time()
                }
            except Exception as e:
                print(f"[!] Ошибка: {e}"); return

        writer = self.active_connections[target_id]
        msg_type = "handshake" if is_handshake else "text"
        
        packet = self.create_payload(text, target_id, target_pub_key_b64, msg_type)
        writer.write(packet)
        await writer.drain()