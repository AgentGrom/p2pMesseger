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
        # Храним сокеты по никнеймам для мгновенного доступа
        self.active_connections = {} # { "Nick": writer }
        self.contacts = {} 

    async def start(self):
        server = await asyncio.start_server(self.handle_incoming, self.host, self.port)
        
        # Генерируем строку ключа для вывода в консоль
        pub_key_str = base64.b64encode(self.client.public_key.encode()).decode()
        
        print(f"\n" + "="*50)
        print(f"[SYSTEM] Узел {self.user_id} запущен!")
        print(f"[SYSTEM] Порт: {self.port}")
        print(f"[SYSTEM] Твой публичный ключ (передай его другу):")
        print(f"{pub_key_str}") # Вот здесь ключ выводится в консоль
        print("="*50 + "\n")
        
        asyncio.create_task(self.cleanup_contacts())
        
        async with server:
            await server.serve_forever()

    async def handle_incoming(self, reader, writer):
        peer = writer.get_extra_info('peername')
        peer_ip = peer[0]
        current_nickname = None # Никнейм собеседника в этой сессии

        try:
            while True:
                data = await reader.read(8192)
                if not data: break
                
                msg_dict = json.loads(data.decode())
                nickname = msg_dict.get("sender_id")
                
                if nickname:
                    current_nickname = nickname
                    # Привязываем этот сокет к нику, чтобы отвечать в него же
                    self.active_connections[nickname] = writer
                    
                    # Обновляем инфо в контактах
                    self.contacts[nickname] = {
                        "ip": peer_ip,
                        "port": msg_dict.get("sender_listen_port", peer[1]),
                        "pub_key": msg_dict.get("sender_pub_key"),
                        "last_seen": time.time()
                    }
                    # Удаляем временный контакт, если был
                    self.contacts.pop(f"pending_{peer_ip}", None)

                    # Если это рукопожатие, подтверждаем
                    if msg_dict.get("type") == "handshake":
                        resp = self.create_payload("ACK", nickname, msg_dict["sender_pub_key"], "handshake_reply")
                        writer.write(resp)
                        await writer.drain()

                if msg_dict.get("type") == "text":
                    msg = P2PMessage(**msg_dict)
                    decrypted = self.client.decrypt_symmetric(
                        msg.sender_id, base64.b64decode(msg.sender_pub_key), msg.encrypted_payload
                    )
                    print(f"\n[{msg.sender_id}]: {decrypted}")
                    print(f"[{self.user_id}] > ", end="", flush=True)

        except Exception as e:
            print(f"\n[!] Ошибка в соединении: {e}")
        finally:
            if current_nickname:
                self.active_connections.pop(current_nickname, None)
            writer.close()

    def create_payload(self, text, target_id, target_pub_key, msg_type="text"):
        encrypted = self.client.encrypt_symmetric(target_id, base64.b64decode(target_pub_key), text)
        payload = P2PMessage(
            sender_id=self.user_id,
            sender_pub_key=base64.b64encode(self.client.public_key.encode()).decode(),
            encrypted_payload=encrypted,
            type=msg_type
        ).model_dump()
        payload["sender_listen_port"] = self.port
        return json.dumps(payload).encode()

    async def send_to_contact(self, alias: str, text: str):
        if alias not in self.contacts:
            print(f"[!] Ошибка: Контакт '{alias}' не найден!"); return
        
        c = self.contacts[alias]
        # ВАЖНО: Если у нас уже есть активный сокет для этого НИКА, используем его
        if alias in self.active_connections:
            try:
                writer = self.active_connections[alias]
                packet = self.create_payload(text, alias, c["pub_key"], "text")
                writer.write(packet)
                await writer.drain()
                return
            except Exception:
                self.active_connections.pop(alias, None)

        # Если сокета нет, пробуем создать по IP (как раньше)
        await self.send_message(c["ip"], c["port"], c["pub_key"], text, is_handshake=False, target_name=alias)

    async def send_message(self, target_ip: str, target_port: int, target_pub_key_b64: str, text: str, is_handshake=True, target_name=None):
        # Если мы знаем ник, проверяем сокет
        if target_name and target_name in self.active_connections:
            writer = self.active_connections[target_name]
        else:
            try:
                reader, writer = await asyncio.open_connection(target_ip, target_port)
                asyncio.create_task(self.handle_incoming(reader, writer))
                # Временно записываем сокет, пока не подтвержден ник
                self.active_connections[target_name or f"pending_{target_ip}"] = writer
            except Exception as e:
                print(f"[!] Ошибка соединения: {e}"); return

        msg_type = "handshake" if is_handshake else "text"
        packet = self.create_payload(text, target_name or "unknown", target_pub_key_b64, msg_type)
        writer.write(packet)
        await writer.drain()

    async def cleanup_contacts(self):
        while True:
            await asyncio.sleep(60)
            now = time.time()
            to_delete = [n for n, info in self.contacts.items() if now - info.get('last_seen', 0) > 300]
            for n in to_delete:
                self.contacts.pop(n, None)
                self.active_connections.pop(n, None)