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
        # Храним сокеты по НИКНЕЙМАМ
        self.active_connections = {} 
        self.contacts = {} 

    async def start(self):
        server = await asyncio.start_server(self.handle_incoming, self.host, self.port)
        pub_key_str = base64.b64encode(self.client.public_key.encode()).decode()
        print(f"\n[SYSTEM] Узел {self.user_id} запущен!")
        print(f"[SYSTEM] Ключ: {pub_key_str}\n" + "="*30)
        
        asyncio.create_task(self.cleanup_contacts())
        async with server:
            await server.serve_forever()

    async def handle_incoming(self, reader, writer):
        peer = writer.get_extra_info('peername')
        peer_ip = peer[0]
        current_nick = None

        try:
            while True:
                data = await reader.read(8192)
                if not data: break
                
                msg_dict = json.loads(data.decode())
                sender_nick = msg_dict.get("sender_id")
                
                if sender_nick:
                    current_nick = sender_nick
                    # ВАЖНО: Привязываем входящий сокет к нику
                    self.active_connections[sender_nick] = writer
                    
                    # Обновляем контакт (всегда используем ник как главный ID)
                    self.contacts[sender_nick] = {
                        "ip": peer_ip,
                        "port": msg_dict.get("sender_listen_port", peer[1]),
                        "pub_key": msg_dict.get("sender_pub_key"),
                        "last_seen": time.time()
                    }
                    self.contacts.pop(f"pending_{peer_ip}", None)

                    # Авто-ответ на рукопожатие
                    if msg_dict.get("type") == "handshake":
                        resp = self.create_payload("Handshake ACK", sender_nick, msg_dict["sender_pub_key"], "handshake_reply")
                        writer.write(resp)
                        await writer.drain()

                if msg_dict.get("type") == "text":
                    msg = P2PMessage(**msg_dict)
                    # Дешифруем, используя ник как ID сессии
                    decrypted = self.client.decrypt_symmetric(
                        sender_nick, base64.b64decode(msg.sender_pub_key), msg.encrypted_payload
                    )
                    print(f"\n[{sender_nick}]: {decrypted}")
                    print(f"[{self.user_id}] > ", end="", flush=True)

        except Exception: pass
        finally:
            if current_nick: self.active_connections.pop(current_nick, None)
            writer.close()

    def create_payload(self, text, target_id, target_pub_key, msg_type="text"):
        # Используем target_id (ник) для ядра, чтобы сессия не сбрасывалась
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
            print(f"[!] Контакт {alias} не найден"); return
        
        c = self.contacts[alias]
        
        # ГЛАВНОЕ: Ищем уже открытый сокет (неважно, кто его открыл)
        writer = self.active_connections.get(alias)
        
        if writer and not writer.is_closing():
            try:
                packet = self.create_payload(text, alias, c["pub_key"], "text")
                writer.write(packet)
                await writer.drain()
                return # Успешно отправили в существующий канал
            except Exception:
                self.active_connections.pop(alias, None)

        # Если старого канала нет, создаем новый
        await self.send_message(c["ip"], c["port"], c["pub_key"], text, is_handshake=False, target_name=alias)

    async def send_message(self, ip, port, key, text, is_handshake=True, target_name=None):
        try:
            reader, writer = await asyncio.open_connection(ip, port)
            # Временно вешаем сокет на IP или имя
            conn_id = target_name or f"pending_{ip}"
            self.active_connections[conn_id] = writer
            asyncio.create_task(self.handle_incoming(reader, writer))
            
            # Если это первый коннект, создаем временный контакт
            if is_handshake and not target_name:
                self.contacts[conn_id] = {"ip": ip, "port": port, "pub_key": key, "last_seen": time.time()}

            msg_type = "handshake" if is_handshake else "text"
            packet = self.create_payload(text, target_name or "unknown", key, msg_type)
            writer.write(packet)
            await writer.drain()
        except Exception as e:
            print(f"[!] Ошибка подключения: {e}")

    async def cleanup_contacts(self):
        while True:
            await asyncio.sleep(60)
            now = time.time()
            to_delete = [n for n, i in self.contacts.items() if now - i['last_seen'] > 300]
            for n in to_delete:
                self.contacts.pop(n, None)
                self.active_connections.pop(n, None)