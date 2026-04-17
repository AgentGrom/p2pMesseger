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
        # Сокеты храним по никнеймам
        self.active_connections = {} 
        self.contacts = {} 

    async def start(self):
        server = await asyncio.start_server(self.handle_incoming, self.host, self.port)
        pub_key_str = base64.b64encode(self.client.public_key.encode()).decode()
        print(f"\n[SYSTEM] Узел {self.user_id} на порту {self.port}")
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
                    # 1. Если это новый ник — регистрируем его в соединениях и контактах
                    if sender_nick not in self.contacts:
                        self.contacts[sender_nick] = {
                            "ip": peer_ip,
                            "port": msg_dict.get("sender_listen_port", peer[1]),
                            "pub_key": msg_dict.get("sender_pub_key"),
                            "last_seen": time.time()
                        }
                        self.active_connections[sender_nick] = writer
                        print(f"\n[+] Новый контакт: {sender_nick} (@{peer_ip})")
                        
                        # 2. Если нам прислали handshake (первый раз), отвечаем своим handshake
                        if msg_dict.get("type") == "handshake":
                            # Отправляем инфо о себе в ответ
                            resp = self.create_payload("Handshake ACK", sender_nick, msg_dict["sender_pub_key"], "handshake_reply")
                            writer.write(resp)
                            await writer.drain()
                    
                    # Обновляем время активности
                    self.contacts[sender_nick]["last_seen"] = time.time()
                    self.active_connections[sender_nick] = writer
                    current_nick = sender_nick

                # 3. Обработка обычных сообщений
                if msg_dict.get("type") == "text":
                    msg = P2PMessage(**msg_dict)
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
        # target_id здесь — это ник собеседника. Ядро будет использовать его для сессии.
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
            print(f"[!] Ошибка: Контакт {alias} не найден"); return
        
        c = self.contacts[alias]
        writer = self.active_connections.get(alias)
        
        if writer and not writer.is_closing():
            try:
                packet = self.create_payload(text, alias, c["pub_key"], "text")
                writer.write(packet)
                await writer.drain()
            except Exception:
                self.active_connections.pop(alias, None)
                print("[!] Соединение потеряно.")
        else:
            print("[!] Ошибка: Соединение с контактом неактивно.")

    async def send_initial_handshake(self, ip, port, key):
        """Метод для команды connect: просто стучимся и шлем данные о себе"""
        try:
            reader, writer = await asyncio.open_connection(ip, port)
            # Временно сохраняем сокет по IP, пока не узнаем ник в handle_incoming
            temp_id = f"conn_{ip}" 
            self.active_connections[temp_id] = writer
            asyncio.create_task(self.handle_incoming(reader, writer))
            
            # Шлем handshake. ID сессии в ядре пока ставим "initial", 
            # так как мы еще не знаем ник получателя.
            packet = self.create_payload("HELLO", "initial", key, "handshake")
            writer.write(packet)
            await writer.drain()
            print(f"[*] Запрос отправлен на {ip}:{port}...")
        except Exception as e:
            print(f"[!] Ошибка подключения: {e}")

    async def cleanup_contacts(self):
        while True:
            await asyncio.sleep(60)
            now = time.time()
            to_delete = [n for n, i in self.contacts.items() if now - i['last_seen'] > 300]
            for n in to_delete:
                print(f"\n[SYSTEM] Контакт '{n}' удален за неактивность.")
                self.contacts.pop(n, None)
                self.active_connections.pop(n, None)