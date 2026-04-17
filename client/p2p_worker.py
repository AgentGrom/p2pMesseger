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
        self.active_connections = {}
        # Контакты теперь хранят время последнего сообщения
        self.contacts = {} # { "Nick": {"ip": "..", "port": .., "pub_key": "..", "last_seen": timestamp} }

    async def start(self):
        server = await asyncio.start_server(self.handle_incoming, self.host, self.port)
        # Запускаем фоновую очистку старых контактов
        asyncio.create_task(self.cleanup_contacts())
        
        pub_key_str = base64.b64encode(self.client.public_key.encode()).decode()
        print(f"\n[SYSTEM] Узел {self.user_id} на порту {self.port}")
        print(f"[SYSTEM] Ключ: {pub_key_str}")
        async with server:
            await server.serve_forever()

    async def cleanup_contacts(self):
        """Раз в минуту проверяет, кто не писал нам более 5 минут"""
        while True:
            await asyncio.sleep(60)
            now = time.time()
            to_delete = [name for name, info in self.contacts.items() 
                         if now - info['last_seen'] > 300] # 300 сек = 5 мин
            for name in to_delete:
                print(f"\n[SYSTEM] Контакт '{name}' удален по таймауту.")
                self.contacts.pop(name)

    async def handle_incoming(self, reader, writer):
        peer = writer.get_extra_info('peername')
        try:
            while True:
                data = await reader.read(8192)
                if not data: break
                
                msg_dict = json.loads(data.decode())
                nickname = msg_dict.get("sender_id")
                
                if nickname:
                    # Обновляем или добавляем контакт
                    self.contacts[nickname] = {
                        "ip": peer[0],
                        "port": msg_dict.get("sender_listen_port", peer[1]), # Берем РЕАЛЬНЫЙ порт из сообщения
                        "pub_key": msg_dict.get("sender_pub_key"),
                        "last_seen": time.time()
                    }
                
                if msg_dict.get("encrypted_payload"):
                    msg = P2PMessage(**msg_dict)
                    decrypted = self.client.decrypt_symmetric(
                        msg.sender_id, base64.b64decode(msg.sender_pub_key), msg.encrypted_payload
                    )
                    print(f"\n[{msg.sender_id}]: {decrypted}")
                    print(f"[{self.user_id}] > ", end="", flush=True)

        except Exception: pass
        finally: writer.close()

    async def send_message(self, target_ip: str, target_port: int, target_pub_key_b64: str, text: str, target_name="Unknown"):
        target_id = f"{target_ip}:{target_port}"
        
        if target_id not in self.active_connections:
            try:
                reader, writer = await asyncio.open_connection(target_ip, target_port)
                self.active_connections[target_id] = (reader, writer)
                asyncio.create_task(self.handle_incoming(reader, writer))
                # Сразу сохраняем того, к кому подключаемся
                self.contacts[target_name] = {
                    "ip": target_ip, "port": target_port, "pub_key": target_pub_key_b64, "last_seen": time.time()
                }
            except Exception as e:
                print(f"[!] Ошибка: {e}"); return

        _, writer = self.active_connections[target_id]
        
        payload = P2PMessage(
            sender_id=self.user_id,
            sender_pub_key=base64.b64encode(self.client.public_key.encode()).decode(),
            encrypted_payload=self.client.encrypt_symmetric(target_id, base64.b64decode(target_pub_key_b64), text),
            type="text"
        )
        
        # Добавляем в JSON наш порт, чтобы получатель знал, куда отвечать
        data = payload.model_dump()
        data["sender_listen_port"] = self.port 
        
        writer.write(json.dumps(data).encode())
        await writer.drain()