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

    async def start(self):
        server = await asyncio.start_server(self.handle_incoming, self.host, self.port)
        print(f"[*] Узел {self.user_id} запущен на {self.host}:{self.port}")
        # Печатаем ключ для удобства ручного тестирования
        pub_key_str = base64.b64encode(self.client.public_key.encode()).decode()
        print(f"[*] Публичный ключ: {pub_key_str}")
        
        async with server:
            await server.serve_forever()

    async def handle_incoming(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        data = await reader.read(8192)
        try:
            msg_data = json.loads(data.decode())
            msg = P2PMessage(**msg_data)
            
            sender_pub_key = base64.b64decode(msg.sender_pub_key) 
            
            decrypted_text = self.client.decrypt_symmetric(
                msg.sender_id, 
                sender_pub_key, 
                msg.encrypted_payload
            )
            
            print(f"\n[Сообщение от {msg.sender_id}]: {decrypted_text}")
        except Exception as e:
            print(f"[!] Ошибка обработки: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def send_message(self, target_ip: str, target_port: int, target_pub_key_b64: str, text: str):
        pub_key_bytes = base64.b64decode(target_pub_key_b64)
        
        # Шифруем данные через ядро
        target_id = f"{target_ip}:{target_port}"
        encrypted_data = self.client.encrypt_symmetric(
            target_id, 
            pub_key_bytes, 
            text
        )
        
        # Формируем объект сообщения
        payload = P2PMessage(
            sender_id=self.user_id,
            sender_pub_key=base64.b64encode(self.client.public_key.encode()).decode(),
            encrypted_payload=encrypted_data,
            type="text"
        )

        try:
            reader, writer = await asyncio.open_connection(target_ip, target_port)
            
            writer.write(payload.model_dump_json().encode())
            await writer.drain()
            
            writer.close()
            await writer.wait_closed()
            print(f"[OK] Отправлено на {target_ip}:{target_port}")
        except Exception as e:
            print(f"[!] Ошибка отправки: {e}")