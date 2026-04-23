import asyncio
import base64
import json
import time
from client.core import P2PClient, P2PMessage, MessageType

class P2PWorker:
    def __init__(self, host: str, port: int, user_id: str):
        self.client = P2PClient()
        self.host = host
        self.port = port
        self.user_id = user_id
        # Сокеты храним по никнеймам
        self.active_connections = {} 
        # self.contacts = {} 

    async def start(self):
        server = await asyncio.start_server(self.handle_incoming, self.host, self.port)
        pub_key_str = base64.b64encode(self.client.public_key.encode()).decode()
        print(f"\n[SYSTEM] Узел {self.user_id} на порту {self.port}")
        print(f"[SYSTEM] Ключ: {pub_key_str}\n" + "="*30)
        
        # asyncio.create_task(self.cleanup_contacts())
        async with server:
            await server.serve_forever()

    async def handle_incoming(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info('peername')
        peer_ip, peer_port = peer

        while True:
            data = await reader.read(8192)
            if not data: break
            
            msg = P2PMessage.model_validate_json(data.decode())

            sender_nick = id(writer)
            sender_pub_key = base64.b64decode(msg.sender_pub_key)

            print(msg.type)
            
            if msg.type == MessageType.make_session.name:
                msg_type = MessageType.ack_session

                self.client.add_symmetric_session(sender_nick, writer)

                text = self.client.sessions[sender_nick].shared_key

                box = self.client.assymetric_box(sender_pub_key)
                encrypted_text = base64.b64encode(box.encrypt(text))

                resp = P2PMessage(
                    sender_id=self.user_id,
                    sender_pub_key=base64.b64encode(self.client.public_key.encode()).decode(),
                    payload=encrypted_text,
                    type=msg_type.name
                ).model_dump()

                writer.write(json.dumps(resp).encode())
                await writer.drain()

                print(self.client.sessions)

                print(f"[*] Ответ на соединение от {sender_nick}")

            if msg.type == MessageType.ack_session.name:
                box = self.client.assymetric_box(sender_pub_key)

                key = box.decrypt(base64.b64decode(msg.payload))

                if sender_nick not in self.client.sessions:  continue

                self.client.add_symmetric_session(sender_nick, writer, key)

                print(self.client.sessions)

                print(f"[*] Соединение установлено с {sender_nick}")

            # Обработка обычных сообщений
            if msg.type == MessageType.default.name:
                decrypted = self.client.decrypt_symmetric(
                    sender_nick, base64.b64decode(msg.sender_pub_key), msg.payload
                )
                print(f"\n[{sender_nick}]: {decrypted}")
                print(f"[{self.user_id}] > ", end="", flush=True)

        writer.close()

    def create_payload(self, text, target_id, target_pub_key, msg_type=MessageType.default):
        encrypted = self.client.encrypt_symmetric(target_id, base64.b64decode(target_pub_key), text)
        payload = P2PMessage(
            sender_id=self.user_id,
            sender_pub_key=base64.b64encode(self.client.public_key.encode()).decode(),
            payload=encrypted,
            type=msg_type.name
        ).model_dump()
        return json.dumps(payload).encode()

    async def send_initial_handshake(self, ip:int, port:int):
        reader, writer = await asyncio.open_connection(ip, port)

        asyncio.create_task(self.handle_incoming(reader, writer))

        sender_nick = id(writer)
        self.client.add_symmetric_session(
            sender_nick,
            writer
        )
        
        msg_type = MessageType.make_session

        resp = P2PMessage(
            sender_id=self.user_id,
            sender_pub_key=base64.b64encode(self.client.public_key.encode()).decode(),
            payload="",
            type=msg_type.name
        ).model_dump()

        writer.write(json.dumps(resp).encode())
        await writer.drain()

        print(f"[*] Запрос отправлен на {ip}:{port}...")