import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import pika
import json
import threading

app = FastAPI()

# This class manages browser connections
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                pass # Handle stale connections

manager = ConnectionManager()

# --- RABBITMQ CONSUMER THREAD ---
def start_rabbitmq_consumer(loop):
    # Added durable=True to ensure queue survives RabbitMQ restarts
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', heartbeat=600))
    channel = connection.channel()
    channel.queue_declare(queue='insider_alerts', durable=True)

    def callback(ch, method, properties, body):
        message = body.decode()
        
        # Only broadcast if there are active browser windows
        # If no one is watching, we STILL acknowledge so the queue doesn't back up infinitely,
        # OR you can remove ch.basic_ack if you want it to wait for a browser to connect.
        asyncio.run_coroutine_threadsafe(manager.broadcast(message), loop)
        
        # MANUAL ACKNOWLEDGMENT: Tell RabbitMQ we successfully handled it
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # basic_qos(prefetch_count=1) prevents the bridge from being flooded 
    # It tells RabbitMQ: "Don't give me a new message until I've finished the last one"
    channel.basic_qos(prefetch_count=1)
    
    # auto_ack=False is the most important change here
    channel.basic_consume(queue='insider_alerts', on_message_callback=callback, auto_ack=False)
    
    print("🚀 Bridge connected to RabbitMQ. Waiting for trades...")
    channel.start_consuming()

@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_event_loop()
    threading.Thread(target=start_rabbitmq_consumer, args=(loop,), daemon=True).start()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)