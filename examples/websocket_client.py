#!/usr/bin/env python3
import asyncio
import json
import time
import random
import websockets

class GpingWebSocketClient:
    def __init__(self, url='ws://localhost:1111/ws'):
        self.url = url
        self.websocket = None
        self.pending_requests = {}
        
    async def connect(self):
        self.websocket = await websockets.connect(self.url)
        print(f"Connected to Gping WebSocket server at {self.url}")
        
        # Start listening for messages
        asyncio.create_task(self.listen_for_messages())
        
    async def listen_for_messages(self):
        try:
            while True:
                message = await self.websocket.recv()
                response = json.loads(message)
                
                # Check if we have a pending request for this message ID
                if response.get('id') in self.pending_requests:
                    future = self.pending_requests.pop(response['id'])
                    if response.get('error'):
                        future.set_exception(Exception(json.dumps(response['error'])))
                    else:
                        future.set_result(response.get('result'))
                else:
                    print(f"Received unsolicited message: {response}")
        except websockets.exceptions.ConnectionClosed:
            print("WebSocket connection closed")
            
    def generate_message_id(self):
        timestamp = int(time.time() * 1000)
        random_part = random.randint(100000, 999999)
        return f"{timestamp}_{random_part}"
    
    async def get_location(self, ip):
        message_id = self.generate_message_id()
        message = {
            'id': message_id,
            'method': 'get_location',
            'params': {'ip': ip}
        }
        
        # Create a future to wait for the response
        future = asyncio.Future()
        self.pending_requests[message_id] = future
        
        # Send the request
        print(f"Sending get_location request: {message}")
        await self.websocket.send(json.dumps(message))
        
        # Wait for the response
        return await future
    
    async def send_tping_data(self, gps, time, address):
        message_id = self.generate_message_id()
        message = {
            'id': message_id,
            'method': 'guess_location',
            'params': {
                'gps': gps,
                'time': time,
                'address': address
            }
        }
        
        # Create a future to wait for the response
        future = asyncio.Future()
        self.pending_requests[message_id] = future
        
        # Send the request
        print(f"Sending guess_location request: {message}")
        await self.websocket.send(json.dumps(message))
        
        # Wait for the response
        return await future
    
    async def close(self):
        if self.websocket:
            await self.websocket.close()
            print("WebSocket connection closed")

async def main():
    client = GpingWebSocketClient()
    await client.connect()
    
    try:
        # Example: Get location
        result = await client.get_location('203.0.113.42')
        print(f"Get location result: {result}")
        
        # Example: Send Tping data
        # result = await client.send_tping_data('Seoul, South Korea', 150, 'TpingAddress1123456789')
        # print(f"Send Tping data result: {result}")
        
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main()) 