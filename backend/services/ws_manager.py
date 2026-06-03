import logging
from typing import List, Dict, Set, Any
from fastapi import WebSocket

logger = logging.getLogger("WebSocketManager")

class ConnectionManager:
    """
    ConnectionManager tracks active WebSocket channels and broadcasts 
    live telemetry streams from computer vision threads to frontend clients.
    """
    def __init__(self):
        # A list of active WebSocket connection instances
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        """Accepts a client WebSocket handshake and registers the channel."""
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket client connected. Active channels: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        """Unregisters a client connection."""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket client disconnected. Active channels: {len(self.active_connections)}")

    async def send_personal_message(self, message: Dict[str, Any], websocket: WebSocket):
        """Sends a JSON frame to a specific client channel."""
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.debug(f"Failed sending private WebSocket packet: {e}")
            self.disconnect(websocket)

    async def broadcast(self, message: Dict[str, Any]):
        """Broadcasts a JSON packet to all registered dashboard clients."""
        if not self.active_connections:
            return
            
        logger.debug(f"Broadcasting websocket telemetry. Clients: {len(self.active_connections)}")
        
        # Make a copy to avoid mutation issues during iteration
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception as e:
                # Handle socket closed issues gracefully
                logger.debug(f"Broadcasting failure to client channel: {e}")
                self.disconnect(connection)


# Singleton manager instance
ws_connection_manager = ConnectionManager()
