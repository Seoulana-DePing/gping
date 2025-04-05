// WebSocket RPC client example for Gping

// Connect to the WebSocket
const socket = new WebSocket('ws://localhost:1111/ws');

// Generate a unique ID for each message
function generateMessageId() {
  return Date.now().toString() + Math.random().toString().substring(2, 8);
}

// Store pending requests
const pendingRequests = new Map();

// Handle the WebSocket connection
socket.onopen = function() {
  console.log('Connected to Gping WebSocket server');
  
  // Example: Call get_location
  getLocation('203.0.113.42');
  
  // Example: Send Tping data
  // sendTpingData('Seoul, South Korea', 150, 'TpingAddress1123456789');
};

// Handle incoming messages
socket.onmessage = function(event) {
  const response = JSON.parse(event.data);
  
  // Check if we have a pending request for this message ID
  if (pendingRequests.has(response.id)) {
    const { method, resolve, reject } = pendingRequests.get(response.id);
    pendingRequests.delete(response.id);
    
    console.log(`Received response for method ${method}:`, response);
    
    if (response.error) {
      reject(response.error);
    } else {
      resolve(response.result);
    }
  } else {
    console.log('Received unsolicited message:', response);
  }
};

// Handle WebSocket errors
socket.onerror = function(error) {
  console.error('WebSocket error:', error);
};

// Handle WebSocket connection close
socket.onclose = function() {
  console.log('WebSocket connection closed');
};

// Function to get location of an IP address
function getLocation(ip) {
  const id = generateMessageId();
  const params = { ip };
  
  return new Promise((resolve, reject) => {
    pendingRequests.set(id, { method: 'get_location', resolve, reject });
    
    const message = {
      id: id,
      method: 'get_location',
      params: params
    };
    
    console.log('Sending get_location request:', message);
    socket.send(JSON.stringify(message));
  });
}

// Function to send Tping data
function sendTpingData(gps, time, address) {
  const id = generateMessageId();
  const params = { gps, time, address };
  
  return new Promise((resolve, reject) => {
    pendingRequests.set(id, { method: 'guess_location', resolve, reject });
    
    const message = {
      id: id,
      method: 'guess_location',
      params: params
    };
    
    console.log('Sending guess_location request:', message);
    socket.send(JSON.stringify(message));
  });
} 