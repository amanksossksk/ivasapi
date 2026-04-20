import re
import ssl
import json
import asyncio
import phonenumbers
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from phonenumbers import geocoder
import websockets
import threading
from collections import defaultdict, deque
import os
import time
from pathlib import Path
import uuid

app = Flask(__name__)
CORS(app)

# Configuration Files
WSS_CONFIG_FILE = "wss.json"
OTP_FILE = "otp.json"
STATS_FILE = "stats.json"
NUMBERS_FILE = "numbers_pool.json"

# Global variables - Using simple lists without complex locks
otp_storage = defaultdict(list)
ws_connection = None
ws_thread = None
is_ws_running = False

# Number pool management - Simple list approach
number_pool = []  # Simple list for numbers
served_numbers = set()  # Track served numbers to avoid duplicates
available_numbers_count = 0
total_numbers_loaded = 0
numbers_served_count = 0

stats = {
    "total_otps": 0,
    "today_otps": 0,
    "active_numbers": set(),
    "last_otp_time": None,
    "connection_status": "disconnected",
    "last_connection_attempt": None,
    "uptime": None,
    "numbers_pool": {
        "available": 0,
        "total_loaded": 0,
        "served": 0
    }
}

# Load WebSocket config
def load_wss_config():
    """Load WebSocket configuration from wss.json"""
    default_config = {
        "websocket_url": "",
        "ping_interval": 25000,
        "auto_reconnect": True,
        "reconnect_delay": 5,
        "max_otps_per_number": 100
    }
    
    try:
        if os.path.exists(WSS_CONFIG_FILE):
            with open(WSS_CONFIG_FILE, 'r') as f:
                config = json.load(f)
                # Merge with defaults
                for key in default_config:
                    if key not in config:
                        config[key] = default_config[key]
                return config
        else:
            # Create default config
            with open(WSS_CONFIG_FILE, 'w') as f:
                json.dump(default_config, f, indent=2)
            return default_config
    except Exception as e:
        print(f"❌ Error loading WSS config: {e}")
        return default_config

def save_wss_config(config):
    """Save WebSocket configuration to wss.json"""
    try:
        with open(WSS_CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
        return True
    except Exception as e:
        print(f"❌ Error saving WSS config: {e}")
        return False

# Number Pool Management Functions
def load_numbers_pool():
    """Load numbers from persistent storage"""
    global number_pool, available_numbers_count, total_numbers_loaded, numbers_served_count, served_numbers
    
    try:
        if os.path.exists(NUMBERS_FILE):
            with open(NUMBERS_FILE, 'r') as f:
                data = json.load(f)
                number_pool = data.get('available_numbers', [])
                served_numbers = set(data.get('served_numbers', []))
                available_numbers_count = len(number_pool)
                total_numbers_loaded = data.get('total_loaded', 0)
                numbers_served_count = data.get('served_count', 0)
                
                # Update stats
                update_number_pool_stats()
                    
            print(f"✅ Loaded {available_numbers_count} numbers from pool")
        else:
            # Create empty file
            save_numbers_pool()
    except Exception as e:
        print(f"❌ Error loading numbers pool: {e}")
        # Initialize empty pool
        number_pool = []
        served_numbers = set()
        available_numbers_count = 0
        total_numbers_loaded = 0
        numbers_served_count = 0

def save_numbers_pool():
    """Save numbers pool to persistent storage"""
    try:
        data = {
            'available_numbers': number_pool,
            'served_numbers': list(served_numbers),
            'total_loaded': total_numbers_loaded,
            'served_count': numbers_served_count,
            'last_updated': datetime.now().isoformat()
        }
        
        # Write to temp file first, then rename for atomic operation
        temp_file = NUMBERS_FILE + '.tmp'
        with open(temp_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        # Atomic rename
        os.replace(temp_file, NUMBERS_FILE)
        return True
    except Exception as e:
        print(f"❌ Error saving numbers pool: {e}")
        return False

def update_number_pool_stats():
    """Update stats with current number pool information"""
    stats["numbers_pool"] = {
        "available": available_numbers_count,
        "total_loaded": total_numbers_loaded,
        "served": numbers_served_count
    }

def validate_and_clean_number(number):
    """Validate and clean a phone number"""
    # Remove all non-digit characters
    clean_number = re.sub(r'[^\d]', '', str(number))
    
    # Basic validation
    if len(clean_number) < 10 or len(clean_number) > 15:
        return None
    
    # Simple validation - just ensure it's all digits and reasonable length
    if clean_number.isdigit() and len(clean_number) >= 10:
        return clean_number
    
    return None

def add_numbers_from_text(text_content):
    """Parse and add numbers from text content"""
    global total_numbers_loaded, available_numbers_count
    
    numbers = []
    lines = text_content.strip().split('\n')
    
    for line in lines:
        # Skip empty lines and comments
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        
        # Extract numbers from the line
        found_numbers = re.findall(r'\b\d{10,15}\b', line)
        for num in found_numbers:
            clean_number = validate_and_clean_number(num)
            if clean_number:
                numbers.append(clean_number)
    
    # Add valid numbers to pool
    added_count = 0
    duplicate_count = 0
    
    for number in numbers:
        if number not in served_numbers and number not in number_pool:
            number_pool.append(number)
            added_count += 1
        else:
            duplicate_count += 1
    
    total_numbers_loaded += added_count
    available_numbers_count = len(number_pool)
    update_number_pool_stats()
    
    # Save after modifications
    save_numbers_pool()
    
    return {
        'added': added_count,
        'duplicates': duplicate_count,
        'total_in_pool': available_numbers_count
    }

def clear_number_pool():
    """Clear all numbers from the pool"""
    global number_pool, available_numbers_count, total_numbers_loaded, numbers_served_count, served_numbers
    
    number_pool.clear()
    served_numbers.clear()
    available_numbers_count = 0
    total_numbers_loaded = 0
    numbers_served_count = 0
    update_number_pool_stats()
    
    save_numbers_pool()

def get_next_number():
    """Get the next available number from the pool (FIFO)"""
    global available_numbers_count, numbers_served_count
    
    if number_pool:
        # Get first number and remove it
        number = number_pool.pop(0)
        served_numbers.add(number)
        available_numbers_count = len(number_pool)
        numbers_served_count += 1
        update_number_pool_stats()
        
        # Save state after modification
        save_numbers_pool()
        
        return number
    else:
        return None

# Load OTPs
def load_otps():
    """Load OTPs from JSON file"""
    try:
        if os.path.exists(OTP_FILE):
            with open(OTP_FILE, 'r') as f:
                data = json.load(f)
                for number, otps in data.items():
                    otp_storage[number] = otps
                    stats["active_numbers"].add(number)
            
            # Calculate total OTPs
            stats["total_otps"] = sum(len(otps) for otps in otp_storage.values())
            
            # Calculate today's OTPs
            today = datetime.now().date()
            today_count = 0
            for number, otps in otp_storage.items():
                for otp in otps:
                    otp_date = datetime.fromisoformat(otp['timestamp']).date()
                    if otp_date == today:
                        today_count += 1
            stats["today_otps"] = today_count
            
            print(f"✅ Loaded {stats['total_otps']} OTPs from {len(otp_storage)} numbers")
    except Exception as e:
        print(f"❌ Error loading OTPs: {e}")

def save_otps():
    """Save OTPs to JSON file"""
    try:
        with open(OTP_FILE, 'w') as f:
            json.dump(dict(otp_storage), f, indent=2, default=str)
    except Exception as e:
        print(f"❌ Error saving OTPs: {e}")

def save_stats():
    """Save statistics to JSON file"""
    try:
        stats_copy = stats.copy()
        stats_copy["active_numbers"] = list(stats["active_numbers"])
        with open(STATS_FILE, 'w') as f:
            json.dump(stats_copy, f, indent=2, default=str)
    except Exception as e:
        print(f"❌ Error saving stats: {e}")

def add_otp(info):
    """Add OTP to storage"""
    number = info["number"]
    otp_data = {
        "id": f"{number}_{int(time.time() * 1000)}",
        "service": info["service"],
        "otp": info["otp"],
        "full_message": info["full_message"],
        "country": info.get("country", "Unknown"),
        "timestamp": datetime.now().isoformat(),
        "time_formatted": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "source": info.get("source", "WebSocket")
    }
    # Add to storage
    otp_storage[number].append(otp_data)
    
    # Update stats
    stats["total_otps"] += 1
    stats["today_otps"] += 1
    stats["active_numbers"].add(number)
    stats["last_otp_time"] = datetime.now().isoformat()
    
    # Limit OTPs per number
    config = load_wss_config()
    max_otps = config.get("max_otps_per_number", 100)
    if len(otp_storage[number]) > max_otps:
        otp_storage[number] = otp_storage[number][-max_otps:]
    
    # Save to files
    save_otps()
    save_stats()
    
    print(f"📝 Saved OTP for +{number} - {info['service']}: {info['otp']}")
    return otp_data

async def send_ping(websocket, ping_interval, ping_msg="3"):
    """Send ping messages to keep WebSocket alive"""
    while True:
        await asyncio.sleep(ping_interval / 1000)
        try:
            await websocket.send(ping_msg)
        except Exception:
            break

async def websocket_handler():
    """Main WebSocket connection handler"""
    global is_ws_running, stats
    
    config = load_wss_config()
    ws_url = config.get("websocket_url", "")
    
    if not ws_url:
        print("❌ No WebSocket URL configured")
        stats["connection_status"] = "no_config"
        return
    
    ssl_context = ssl._create_unverified_context()
    
    while is_ws_running:
        try:
            stats["last_connection_attempt"] = datetime.now().isoformat()
            stats["connection_status"] = "connecting"
            
            async with websockets.connect(ws_url, ssl=ssl_context) as websocket:
                stats["connection_status"] = "connected"
                stats["uptime"] = datetime.now().isoformat()
                print(f"✅ Connected to WebSocket")
                
                # Handle initial message
                initial_message = await websocket.recv()
                ping_interval = config.get("ping_interval", 25000)
                
                try:
                    if initial_message.startswith("0{") and initial_message.endswith("}"):
                        data = json.loads(initial_message[1:])
                        ping_interval = data.get("pingInterval", ping_interval)
                except:
                    pass
                
                # Send namespace connection
                await websocket.send("40/livesms,")
                
                # Start ping task
                ping_task = asyncio.create_task(send_ping(websocket, ping_interval))
                
                # Message loop
                while is_ws_running:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=30)
                        print(message)
                        if message.startswith("42/livesms,"):
                            json_str = message[message.find("["):]
                            data = json.loads(json_str)
                            
                            if isinstance(data, list) and len(data) > 1 and isinstance(data[1], dict):
                                sms = data[1]
                                message_text = sms.get("message", "")
                                
                                # Extract OTP
                                otp_match = re.search(r"\b\d{4,8}\b", message_text)
                                otp = otp_match.group(0) if otp_match else None
                                
                                if otp and sms.get("recipient"):
                                    number = sms.get("recipient")
                                    
                                    # Get country
                                    try:
                                        parsed = phonenumbers.parse("+" + number)
                                        country = geocoder.description_for_number(parsed, "en")
                                    except:
                                        country = sms.get("range", "Unknown")
                                    
                                    info = {
                                        "service": sms.get("originator", "Unknown"),
                                        "number": number,
                                        "otp": otp,
                                        "country": country,
                                        "full_message": message_text,
                                        "source": "WebSocket"
                                    }
                                    
                                    add_otp(info)
                                    
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        print(f"❌ Message processing error: {e}")
                        break
                
                ping_task.cancel()
                
        except Exception as e:
            stats["connection_status"] = "disconnected"
            print(f"❌ Connection error: {e}")
            
            if config.get("auto_reconnect", True) and is_ws_running:
                delay = config.get("reconnect_delay", 5)
                print(f"🔄 Reconnecting in {delay}s...")
                await asyncio.sleep(delay)
            else:
                break

def start_websocket():
    """Start WebSocket connection in background thread"""
    global is_ws_running, ws_thread
    
    if ws_thread and ws_thread.is_alive():
        return
    
    is_ws_running = True
    
    def run_async_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(websocket_handler())
    
    ws_thread = threading.Thread(target=run_async_loop, daemon=True)
    ws_thread.start()

def stop_websocket():
    """Stop WebSocket connection"""
    global is_ws_running
    is_ws_running = False
    stats["connection_status"] = "disconnected"

# API Routes
@app.route('/')
def dashboard():
    """Serve the dashboard"""
    return render_template('dashboard.html')

@app.route('/api/config', methods=['GET', 'POST'])
def handle_config():
    """Get or update WebSocket configuration"""
    if request.method == 'GET':
        config = load_wss_config()
        return jsonify(config)
    
    elif request.method == 'POST':
        try:
            new_config = request.json
            if save_wss_config(new_config):
                # Restart WebSocket with new config
                stop_websocket()
                time.sleep(1)
                start_websocket()
                return jsonify({"status": "success", "message": "Configuration updated"})
            else:
                return jsonify({"status": "error", "message": "Failed to save config"}), 500
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 400

@app.route('/api/getnumber', methods=['GET'])
def get_number():
    """
    Get the next available number from the pool.
    Once retrieved, the number is permanently removed from the pool.
    """
    try:
        # Quick response without any blocking operations
        if not number_pool:
            return jsonify({
                "status": "empty",
                "message": "No numbers available in the pool",
                "remaining_in_pool": 0
            }), 404
        
        # Get number (this is fast since we're just popping from a list)
        number = get_next_number()
        
        if number:
            return jsonify({
                "status": "success",
                "number": number,
                "remaining_in_pool": available_numbers_count,
                "timestamp": datetime.now().isoformat()
            })
        else:
            return jsonify({
                "status": "empty",
                "message": "No numbers available in the pool",
                "remaining_in_pool": 0
            }), 404
            
    except Exception as e:
        print(f"❌ Error in get_number: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/numbers/upload', methods=['POST'])
def upload_numbers():
    """Upload numbers from a text file"""
    try:
        if 'file' not in request.files:
            return jsonify({"status": "error", "message": "No file uploaded"}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({"status": "error", "message": "No file selected"}), 400
        
        if not file.filename.endswith('.txt'):
            return jsonify({"status": "error", "message": "Only .txt files are allowed"}), 400
        
        # Read file content
        text_content = file.read().decode('utf-8')
        
        # Process numbers
        result = add_numbers_from_text(text_content)
        
        return jsonify({
            "status": "success",
            "message": f"Successfully added {result['added']} numbers",
            "added": result['added'],
            "duplicates": result['duplicates'],
            "total_in_pool": result['total_in_pool']
        })
        
    except Exception as e:
        print(f"❌ Error in upload_numbers: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/numbers/pool', methods=['GET'])
def get_number_pool_status():
    """Get current number pool status"""
    try:
        preview = number_pool[:10] if number_pool else []
        
        return jsonify({
            "status": "success",
            "available_numbers": available_numbers_count,
            "total_loaded": total_numbers_loaded,
            "served_count": numbers_served_count,
            "preview": preview
        })
    except Exception as e:
        print(f"❌ Error in get_number_pool_status: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/numbers/clear', methods=['POST'])
def clear_numbers():
    """Clear all numbers from the pool"""
    try:
        clear_number_pool()
        return jsonify({
            "status": "success",
            "message": "Number pool cleared successfully",
            "available_numbers": 0
        })
    except Exception as e:
        print(f"❌ Error in clear_numbers: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/numbers/add', methods=['POST'])
def add_single_number():
    """Add a single number to the pool"""
    try:
        data = request.json
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400
            
        number = data.get('number', '')
        
        if not number:
            return jsonify({"status": "error", "message": "Number is required"}), 400
        
        clean_number = validate_and_clean_number(number)
        
        if not clean_number:
            return jsonify({"status": "error", "message": "Invalid phone number"}), 400
        
        global total_numbers_loaded, available_numbers_count
        
        if clean_number in served_numbers or clean_number in number_pool:
            return jsonify({
                "status": "error",
                "message": "Number already exists in pool or has been served"
            }), 409
        
        number_pool.append(clean_number)
        total_numbers_loaded += 1
        available_numbers_count = len(number_pool)
        update_number_pool_stats()
        
        save_numbers_pool()
        
        return jsonify({
            "status": "success",
            "message": "Number added successfully",
            "number": clean_number,
            "total_in_pool": available_numbers_count
        })
        
    except Exception as e:
        print(f"❌ Error in add_single_number: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/otp/<number>', methods=['GET'])
def get_otp_by_number(number):
    """Get OTPs for a specific number"""
    # Clean the number
    clean_number = re.sub(r'[^\d]', '', number)
    
    if clean_number in otp_storage:
        otps = otp_storage[clean_number]
        
        # Filter options
        limit = request.args.get('limit', type=int)
        service = request.args.get('service')
        from_time = request.args.get('from')
        
        filtered_otps = otps
        
        if service:
            filtered_otps = [o for o in filtered_otps if o['service'].lower() == service.lower()]
        
        if from_time:
            from_dt = datetime.fromisoformat(from_time)
            filtered_otps = [o for o in filtered_otps if datetime.fromisoformat(o['timestamp']) >= from_dt]
        
        if limit:
            filtered_otps = filtered_otps[-limit:]
        
        return jsonify({
            "number": clean_number,
            "count": len(filtered_otps),
            "otps": filtered_otps
        })
    
    return jsonify({"number": clean_number, "count": 0, "otps": []})

@app.route('/api/otp', methods=['POST'])
def query_otp():
    """POST endpoint to query OTP by number"""
    try:
        data = request.json
        number = data.get('number', '')
        
        if not number:
            return jsonify({"error": "Number is required"}), 400
        
        clean_number = re.sub(r'[^\d]', '', number)
        
        if clean_number in otp_storage and otp_storage[clean_number]:
            # Get the latest OTP
            latest_otp = otp_storage[clean_number][-1]
            
            return jsonify({
                "status": "success",
                "number": clean_number,
                "otp": latest_otp['otp'],
                "service": latest_otp['service'],
                "timestamp": latest_otp['time_formatted'],
                "full_message": latest_otp['full_message']
            })
        else:
            return jsonify({
                "status": "pending",
                "number": clean_number,
                "message": "No OTP found for this number"
            })
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get current statistics"""
    return jsonify({
        "total_otps": stats["total_otps"],
        "today_otps": stats["today_otps"],
        "active_numbers": len(stats["active_numbers"]),
        "last_otp_time": stats["last_otp_time"],
        "connection_status": stats["connection_status"],
        "uptime": stats["uptime"],
        "last_connection_attempt": stats["last_connection_attempt"],
        "numbers_pool": stats["numbers_pool"]
    })

@app.route('/api/otps/recent', methods=['GET'])
def get_recent_otps():
    """Get recent OTPs across all numbers"""
    limit = request.args.get('limit', 50, type=int)
    
    all_otps = []
    for number, otps in otp_storage.items():
        for otp in otps:
            otp_copy = otp.copy()
            otp_copy['number'] = number
            all_otps.append(otp_copy)
    
    # Sort by timestamp (newest first)
    all_otps.sort(key=lambda x: x['timestamp'], reverse=True)
    
    return jsonify({
        "count": len(all_otps[:limit]),
        "otps": all_otps[:limit]
    })

@app.route('/api/otps/clear', methods=['POST'])
def clear_otps():
    """Clear all OTPs"""
    try:
        data = request.json or {}
        number = data.get('number')
        
        if number:
            clean_number = re.sub(r'[^\d]', '', number)
            if clean_number in otp_storage:
                del otp_storage[clean_number]
                stats["active_numbers"].discard(clean_number)
        else:
            otp_storage.clear()
            stats["active_numbers"].clear()
            stats["total_otps"] = 0
            stats["today_otps"] = 0
        
        save_otps()
        save_stats()
        
        return jsonify({"status": "success", "message": "OTPs cleared"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/connection/restart', methods=['POST'])
def restart_connection():
    """Restart WebSocket connection"""
    try:
        stop_websocket()
        time.sleep(1)
        start_websocket()
        return jsonify({"status": "success", "message": "Connection restarted"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Initialize
print("🚀 Starting OTP Dashboard Server...")
load_otps()
load_numbers_pool()  # Load number pool on startup
start_websocket()

if __name__ == '__main__':
    # Run with threading disabled for Flask to avoid issues
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=False)
