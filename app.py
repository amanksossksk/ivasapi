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
import copy

app = Flask(__name__)
CORS(app)

# Configuration Files
WSS_CONFIG_FILE = "wss.json"
OTP_FILE = "otp.json"
STATS_FILE = "stats.json"
POOLS_FILE = "pools.json"
POOLS_CONFIG_FILE = "pools_config.json"

# Global variables
otp_storage = defaultdict(list)
ws_connection = None
ws_thread = None
is_ws_running = False

# Pool Management
pools = {}
current_pool = "default"
pool_stats = {}

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
        "served": 0,
        "current_pool": "default"
    }
}

# ==================== POOL MANAGEMENT ====================

def load_pools_config():
    """Load pool configuration"""
    default_config = {
        "pools": {
            "default": {
                "name": "Default Pool",
                "description": "Main number pool",
                "country": "All",
                "active": True
            }
        },
        "current_pool": "default"
    }
    
    try:
        if os.path.exists(POOLS_CONFIG_FILE):
            with open(POOLS_CONFIG_FILE, 'r') as f:
                config = json.load(f)
                # Ensure default pool exists in config
                if 'pools' not in config:
                    config['pools'] = {}
                if 'default' not in config['pools']:
                    config['pools']['default'] = default_config['pools']['default']
                if 'current_pool' not in config:
                    config['current_pool'] = 'default'
                return config
        else:
            with open(POOLS_CONFIG_FILE, 'w') as f:
                json.dump(default_config, f, indent=2)
            return default_config
    except Exception as e:
        print(f"❌ Error loading pools config: {e}")
        return default_config

def save_pools_config(config):
    """Save pool configuration"""
    try:
        with open(POOLS_CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
        return True
    except Exception as e:
        print(f"❌ Error saving pools config: {e}")
        return False

def load_pools():
    """Load all pools from persistent storage"""
    global pools, current_pool, pool_stats
    
    try:
        if os.path.exists(POOLS_FILE):
            with open(POOLS_FILE, 'r') as f:
                data = json.load(f)
                pools = data.get('pools', {})
                current_pool = data.get('current_pool', 'default')
                pool_stats = data.get('pool_stats', {})
                
                # Ensure default pool exists
                if 'default' not in pools:
                    pools['default'] = {
                        "numbers": [],
                        "served": [],
                        "total_loaded": 0,
                        "served_count": 0
                    }
                
                # Ensure current_pool exists
                if current_pool not in pools:
                    current_pool = 'default'
                
                update_pool_stats()
                print(f"✅ Loaded {len(pools)} pools")
                return True
        else:
            # Initialize with default pool
            pools = {
                "default": {
                    "numbers": [],
                    "served": [],
                    "total_loaded": 0,
                    "served_count": 0
                }
            }
            current_pool = "default"
            pool_stats = {}
            save_pools()
            print("✅ Created default pool")
            return True
    except Exception as e:
        print(f"❌ Error loading pools: {e}")
        # Fallback to default
        pools = {
            "default": {
                "numbers": [],
                "served": [],
                "total_loaded": 0,
                "served_count": 0
            }
        }
        current_pool = "default"
        pool_stats = {}
        return False

def save_pools():
    """Save all pools to persistent storage"""
    try:
        data = {
            'pools': pools,
            'current_pool': current_pool,
            'pool_stats': pool_stats,
            'last_updated': datetime.now().isoformat()
        }
        
        temp_file = POOLS_FILE + '.tmp'
        with open(temp_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        os.replace(temp_file, POOLS_FILE)
        return True
    except Exception as e:
        print(f"❌ Error saving pools: {e}")
        return False

def update_pool_stats():
    """Update statistics for current pool"""
    global pool_stats
    
    try:
        pool_data = pools.get(current_pool, {})
        if not pool_data:
            pool_data = {"numbers": [], "served": [], "total_loaded": 0, "served_count": 0}
        
        available = len(pool_data.get('numbers', []))
        total_loaded = pool_data.get('total_loaded', 0)
        served = pool_data.get('served_count', 0)
        
        pool_stats[current_pool] = {
            "available": available,
            "total_loaded": total_loaded,
            "served": served
        }
        
        # Update main stats
        stats["numbers_pool"] = {
            "available": available,
            "total_loaded": total_loaded,
            "served": served,
            "current_pool": current_pool
        }
    except Exception as e:
        print(f"❌ Error updating pool stats: {e}")

def set_current_pool(pool_name):
    """Set the current active pool"""
    global current_pool
    
    if pool_name in pools:
        current_pool = pool_name
        update_pool_stats()
        save_pools()
        
        # Update config
        config = load_pools_config()
        config['current_pool'] = pool_name
        save_pools_config(config)
        
        print(f"✅ Switched to pool: {pool_name}")
        return True
    else:
        print(f"❌ Pool not found: {pool_name}")
        return False

def create_pool(pool_name, description="", country="All"):
    """Create a new pool"""
    if pool_name in pools:
        return False
    
    # Clean pool name
    pool_name = pool_name.strip().replace(' ', '_').lower()
    
    pools[pool_name] = {
        "numbers": [],
        "served": [],
        "total_loaded": 0,
        "served_count": 0
    }
    
    # Update config
    config = load_pools_config()
    config['pools'][pool_name] = {
        "name": pool_name,
        "description": description,
        "country": country,
        "active": True
    }
    save_pools_config(config)
    
    save_pools()
    print(f"✅ Created pool: {pool_name}")
    return True

def delete_pool(pool_name):
    """Delete a pool (cannot delete default)"""
    if pool_name == "default":
        return False
    
    if pool_name in pools:
        del pools[pool_name]
        
        # Update config
        config = load_pools_config()
        if pool_name in config['pools']:
            del config['pools'][pool_name]
        save_pools_config(config)
        
        # If current pool was deleted, switch to default
        if current_pool == pool_name:
            set_current_pool("default")
        
        save_pools()
        print(f"✅ Deleted pool: {pool_name}")
        return True
    return False

def get_pool_numbers(pool_name=None):
    """Get numbers from a specific pool"""
    if pool_name is None:
        pool_name = current_pool
    
    pool_data = pools.get(pool_name, {})
    return pool_data.get('numbers', [])

def validate_and_clean_number(number):
    """Validate and clean a phone number"""
    clean_number = re.sub(r'[^\d]', '', str(number))
    
    if len(clean_number) < 10 or len(clean_number) > 15:
        return None
    
    if clean_number.isdigit() and len(clean_number) >= 10:
        return clean_number
    
    return None

def add_numbers_to_pool(text_content, pool_name=None):
    """Add numbers to a specific pool"""
    global pools
    
    if pool_name is None:
        pool_name = current_pool
    
    if pool_name not in pools:
        return {'added': 0, 'duplicates': 0, 'total_in_pool': 0, 'error': 'Pool not found'}
    
    numbers = []
    lines = text_content.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        
        found_numbers = re.findall(r'\b\d{10,15}\b', line)
        for num in found_numbers:
            clean_number = validate_and_clean_number(num)
            if clean_number:
                numbers.append(clean_number)
    
    pool_data = pools[pool_name]
    served_set = set(pool_data.get('served', []))
    current_numbers = set(pool_data.get('numbers', []))
    
    added_count = 0
    duplicate_count = 0
    
    for number in numbers:
        if number not in served_set and number not in current_numbers:
            pool_data['numbers'].append(number)
            added_count += 1
        else:
            duplicate_count += 1
    
    pool_data['total_loaded'] = pool_data.get('total_loaded', 0) + added_count
    update_pool_stats()
    save_pools()
    
    return {
        'added': added_count,
        'duplicates': duplicate_count,
        'total_in_pool': len(pool_data['numbers']),
        'pool': pool_name
    }

def get_next_number_from_pool(pool_name=None):
    """Get next number from a specific pool (FIFO)"""
    global pools
    
    if pool_name is None:
        pool_name = current_pool
    
    if pool_name not in pools:
        return None
    
    pool_data = pools[pool_name]
    numbers = pool_data.get('numbers', [])
    
    if numbers:
        number = numbers.pop(0)
        if 'served' not in pool_data:
            pool_data['served'] = []
        pool_data['served'].append(number)
        pool_data['served_count'] = len(pool_data['served'])
        update_pool_stats()
        save_pools()
        return number
    
    return None

def clear_pool(pool_name=None):
    """Clear a specific pool"""
    global pools
    
    if pool_name is None:
        pool_name = current_pool
    
    if pool_name not in pools:
        return False
    
    pool_data = pools[pool_name]
    pool_data['numbers'] = []
    pool_data['served'] = []
    pool_data['total_loaded'] = 0
    pool_data['served_count'] = 0
    
    update_pool_stats()
    save_pools()
    return True

def get_pool_stats(pool_name=None):
    """Get statistics for a specific pool"""
    if pool_name is None:
        pool_name = current_pool
    
    pool_data = pools.get(pool_name, {})
    return {
        "available": len(pool_data.get('numbers', [])),
        "total_loaded": pool_data.get('total_loaded', 0),
        "served": pool_data.get('served_count', 0),
        "pool": pool_name
    }

def get_all_pools_info():
    """Get information about all pools"""
    info = {}
    for pool_name, pool_data in pools.items():
        info[pool_name] = {
            "available": len(pool_data.get('numbers', [])),
            "total_loaded": pool_data.get('total_loaded', 0),
            "served": pool_data.get('served_count', 0)
        }
    return info

# ==================== OTP FUNCTIONS ====================

def load_otps():
    """Load OTPs from JSON file"""
    try:
        if os.path.exists(OTP_FILE):
            with open(OTP_FILE, 'r') as f:
                data = json.load(f)
                for number, otps in data.items():
                    otp_storage[number] = otps
                    stats["active_numbers"].add(number)
            
            stats["total_otps"] = sum(len(otps) for otps in otp_storage.values())
            
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
        stats_copy["pools"] = get_all_pools_info()
        stats_copy["current_pool"] = current_pool
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
    
    otp_storage[number].append(otp_data)
    
    stats["total_otps"] += 1
    stats["today_otps"] += 1
    stats["active_numbers"].add(number)
    stats["last_otp_time"] = datetime.now().isoformat()
    
    config = load_wss_config()
    max_otps = config.get("max_otps_per_number", 100)
    if len(otp_storage[number]) > max_otps:
        otp_storage[number] = otp_storage[number][-max_otps:]
    
    save_otps()
    save_stats()
    
    print(f"📝 Saved OTP for +{number} - {info['service']}: {info['otp']}")
    return otp_data

# ==================== WEBSOCKET ====================

def load_wss_config():
    """Load WebSocket configuration"""
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
                for key in default_config:
                    if key not in config:
                        config[key] = default_config[key]
                return config
        else:
            with open(WSS_CONFIG_FILE, 'w') as f:
                json.dump(default_config, f, indent=2)
            return default_config
    except Exception as e:
        print(f"❌ Error loading WSS config: {e}")
        return default_config

def save_wss_config(config):
    """Save WebSocket configuration"""
    try:
        with open(WSS_CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
        return True
    except Exception as e:
        print(f"❌ Error saving WSS config: {e}")
        return False

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
                
                initial_message = await websocket.recv()
                ping_interval = config.get("ping_interval", 25000)
                
                try:
                    if initial_message.startswith("0{") and initial_message.endswith("}"):
                        data = json.loads(initial_message[1:])
                        ping_interval = data.get("pingInterval", ping_interval)
                except:
                    pass
                
                await websocket.send("40/livesms,")
                
                ping_task = asyncio.create_task(send_ping(websocket, ping_interval))
                
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
                                
                                otp_match = re.search(r"\b\d{4,8}\b", message_text)
                                otp = otp_match.group(0) if otp_match else None
                                
                                if otp and sms.get("recipient"):
                                    number = sms.get("recipient")
                                    
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

# ==================== API ROUTES ====================

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
                stop_websocket()
                time.sleep(1)
                start_websocket()
                return jsonify({"status": "success", "message": "Configuration updated"})
            else:
                return jsonify({"status": "error", "message": "Failed to save config"}), 500
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 400

# ==================== POOL API ROUTES ====================

@app.route('/api/pools', methods=['GET'])
def get_pools():
    """Get all pools information"""
    try:
        pools_info = get_all_pools_info()
        config = load_pools_config()
        
        return jsonify({
            "status": "success",
            "pools": pools_info,
            "current_pool": current_pool,
            "pool_config": config.get('pools', {})
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/pools/switch', methods=['POST'])
def switch_pool():
    """Switch to a different pool"""
    try:
        data = request.json
        pool_name = data.get('pool')
        
        if not pool_name:
            return jsonify({"status": "error", "message": "Pool name required"}), 400
        
        if set_current_pool(pool_name):
            return jsonify({
                "status": "success",
                "message": f"Switched to pool: {pool_name}",
                "current_pool": current_pool,
                "pool_stats": get_pool_stats(pool_name)
            })
        else:
            return jsonify({"status": "error", "message": "Pool not found"}), 404
            
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/pools/create', methods=['POST'])
def create_new_pool():
    """Create a new pool"""
    try:
        data = request.json
        pool_name = data.get('name', '').strip()
        description = data.get('description', '')
        country = data.get('country', 'All')
        
        if not pool_name:
            return jsonify({"status": "error", "message": "Pool name required"}), 400
        
        # Clean pool name
        pool_name = pool_name.replace(' ', '_').lower()
        
        if create_pool(pool_name, description, country):
            return jsonify({
                "status": "success",
                "message": f"Pool '{pool_name}' created",
                "pool": pool_name
            })
        else:
            return jsonify({"status": "error", "message": "Pool already exists"}), 409
            
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/pools/delete', methods=['POST'])
def delete_existing_pool():
    """Delete a pool"""
    try:
        data = request.json
        pool_name = data.get('pool')
        
        if not pool_name:
            return jsonify({"status": "error", "message": "Pool name required"}), 400
        
        if delete_pool(pool_name):
            return jsonify({
                "status": "success",
                "message": f"Pool '{pool_name}' deleted"
            })
        else:
            return jsonify({"status": "error", "message": "Cannot delete default pool or pool not found"}), 400
            
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/pools/<pool_name>/numbers', methods=['GET'])
def get_pool_numbers_api(pool_name):
    """Get numbers from a specific pool"""
    try:
        numbers = get_pool_numbers(pool_name)
        preview = numbers[:10] if numbers else []
        
        return jsonify({
            "status": "success",
            "pool": pool_name,
            "available": len(numbers),
            "preview": preview
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/pools/<pool_name>/upload', methods=['POST'])
def upload_numbers_to_pool(pool_name):
    """Upload numbers to a specific pool"""
    try:
        if 'file' not in request.files:
            return jsonify({"status": "error", "message": "No file uploaded"}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({"status": "error", "message": "No file selected"}), 400
        
        if not file.filename.endswith('.txt'):
            return jsonify({"status": "error", "message": "Only .txt files are allowed"}), 400
        
        text_content = file.read().decode('utf-8')
        result = add_numbers_to_pool(text_content, pool_name)
        
        if 'error' in result:
            return jsonify({"status": "error", "message": result['error']}), 404
        
        return jsonify({
            "status": "success",
            "message": f"Successfully added {result['added']} numbers to pool '{pool_name}'",
            "added": result['added'],
            "duplicates": result['duplicates'],
            "total_in_pool": result['total_in_pool'],
            "pool": pool_name
        })
        
    except Exception as e:
        print(f"❌ Error in upload_numbers_to_pool: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/pools/<pool_name>/add', methods=['POST'])
def add_single_number_to_pool(pool_name):
    """Add a single number to a specific pool"""
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
        
        if pool_name not in pools:
            return jsonify({"status": "error", "message": "Pool not found"}), 404
        
        pool_data = pools[pool_name]
        served_set = set(pool_data.get('served', []))
        current_numbers = set(pool_data.get('numbers', []))
        
        if clean_number in served_set or clean_number in current_numbers:
            return jsonify({
                "status": "error",
                "message": "Number already exists in pool or has been served"
            }), 409
        
        pool_data['numbers'].append(clean_number)
        pool_data['total_loaded'] = pool_data.get('total_loaded', 0) + 1
        update_pool_stats()
        save_pools()
        
        return jsonify({
            "status": "success",
            "message": "Number added successfully",
            "number": clean_number,
            "total_in_pool": len(pool_data['numbers']),
            "pool": pool_name
        })
        
    except Exception as e:
        print(f"❌ Error in add_single_number_to_pool: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/pools/<pool_name>/clear', methods=['POST'])
def clear_pool_api(pool_name):
    """Clear a specific pool"""
    try:
        if clear_pool(pool_name):
            return jsonify({
                "status": "success",
                "message": f"Pool '{pool_name}' cleared successfully",
                "pool": pool_name
            })
        else:
            return jsonify({"status": "error", "message": "Pool not found"}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/pools/<pool_name>/stats', methods=['GET'])
def get_pool_stats_api(pool_name):
    """Get statistics for a specific pool"""
    try:
        stats_data = get_pool_stats(pool_name)
        return jsonify({
            "status": "success",
            **stats_data
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ==================== LEGACY API ROUTES ====================

@app.route('/api/getnumber', methods=['GET'])
def get_number():
    """Get the next available number from the current pool"""
    try:
        number = get_next_number_from_pool()
        
        if number:
            pool_stats_data = get_pool_stats()
            return jsonify({
                "status": "success",
                "number": number,
                "remaining_in_pool": pool_stats_data['available'],
                "pool": current_pool,
                "timestamp": datetime.now().isoformat()
            })
        else:
            return jsonify({
                "status": "empty",
                "message": "No numbers available in the current pool",
                "remaining_in_pool": 0,
                "pool": current_pool
            }), 404
            
    except Exception as e:
        print(f"❌ Error in get_number: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/numbers/upload', methods=['POST'])
def upload_numbers():
    """Upload numbers to the current pool"""
    try:
        if 'file' not in request.files:
            return jsonify({"status": "error", "message": "No file uploaded"}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({"status": "error", "message": "No file selected"}), 400
        
        if not file.filename.endswith('.txt'):
            return jsonify({"status": "error", "message": "Only .txt files are allowed"}), 400
        
        text_content = file.read().decode('utf-8')
        result = add_numbers_to_pool(text_content)
        
        return jsonify({
            "status": "success",
            "message": f"Successfully added {result['added']} numbers to pool '{current_pool}'",
            "added": result['added'],
            "duplicates": result['duplicates'],
            "total_in_pool": result['total_in_pool'],
            "pool": current_pool
        })
        
    except Exception as e:
        print(f"❌ Error in upload_numbers: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/numbers/pool', methods=['GET'])
def get_number_pool_status():
    """Get current pool status"""
    try:
        numbers = get_pool_numbers()
        preview = numbers[:10] if numbers else []
        pool_stats_data = get_pool_stats()
        
        return jsonify({
            "status": "success",
            "available_numbers": pool_stats_data['available'],
            "total_loaded": pool_stats_data['total_loaded'],
            "served_count": pool_stats_data['served'],
            "current_pool": current_pool,
            "preview": preview
        })
    except Exception as e:
        print(f"❌ Error in get_number_pool_status: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/numbers/clear', methods=['POST'])
def clear_numbers():
    """Clear the current pool"""
    try:
        clear_pool()
        return jsonify({
            "status": "success",
            "message": f"Pool '{current_pool}' cleared successfully",
            "available_numbers": 0,
            "pool": current_pool
        })
    except Exception as e:
        print(f"❌ Error in clear_numbers: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/numbers/add', methods=['POST'])
def add_single_number():
    """Add a single number to the current pool"""
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
        
        pool_data = pools[current_pool]
        served_set = set(pool_data.get('served', []))
        current_numbers = set(pool_data.get('numbers', []))
        
        if clean_number in served_set or clean_number in current_numbers:
            return jsonify({
                "status": "error",
                "message": "Number already exists in pool or has been served"
            }), 409
        
        pool_data['numbers'].append(clean_number)
        pool_data['total_loaded'] = pool_data.get('total_loaded', 0) + 1
        update_pool_stats()
        save_pools()
        
        return jsonify({
            "status": "success",
            "message": "Number added successfully",
            "number": clean_number,
            "total_in_pool": len(pool_data['numbers']),
            "pool": current_pool
        })
        
    except Exception as e:
        print(f"❌ Error in add_single_number: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# ==================== OTP API ROUTES ====================

@app.route('/api/otp/<number>', methods=['GET'])
def get_otp_by_number(number):
    """Get OTPs for a specific number"""
    clean_number = re.sub(r'[^\d]', '', number)
    
    if clean_number in otp_storage:
        otps = otp_storage[clean_number]
        
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
    try:
        return jsonify({
            "total_otps": stats["total_otps"],
            "today_otps": stats["today_otps"],
            "active_numbers": len(stats["active_numbers"]),
            "last_otp_time": stats["last_otp_time"],
            "connection_status": stats["connection_status"],
            "uptime": stats["uptime"],
            "last_connection_attempt": stats["last_connection_attempt"],
            "numbers_pool": stats["numbers_pool"],
            "current_pool": current_pool,
            "pools": get_all_pools_info()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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

# ==================== INITIALIZATION ====================

print("🚀 Starting OTP Dashboard Server with Multi-Pool Support...")
load_pools()
load_otps()
start_websocket()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=False)
