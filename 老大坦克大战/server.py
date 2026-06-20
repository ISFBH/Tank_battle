"""
坦克大战 - 局域网多人联机中继服务器
使用方法:
  1. 安装依赖: pip install websockets
  2. 启动服务器: python server.py
  3. 两个玩家在浏览器中打开 tank-battle.html，进入"多人联机"选项
  4. 一个玩家创建房间，另一个输入房间号加入
"""

import asyncio
import json
import random
import string
import time
from collections import defaultdict

try:
    import websockets
except ImportError:
    print("=" * 50)
    print("需要安装 websockets 库:")
    print("  pip install websockets")
    print("=" * 50)
    exit(1)

# ==================== 配置 ====================
HOST = "0.0.0.0"       # 监听所有网络接口
PORT = 8765             # WebSocket 端口
MAX_ROOMS = 50          # 最大房间数
ROOM_CODE_LEN = 5       # 房间号长度
HEARTBEAT_INTERVAL = 10  # 心跳间隔(秒)
PLAYER_TIMEOUT = 30      # 玩家超时(秒)

# ==================== 全局状态 ====================
rooms = {}       # room_code -> Room
ws_to_room = {}  # websocket -> room_code
ws_to_pid = {}   # websocket -> player_id (0 or 1)


def generate_room_code():
    """生成随机房间号"""
    chars = string.ascii_uppercase + string.digits
    while True:
        code = ''.join(random.choices(chars, k=ROOM_CODE_LEN))
        if code not in rooms:
            return code


class Room:
    def __init__(self, code, host_ws):
        self.code = code
        self.players = {0: None, 1: None}  # player_id -> websocket
        self.game_state = "waiting"  # "waiting" | "playing" | "ended"
        self.level = 1
        self.difficulty = "classic"
        self.created_at = time.time()
        self.last_activity = time.time()

    @property
    def player_count(self):
        return sum(1 for ws in self.players.values() if ws is not None)

    def is_full(self):
        return self.player_count >= 2

    def get_other_ws(self, ws):
        """获取同房间另一个玩家的 websocket"""
        for pid, p_ws in self.players.items():
            if p_ws is ws:
                continue
            return p_ws
        return None

    def get_player_id(self, ws):
        """获取玩家ID"""
        for pid, p_ws in self.players.items():
            if p_ws is ws:
                return pid
        return None


async def broadcast_to_room(room, message, exclude_ws=None):
    """向房间内所有玩家广播消息"""
    data = json.dumps(message, ensure_ascii=False)
    tasks = []
    for pid, ws in room.players.items():
        if ws is not None and ws is not exclude_ws:
            tasks.append(send_safe(ws, data))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def send_safe(ws, data):
    """安全发送消息"""
    try:
        await ws.send(data)
    except Exception:
        pass


async def remove_player(ws):
    """移除玩家"""
    if ws not in ws_to_room:
        return
    room_code = ws_to_room[ws]
    pid = ws_to_pid.get(ws, -1)
    room = rooms.get(room_code)
    if room:
        room.players[pid] = None
        other_ws = room.get_other_ws(ws)
        if other_ws:
            await send_safe(other_ws, json.dumps({
                "type": "player_left",
                "playerId": pid
            }, ensure_ascii=False))
        # 如果房间空了，删除房间
        if room.player_count == 0:
            del rooms[room_code]
    del ws_to_room[ws]
    if ws in ws_to_pid:
        del ws_to_pid[ws]
    print(f"[房间 {room_code}] 玩家 {pid} 断开连接")


async def handle_message(ws, room, raw_msg):
    """处理客户端消息"""
    try:
        msg = json.loads(raw_msg)
    except json.JSONDecodeError:
        return

    msg_type = msg.get("type", "")

    if msg_type == "ping":
        await send_safe(ws, json.dumps({"type": "pong"}))
        return

    if msg_type == "input":
        # 转发玩家输入给另一个玩家
        pid = room.get_player_id(ws)
        if pid is not None:
            msg["playerId"] = pid
            other_ws = room.get_other_ws(ws)
            if other_ws:
                await send_safe(other_ws, json.dumps(msg, ensure_ascii=False))
        return

    if msg_type == "player_state":
        # 转发玩家状态给另一个玩家
        pid = room.get_player_id(ws)
        if pid is not None:
            msg["playerId"] = pid
            other_ws = room.get_other_ws(ws)
            if other_ws:
                await send_safe(other_ws, json.dumps(msg, ensure_ascii=False))
        return

    if msg_type == "bullet":
        # 转发子弹创建
        pid = room.get_player_id(ws)
        if pid is not None:
            msg["playerId"] = pid
            other_ws = room.get_other_ws(ws)
            if other_ws:
                await send_safe(other_ws, json.dumps(msg, ensure_ascii=False))
        return

    if msg_type == "enemy_hit":
        # 转发敌人被击杀事件
        pid = room.get_player_id(ws)
        if pid is not None:
            msg["playerId"] = pid
            other_ws = room.get_other_ws(ws)
            if other_ws:
                await send_safe(other_ws, json.dumps(msg, ensure_ascii=False))
        return

    if msg_type == "game_event":
        # 转发游戏事件（基地被毁、游戏结束等）
        pid = room.get_player_id(ws)
        if pid is not None:
            msg["playerId"] = pid
            await broadcast_to_room(room, msg, exclude_ws=None)
        return

    if msg_type == "start_game":
        # 房主开始游戏
        pid = room.get_player_id(ws)
        if pid == 0:  # 只有房主可以开始
            room.game_state = "playing"
            room.level = msg.get("level", 1)
            room.difficulty = msg.get("difficulty", "classic")
            await broadcast_to_room(room, {
                "type": "game_start",
                "level": room.level,
                "difficulty": room.difficulty
            })
        return

    if msg_type == "restart_game":
        # 重新开始
        pid = room.get_player_id(ws)
        if pid == 0:
            room.game_state = "playing"
            await broadcast_to_room(room, {
                "type": "game_restart",
                "level": room.level,
                "difficulty": room.difficulty
            })
        return

    if msg_type == "level_complete":
        # 关卡完成
        pid = room.get_player_id(ws)
        if pid is not None:
            await broadcast_to_room(room, {
                "type": "level_complete",
                "playerId": pid
            })
        return

    if msg_type == "chat":
        # 聊天消息
        pid = room.get_player_id(ws)
        if pid is not None:
            msg["playerId"] = pid
            await broadcast_to_room(room, msg)
        return


async def handler(websocket):
    """WebSocket 连接处理"""
    print(f"[新连接] {websocket.remote_address}")
    try:
        # 等待第一条消息（创建房间或加入房间）
        first_msg = await asyncio.wait_for(websocket.recv(), timeout=30)
        msg = json.loads(first_msg)
        msg_type = msg.get("type", "")

        if msg_type == "create_room":
            # 创建房间
            if len(rooms) >= MAX_ROOMS:
                await send_safe(websocket, json.dumps({
                    "type": "error",
                    "message": "服务器房间已满"
                }))
                return

            code = generate_room_code()
            room = Room(code, websocket)
            room.players[0] = websocket
            rooms[code] = room
            ws_to_room[websocket] = code
            ws_to_pid[websocket] = 0

            await send_safe(websocket, json.dumps({
                "type": "room_created",
                "roomCode": code,
                "playerId": 0
            }))
            print(f"[房间 {code}] 创建成功，等待另一名玩家加入...")

        elif msg_type == "join_room":
            # 加入房间
            code = msg.get("roomCode", "").upper().strip()
            room = rooms.get(code)
            if not room:
                await send_safe(websocket, json.dumps({
                    "type": "error",
                    "message": "房间不存在"
                }))
                return
            if room.is_full():
                await send_safe(websocket, json.dumps({
                    "type": "error",
                    "message": "房间已满"
                }))
                return

            # 分配玩家ID
            pid = 0 if room.players[0] is None else 1
            room.players[pid] = websocket
            ws_to_room[websocket] = code
            ws_to_pid[websocket] = pid

            # 通知加入者
            await send_safe(websocket, json.dumps({
                "type": "room_joined",
                "roomCode": code,
                "playerId": pid
            }))

            # 通知房主有人加入
            host_ws = room.players[0] if pid == 1 else room.players[1]
            if host_ws:
                await send_safe(host_ws, json.dumps({
                    "type": "player_joined",
                    "playerId": pid
                }))

            print(f"[房间 {code}] 玩家 {pid} 加入，当前 {room.player_count}/2 人")

        else:
            await send_safe(websocket, json.dumps({
                "type": "error",
                "message": "未知操作"
            }))
            return

        # 消息循环
        async for raw_msg in websocket:
            room_code = ws_to_room.get(websocket)
            if room_code and room_code in rooms:
                await handle_message(websocket, rooms[room_code], raw_msg)

    except asyncio.TimeoutError:
        await send_safe(websocket, json.dumps({
            "type": "error",
            "message": "连接超时"
        }))
    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        print(f"[错误] {e}")
    finally:
        await remove_player(websocket)


async def main():
    print("=" * 50)
    print("  坦克大战 - 局域网多人联机服务器")
    print("=" * 50)
    # 获取本机IP
    try:
        s = __import__('socket').socket(__import__('socket').AF_INET, __import__('socket').SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
    except Exception:
        local_ip = "127.0.0.1"

    print(f"  服务器地址: ws://{local_ip}:{PORT}")
    print(f"  监听端口: {PORT}")
    print(f"  等待玩家连接...")
    print("=" * 50)

    async with websockets.serve(handler, HOST, PORT):
        await asyncio.Future()  # 永不结束


if __name__ == "__main__":
    asyncio.run(main())
