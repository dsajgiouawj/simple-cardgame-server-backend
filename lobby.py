# マッチング要求
#   新しくルームを立てるか、既存のルームに入れる
#   ルーム情報の更新
#   立てたまたは入ったルームのメンバーに通知
# ルームから抜ける
#   ルーム情報の更新
#   メンバーに通知
#   最後のメンバーならルーム情報の削除

from redis.client import Redis
from nanoid import generate
from typing import Dict, Any
import time
import random
import json

redis = Redis(host='192.168.3.16', decode_responses=True)
publisher = Redis(host='192.168.3.16', decode_responses=True)

CONNECTION_CHANNEL = 'connection-channel'
CONNECTION_KEY = 'connection-key'
REQUEST = 'request'
GAME_ID = 'game-id'


def main():
    while True:
        request_id = redis.lpop('requests:lobby')
        if request_id is None:
            time.sleep(1)
            continue

        request = redis.hgetall('requests:id:' + request_id)
        if request is None or CONNECTION_CHANNEL not in request or CONNECTION_KEY not in request:
            print('invalid request: request is not exist OR channel or key is not specified')
            continue

        if request[REQUEST] == 'match-make':
            match_make(request)
        elif request[REQUEST] == 'exit-room':
            exit_room(request)
        else:
            print('invalid request: request is neither match-make nor exit-room')


# channel チャンネル名
# id 送信先の指定
# response 送信先に渡すjsonの元になるdictionary
def publish(channel: str, connection_key: Any, response: Dict[str, Any]) -> None:
    response[CONNECTION_KEY] = connection_key
    publisher.publish(channel, json.dumps(response, ensure_ascii=False))


# プレイヤーIDを新規作成
def match_make(request: Dict[str, Any]):
    if request[GAME_ID] is None:
        publish(request[CONNECTION_CHANNEL], request[CONNECTION_KEY],
                {'type': 'response', 'error': 'game-id was not specified'})

    existing_room = redis.spop('rooms:waiting:' + request[GAME_ID])
    player_id = generate()
    player_password = generate()
    register_player(request[CONNECTION_CHANNEL], request[CONNECTION_KEY], player_id, player_password)
    if existing_room is not None:
        join_room(request, existing_room, player_id, player_password)
    else:
        create_room(request, player_id, player_password)


def register_player(connection_channel: str, connection_key: str, player_id: str, password: str):
    key = 'players:id:' + player_id + ':'
    pipe = redis.pipeline()
    pipe.set(key + CONNECTION_CHANNEL, connection_channel)
    pipe.set(key + CONNECTION_KEY, connection_key)
    pipe.set(key + 'password', password)
    pipe.execute()


def exit_room(request):
    pass


# 現状二人プレイ前提
def join_room(request: Dict[str, Any], room_id: str, player_id: str, player_password: str):
    key = 'rooms:id:' + room_id + ':'
    p0 = redis.get(key + 'player0')
    pipe = redis.pipeline()
    pipe.get('players:id:' + p0 + ':' + CONNECTION_CHANNEL)
    pipe.get('players:id:' + p0 + ':' + CONNECTION_KEY)
    pipe.set(key + 'full', 'True')
    pipe.incr(key + 'num-players')
    pipe.set(key + 'turn', '0')
    if random.random() < 0.5:
        pipe.set(key + 'player1', player_id)
        turn = 1
    else:
        pipe.set(key + 'player0', player_id)
        pipe.set(key + 'player1', p0)
        turn = 0
    pipe.sadd('rooms:playing', room_id)
    op_channel, op_key, *_ = pipe.execute()
    publish(request[CONNECTION_CHANNEL], request[CONNECTION_KEY],
            {'type': 'response', REQUEST: 'match-make', 'result': 'joined',
             'player-id': player_id, 'password': player_password, 'room-id': room_id,
             'opponents': [p0], 'turn': turn, 'start': 'true'})
    publish(op_channel, op_key,
            {'type': 'broadcast', REQUEST: 'match-make', 'result': 'joined',
             'new-player-id': player_id, 'turn': 1 - turn, 'start': 'true'})


def create_room(request: Dict[str, Any], player_id: str, player_password: str):
    room_id = generate()
    key = 'rooms:id:' + room_id + ':'
    pipe = redis.pipeline()
    pipe.set(key + 'player0', player_id)
    pipe.set(key + 'full', 'False')
    pipe.set(key + 'num-players', '1')
    pipe.sadd('rooms:waiting:' + request[GAME_ID], room_id)
    pipe.execute()
    publish(request[CONNECTION_CHANNEL], request[CONNECTION_KEY],
            {'type': 'response', REQUEST: 'match-make', 'result': 'created',
             'player-id': player_id, 'password': player_password, 'room-id': room_id})


if __name__ == '__main__':
    main()
