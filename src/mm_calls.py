import asyncio
import base64
import json
import random
import threading
import time
from urllib.parse import urljoin
import uuid

from ably import AblyRealtime
from ably.types.connectionstate import ConnectionState, ConnectionEvent
from ably.types.tokendetails import TokenDetails
import pysher
import requests
import schedule

from src import config
from src import constants
from src.log import logging


class MMInteractions:
    base_url: str = None
    balance: float = 0
    mm_keys: dict = dict()
    mm_session: dict = dict()
    all_tournaments: dict = dict()    # mapping from string to id
    my_tournaments: dict = dict()
    # key is event id, value is a list of event details and markets
    sport_events: dict = dict()
    wagers: dict = dict()    # all wagers bet in the session
    valid_odds: list = []
    websocket_service: str = ""
    pusher: pysher.Pusher = None

    # ably things
    ably: AblyRealtime = None
    available_channels: list = []

    def __init__(self):
        self.base_url = config.BASE_URL
        self.mm_keys = config.MM_KEYS

    def mm_login(self) -> dict:
        login_url = urljoin(self.base_url, config.URL['mm_login'])
        request_body = {
            'access_key': self.mm_keys.get('access_key'),
            'secret_key': self.mm_keys.get('secret_key'),
        }
        response = requests.post(login_url, data=json.dumps(request_body))
        if response.status_code != 200:
            logging.debug(response)
            logging.debug(
                "Please check your access key and secrete key to the user_info.json")
            raise Exception("login failed")
        mm_session = json.loads(response.content)['data']
        logging.info(mm_session)
        self.mm_session = mm_session
        logging.info("MM session started")
        return mm_session

    def seeding(self):
        # get allowed odds
        logging.info("start to get allowed odds")
        odds_ladder_url = urljoin(self.base_url, config.URL['mm_odds_ladder'])
        odds_response = requests.get(
            odds_ladder_url, headers=self.__get_auth_header())
        if odds_response.status_code != 200:
            logging.info(
                "not able to get valid odds from api, fall back to local constants")
            self.valid_odds = constants.VALID_ODDS_BACKUP
        else:
            self.valid_odds = odds_response.json()['data']

        # initiate available tournaments/sport_events
        # tournaments
        logging.info("start seeding tournaments/events/markets")
        t_url = urljoin(self.base_url, config.URL['mm_tournaments'])
        headers = self.__get_auth_header()
        all_tournaments_response = requests.get(t_url, headers=headers)
        if all_tournaments_response.status_code != 200:
            logging.error(
                f"not able to seed tournaments {all_tournaments_response.status_code}")
            raise Exception("not able to seed tournaments")
        all_tournaments = json.loads(all_tournaments_response.content).get(
            'data', {}).get('tournaments', {})
        self.all_tournaments = all_tournaments

        # get sportevents and markets of each
        event_url = urljoin(self.base_url, config.URL['mm_events'])
        market_url = urljoin(self.base_url, config.URL['mm_markets'])
        for one_t in all_tournaments:
            if one_t['name'] in config.TOURNAMENTS_INTERESTED:
                self.my_tournaments[one_t['id']] = one_t
                events_response = requests.get(
                    event_url, params={'tournament_id': one_t['id']}, headers=headers)
                if events_response.status_code == 200:
                    events = json.loads(events_response.content).get(
                        'data', {}).get('sport_events')
                    if events is None:
                        continue
                    for event in events:
                        market_response = requests.get(market_url, params={'event_id': event['event_id']},
                                                       headers=headers)
                        if market_response.status_code == 200:
                            markets = json.loads(market_response.content).get(
                                'data', {}).get('markets', {})
                            if markets is None:
                                # this is more like a bug in MM api, as the event actually already closed
                                continue
                            event['markets'] = markets
                            self.sport_events[event['event_id']] = event
                            logging.info(
                                f'successfully get markets of events {event["name"]}')
                        else:
                            logging.info(f'failed to get markets of events {event["name"]},'
                                         f' error: {market_response.reason}')
                else:
                    logging.info(
                        f'skip tournament {one_t["name"]} as api request failed')

        logging.info("Done, seeding")
        logging.info(f"found {len(self.my_tournaments)} tournament, ingested {len(self.sport_events)} "
                     f"sport events from {len(config.TOURNAMENTS_INTERESTED)} tournaments")

    def _get_channels(self, socket_id: float):
        # get websocket channels to subscribe to
        auth_endpoint_url = urljoin(self.base_url, config.URL['mm_auth'])
        # auth_endpoint_url = "http://localhost:19002/api/v1/mm/pusher"
        channels_response = requests.post(auth_endpoint_url,
                                          data={'socket_id': socket_id},
                                          headers=self.__get_auth_header())
        if channels_response.status_code != 200:
            logging.error("failed to get channels")
            raise Exception("failed to get channels")
        channels = channels_response.json()
        return channels.get('data', {}).get('authorized_channel', [])

    def get_connection_config(self):
        headers = self.__get_auth_header()
        connection_config_url = urljoin(
            self.base_url, config.URL['mm_connection_config'])
        response = requests.get(connection_config_url, headers=headers)
        if response.status_code != 200:
            logging.error(
                f"failed to get connection config")
            raise Exception("failed to get connection config")
        return response.json()

    async def ably_auth_callback(self, token_params) -> TokenDetails:
        auth_endpoint_url = urljoin(self.base_url, config.URL['mm_auth'])
        headers = self.__get_auth_header()
        response = requests.post(auth_endpoint_url,
                                 data={'subscriptions': [
                                     {'type': 'tournament', 'ids': []}]},
                                 headers=headers)
        json_body = response.json()
        if response.status_code != 200:
            logging.error(
                f"failed to register {response.status_code} {json_body}")
            raise Exception("failed to register")

        data = json_body.get('data', {})
        self.available_channels = data.get('authorized_channel', [])
        authorization = data.get('authorization', {})
        logging.info(f"successfully registered, {authorization}")
        return TokenDetails(
            token=authorization.get('token', None),
            expires=authorization.get('expires', None),
            issued=authorization.get('issued', None),
            capability=authorization.get('capability', None),
            client_id=authorization.get('client_id', None),
        )

    async def ably_binding_private_channels(self):
        if self.websocket_service != 'ably':
            return
        private_channel_name = ""
        private_user_events = []
        for channel in self.available_channels:
            if 'broadcast' not in channel['channel_name']:
                private_channel_name = channel['channel_name']
                private_user_events = channel['binding_events']
        private_channel = self.ably.channels.get(private_channel_name)

        def listener(message):
            print(
                f"processing ably message, event {message.name}, change_type {message.data.get('change_type', '')}")
            print(
                f"event details {base64.b64decode(message.data.get('payload', '{}'))}")
            print(f"=========={message.name}:{message.timestamp}==========")

        for private_event in private_user_events:
            await private_channel.subscribe(private_event['name'], listener)
            logging.info(
                f"subscribed to private channels {private_event['name']}, successfully")

    async def ably_binding_private_broadcast_channels(self):
        if self.websocket_service != 'ably':
            return
        broadcast_channel_name = ""
        for channel in self.available_channels:
            if 'broadcast' in channel['channel_name']:
                broadcast_channel_name = channel['channel_name']
        broadcast_channel = self.ably.channels.get(broadcast_channel_name)

        def listener(message):
            print(
                f"processing ably message, event {message.name}, change_type {message.data.get('change_type', '')}")
            print(
                f"event details {base64.b64decode(message.data.get('payload', '{}'))}")
            print(f"=========={message.name}:{message.timestamp}==========")

        for t_id in self.my_tournaments:
            event_name = f'tournament_{t_id}'
            await broadcast_channel.subscribe(event_name, listener)
            logging.info(
                f"subscribed to private broadcast channels {event_name}, successfully")

    async def subscribe(self):
        connection_config = self.get_connection_config()
        self.websocket_service = connection_config.get("service", "")
        if self.websocket_service == 'ably':
            if self.pusher is not None:
                self.pusher.disconnect()
                self.pusher = None
            if self.ably is not None:
                # do nothing we already connected
                return
            self.ably = AblyRealtime(
                auth_callback=self.ably_auth_callback, auto_connect=False)
            self.ably.connect()
            await self.ably.connection.once_async(ConnectionState.CONNECTED)
            logging.info("successfully connected to ably")
            await self.ably_binding_private_channels()
            await self.ably_binding_private_broadcast_channels()
        elif self.websocket_service == 'pusher':
            if self.ably is not None:
                await self.ably.connection.close()
                self.ably = None
            auth_endpoint_url = urljoin(self.base_url, config.URL['mm_auth'])
            auth_header = self.__get_auth_header()
            auth_headers = {
                "Authorization": auth_header['Authorization'],
                "header-subscriptions": '''[{"type":"tournament","ids":[]}]''',
            }
            self.pusher = pysher.Pusher(key=connection_config.get("key", ""), cluster=connection_config.get("cluster", ""),
                                        auth_endpoint=auth_endpoint_url,
                                        auth_endpoint_headers=auth_headers)

            def public_event_handler(*args, **kwargs):
                print("processing public, Args:", args)
                print(
                    f"event details {base64.b64decode(json.loads(args[0]).get('payload', '{}'))}")
                print("processing public, Kwargs:", kwargs)

            def private_event_handler(*args, **kwargs):
                print("processing private, Args:", args)
                print(
                    f"event details {base64.b64decode(json.loads(args[0]).get('payload', '{}'))}")
                print("processing private, Kwargs:", kwargs)

            # We can't subscribe until we've connected, so we use a callback handler
            # to subscribe when able
            def connect_handler(data):
                socket_id = json.loads(data)['socket_id']
                available_channels = self._get_channels(socket_id)
                broadcast_channel_name = None
                private_channel_name = None
                private_events = None
                for channel in available_channels:
                    if 'broadcast' in channel['channel_name']:
                        broadcast_channel_name = channel['channel_name']
                    else:
                        private_channel_name = channel['channel_name']
                        private_events = channel['binding_events']
                broadcast_channel = self.pusher.subscribe(
                    broadcast_channel_name)
                private_channel = self.pusher.subscribe(private_channel_name)
                for t_id in self.my_tournaments:
                    event_name = f'tournament_{t_id}'
                    broadcast_channel.bind(event_name, public_event_handler)
                    logging.info(
                        f"subscribed to public channel, event name: {event_name}, successfully")

                for private_event in private_events:
                    private_channel.bind(
                        private_event['name'], private_event_handler)
                    logging.info(
                        f"subscribed to private channel, event name: {private_event['name']}, successfully")

            self.pusher.connection.bind(
                'pusher:connection_established', connect_handler)
            self.pusher.connect()

    def get_balance(self):
        balance_url = urljoin(self.base_url, config.URL['mm_balance'])
        response = requests.get(balance_url, headers=self.__get_auth_header())
        if response.status_code != 200:
            logging.error("failed to get balance")
            return
        self.balance = json.loads(response.content).get(
            'data', {}).get('balance', 0)
        logging.info(f"still have ${self.balance} left")

    def start_betting(self):
        logging.info("Start betting, randomly :)")
        bet_url = urljoin(self.base_url, config.URL['mm_place_wager'])
        batch_bet_url = urljoin(self.base_url, config.URL['mm_batch_place'])
        if '.prophetbettingexchange' in bet_url:
            raise Exception(
                "only allowed to run in non production environment")
        for key in self.sport_events:
            one_event = self.sport_events[key]
            for market in one_event.get('markets', []):
                if market['type'] == 'moneyline':
                    # only bet on moneyline
                    if random.random() < 0.3:   # 30% chance to bet
                        for selection in market.get('selections', []):
                            if random.random() < 0.3:  # 30% chance to bet
                                odds_to_bet = self.__get_random_odds()
                                external_id = str(uuid.uuid1())
                                logging.info(
                                    f"going to bet on '{one_event['name']}' on moneyline, side {selection[0]['name']} with odds {odds_to_bet}")
                                body_to_send = {
                                    'external_id': external_id,
                                    'line_id': selection[0]['line_id'],
                                    'odds': odds_to_bet,
                                    'stake': 1.0
                                }
                                bet_response = requests.post(bet_url, json=body_to_send,
                                                             headers=self.__get_auth_header())
                                if bet_response.status_code != 200:
                                    logging.info(
                                        f"failed to bet, error {bet_response.content}")
                                else:
                                    logging.info("successfully")
                                    self.wagers[external_id] = json.loads(
                                        bet_response.content).get('data', {})['wager']['id']
                                # testing batch place wagers
                                batch_n = 3
                                external_id_batch = [
                                    str(uuid.uuid1()) for x in range(batch_n)]
                                batch_body_to_send = [{
                                    'external_id': external_id_batch[x],
                                    'line_id': selection[0]['line_id'],
                                    'odds': odds_to_bet,
                                    'stake': 1.0
                                } for x in range(batch_n)]
                                batch_bet_response = requests.post(batch_bet_url, json={"data": batch_body_to_send},
                                                                   headers=self.__get_auth_header())
                                if batch_bet_response.status_code != 200:
                                    logging.info(
                                        f"failed to bet, error {bet_response.content}")
                                else:
                                    logging.info("successfully")
                                    for wager in batch_bet_response.json()['data']['succeed_wagers']:
                                        self.wagers[wager['external_id']
                                                    ] = wager['id']

    def cancel_all_wagers(self):
        logging.info("CANCELLING ALL WAGERS")
        cancel_all_url = urljoin(
            self.base_url, config.URL['mm_cancel_all_wagers'])
        body = {}
        response = requests.post(
            cancel_all_url, json=body, headers=self.__get_auth_header())
        if response.status_code != 200:
            if response.status_code == 404:
                logging.info("already cancelled")
            else:
                logging.info("failed to cancel")
        else:
            logging.info("cancelled successfully")
            self.wagers = dict()

    def random_cancel_wager(self):
        wager_keys = list(self.wagers.keys())
        for key in wager_keys:
            if key not in self.wagers:
                # just in case already canceled by another thread
                continue
            wager_id = self.wagers[key]
            cancel_url = urljoin(self.base_url, config.URL['mm_cancel_wager'])
            if random.random() < 0.5:  # 50% cancel
                logging.info("start to cancel wager")
                body = {
                    'external_id': key,
                    'wager_id': wager_id,
                }
                response = requests.post(
                    cancel_url, json=body, headers=self.__get_auth_header())
                if response.status_code != 200:
                    if response.status_code == 404:
                        logging.info("already cancelled")
                        if key in self.wagers:
                            self.wagers.pop(key)
                    else:
                        logging.info("failed to cancel")
                else:
                    logging.info("cancelled successfully")
                    self.wagers.pop(key)

    def random_batch_cancel_wagers(self):
        wager_keys = list(self.wagers.keys())
        batch_keys_to_cancel = random.choices(
            wager_keys, k=min(4, len(wager_keys)))
        batch_cancel_body = [{'wager_id': self.wagers[x],
                              'external_id': x} for x in batch_keys_to_cancel]
        batch_cancel_url = urljoin(
            self.base_url, config.URL['mm_batch_cancel'])
        response = requests.post(batch_cancel_url, json={
                                 'data': batch_cancel_body}, headers=self.__get_auth_header())
        if response.status_code != 200:
            if response.status_code == 404:
                logging.info("already cancelled")
                [self.wagers.pop(x) for x in batch_keys_to_cancel]
            else:
                logging.info("failed to cancel")
        else:
            logging.info("cancelled successfully")
            for key in batch_keys_to_cancel:
                try:
                    self.wagers.pop(key)
                except Exception as e:
                    print(e)

    def schedule_in_thread(self):
        while True:
            schedule.run_pending()
            time.sleep(1)

    async def __auto_extend_session(self):
        refresh_url = urljoin(self.base_url, config.URL['mm_refresh'])
        response = requests.post(refresh_url, json={'refresh_token': self.mm_session['refresh_token']},
                                 headers=self.__get_auth_header())
        if response.status_code != 200:
            logging.info("Failed to call refresh endpoint")
        else:
            self.mm_session['access_token'] = response.json()[
                'data']['access_token']
            if self.pusher is not None:
                self.pusher.disconnect()
                self.pusher = None
            await self.subscribe()
            # need to subscribe again, as the old access token will expire soon
            # in real production implementation you would want to have two separate pusher objects, and subscribe
            # first before disconnect the other one
            # or use headersProvider provided by Pushser to auto extend session for you
            # https://pusher.com/docs/channels/using_channels/connection/#userauthenticationheadersprovider-203052782

    def auto_betting(self):
        logging.info("schedule to bet every 10 seconds!")
        schedule.every(5).seconds.do(self.start_betting)
        schedule.every(9).seconds.do(self.random_cancel_wager)
        schedule.every(7).seconds.do(self.random_batch_cancel_wagers)
        schedule.every(8).minutes.do(self.__auto_extend_session)
        # schedule.every(60).seconds.do(self.cancel_all_wagers)

        child_thread = threading.Thread(
            target=self.schedule_in_thread, daemon=False)
        child_thread.start()

    async def keep_alive(self):
        while True:
            await asyncio.sleep(1000)

    def __get_auth_header(self) -> dict:
        return {
            'Authorization': f'Bearer '
                             f'{self.mm_session["access_token"]}',
        }

    def __get_random_odds(self):
        odds = self.valid_odds[random.randint(0, len(self.valid_odds) - 1)]
        odds = odds if random.random() < 0.5 else -1 * odds
        if odds == -100:
            odds = 100
        return odds
