import time

import requests
import json
import pysher
import base64
import schedule
import random
import threading
import uuid

from urllib.parse import urljoin
from src import config
from src.log import logging
from src import constants


class MMInteractions:
    base_url: str = None
    balance: float = 0
    mm_keys: dict = dict()
    mm_session: dict = dict()
    all_tournaments: dict = dict()    # mapping from string to id
    my_tournaments: dict = dict()
    sport_events: dict = dict()   # key is event id, value is a list of event details and markets
    wagers: dict = dict()    # all wagers played in the session
    valid_odds: list = []
    pusher = None

    def __init__(self):
        self.base_url = config.BASE_URL
        self.mm_keys = config.MM_KEYS

    def mm_login(self) -> dict:
        """'
        Login to MM API, and store session keys in self.mm_session
        """
        login_url = urljoin(self.base_url, config.URL['mm_login'])
        request_body = {
            'access_key': self.mm_keys.get('access_key'),
            'secret_key': self.mm_keys.get('secret_key'),
        }
        response = requests.post(login_url, data=json.dumps(request_body))
        if response.status_code != 200:
            logging.debug(response)
            logging.debug("Please check your access key and secrete key to the user_info.json")
            raise Exception("login failed")
        mm_session = json.loads(response.content)['data']
        logging.info(mm_session)
        self.mm_session = mm_session
        logging.info("MM session started")
        return mm_session

    def seeding(self):
        """
        1. Get odds ladder, as only wagers having odds in the odds ladder are allowed.
        2. Get all tournaments/events and markets details of each event.
        :return:
        """
        logging.info("start to get odds ladder")
        odds_ladder_url = urljoin(self.base_url, config.URL['mm_odds_ladder'])
        odds_response = requests.get(odds_ladder_url, headers=self.__get_auth_header())
        if odds_response.status_code != 200:
            logging.info("not able to get valid odds from api, fall back to local constants")
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
            raise Exception("not able to seed tournaments")
        all_tournaments = json.loads(all_tournaments_response.content).get('data', {}).get('tournaments', {})
        self.all_tournaments = all_tournaments

        # get sport events and markets of each event
        event_url = urljoin(self.base_url, config.URL['mm_events'])
        multiple_markets_url = urljoin(self.base_url, config.URL['mm_multiple_markets'])
        for one_t in all_tournaments:
            if one_t['name'] in config.TOURNAMENTS_INTERESTED:
                self.my_tournaments[one_t['id']] = one_t
                events_response = requests.get(event_url, params={'tournament_id': one_t['id']}, headers=headers)
                if events_response.status_code == 200:
                    events = json.loads(events_response.content).get('data', {}).get('sport_events')
                    if events is None:
                        continue

                    event_ids = ','.join([str(event['event_id']) for event in events])
                    # instead of getting markets of one event at a time,
                    # using get_multiple_markets to batch get markets of a list of events
                    multiple_markets_response = requests.get(multiple_markets_url, params={'event_ids': event_ids},
                                                       headers=headers)
                    if multiple_markets_response.status_code == 200:
                        map_market_by_event_id = json.loads(multiple_markets_response.content).get('data', {})
                        for event in events:
                            if str(event['event_id']) not in map_market_by_event_id:
                                # this should not happen, mostly a bug
                                continue
                            event['markets'] = map_market_by_event_id[str(event['event_id'])]
                            self.sport_events[event['event_id']] = event
                            logging.info(f'successfully get markets of events {event["name"]}')
                    else:
                        logging.info(f'failed to get markets of events ids: {",".join([str(event["event_id"]) for event in events])}')
                else:
                    logging.info(f'skip tournament {one_t["name"]} as api request failed')

        logging.info("Done, seeding")
        logging.info(f"found {len(self.my_tournaments)} tournament, ingested {len(self.sport_events)} "
                     f"sport events from {len(config.TOURNAMENTS_INTERESTED)} tournaments")

    def _get_channels(self, socket_id: float):
        """
        Get a list of all channels and topics of each channel that you are allowing to subscribe to.
        Even though there are public and private channels, but the channel id is unique for each API user.
        """
        auth_endpoint_url = urljoin(self.base_url, config.URL['mm_auth'])
        channels_response = requests.post(auth_endpoint_url,
                                          data={'socket_id': socket_id},
                                          headers=self.__get_auth_header())
        if channels_response.status_code != 200:
            logging.error("failed to get channels")
            raise Exception("failed to get channels")
        channels = channels_response.json()
        return channels.get('data', {}).get('authorized_channel', [])

    def _get_connection_config(self):
        """
        Get websocket connection configurations. We are using Pusher as our websocket service,
        and only authenticated channels are used.
        More details can be found in https://pusher.com/docs/channels/using_channels/user-authentication/.

        The connection configuration is designed for stability and infrequent updates.
        However, in the unlikely event of a Pusher cluster incident,
        we will proactively migrate to a new cluster to ensure uninterrupted service.

        To maintain optimal connectivity, we recommend users retrieve the latest connection configuration
        at least once every thirty minutes. If the retrieved configuration remains unchanged,
         no further action is required. In the event of a new configuration being discovered,
         users should update their connection accordingly.
        """
        connection_config_url = urljoin(self.base_url, config.URL['websocket_config'])
        connection_response = requests.get(connection_config_url, headers=self.__get_auth_header())
        if connection_response.status_code != 200:
            logging.error("failed to get connection configs")
            raise Exception("failed to get channels")
        conn_configs = connection_response.json()
        return conn_configs

    def subscribe(self):
        """
        1. get Pusher connection configurations
        2. connect to Pusher websocket service by providing authentication credentials
        3. waiting for the websocket connection successful handshake and then execute connect_handler in the callback
        4. subscribe to public channels on selected topics
        5. subscribe to private channels on all topics
        """
        connection_config = self._get_connection_config()  # not working yet, getting wrong config
        key = connection_config['key']
        cluster = connection_config['cluster']
        
        auth_endpoint_url = urljoin(self.base_url, config.URL['mm_auth'])
        auth_header = self.__get_auth_header()
        auth_headers = {
                           "Authorization": auth_header['Authorization'],
                           "header-subscriptions": '''[{"type":"tournament","ids":[]}]''',
                       }
        self.pusher = pysher.Pusher(key=key, cluster=cluster,
                                    auth_endpoint=auth_endpoint_url,
                                    auth_endpoint_headers=auth_headers)

        def public_event_handler(*args, **kwargs):
            print("processing public, Args:", args)
            print(f"event details {base64.b64decode(json.loads(args[0]).get('payload', '{}'))}")
            print("processing public, Kwargs:", kwargs)

        def private_event_handler(*args, **kwargs):
            print("processing private, Args:", args)
            print(f"event details {base64.b64decode(json.loads(args[0]).get('payload', '{}'))}")
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
            broadcast_channel = self.pusher.subscribe(broadcast_channel_name)
            private_channel = self.pusher.subscribe(private_channel_name)
            for t_id in self.my_tournaments:
                event_name = f'tournament_{t_id}'
                broadcast_channel.bind(event_name, public_event_handler)
                logging.info(f"subscribed to public channel, event name: {event_name}, successfully")

            for private_event in private_events:
                private_channel.bind(private_event['name'], private_event_handler)
                logging.info(f"subscribed to private channel, event name: {private_event['name']}, successfully")

        self.pusher.connection.bind('pusher:connection_established', connect_handler)
        self.pusher.connect()

    def get_balance(self):
        balance_url = urljoin(self.base_url, config.URL['mm_balance'])
        response = requests.get(balance_url, headers=self.__get_auth_header())
        if response.status_code != 200:
            logging.error("failed to get balance")
            return
        self.balance = json.loads(response.content).get('data', {}).get('balance', 0)
        logging.info(f"still have ${self.balance} left")

    def start_playing(self):
        """
        Example on how to place wagers using single wager placement restufl api, place_wager,
         also batch wager placement restfu api place_multiple_wagers
        :return: Wager ids returned from the api are stored in a class object for wager cancellation example
        """
        logging.info("Start playing, randomly :)")
        play_url = urljoin(self.base_url, config.URL['mm_place_wager'])
        batch_play_url = urljoin(self.base_url, config.URL['mm_batch_place'])
        if '.prophetx.co' in play_url:
            raise Exception("only allowed to run in non production environment")
        for key in self.sport_events:
            one_event = self.sport_events[key]
            for market in one_event.get('markets', []):
                if market['type'] == 'moneyline':
                    # only play on moneyline
                    if random.random() < 0.3:   # 30% chance to play
                        for selection in market.get('selections', []):
                            if random.random() < 0.3: #30% chance to play
                                odds_to_play = self.__get_random_odds()
                                external_id = str(uuid.uuid1())
                                logging.info(f"going to play on '{one_event['name']}' on moneyline, side {selection[0]['name']} with odds {odds_to_play}")
                                body_to_send = {
                                    'external_id': external_id,
                                    'line_id': selection[0]['line_id'],
                                    'odds': odds_to_play,
                                    'stake': 1.0
                                }
                                play_response = requests.post(play_url, json=body_to_send,
                                                             headers=self.__get_auth_header())
                                if play_response.status_code != 200:
                                    logging.info(f"failed to play, error {play_response.content}")
                                else:
                                    logging.info("successfully")
                                    self.wagers[external_id] = json.loads(play_response.content).get('data', {})['wager']['id']
                                # testing batch place wagers
                                batch_n = 3
                                external_id_batch = [str(uuid.uuid1()) for x in range(batch_n)]
                                batch_body_to_send = [{
                                    'external_id': external_id_batch[x],
                                    'line_id': selection[0]['line_id'],
                                    'odds': odds_to_play,
                                    'stake': 1.0
                                } for x in range(batch_n)]
                                batch_play_response = requests.post(batch_play_url, json={"data": batch_body_to_send},
                                                                    headers=self.__get_auth_header())
                                if batch_play_response.status_code != 200:
                                    logging.info(f"failed to play, error {play_response.content}")
                                else:
                                    logging.info("successfully")
                                    for wager in batch_play_response.json()['data']['succeed_wagers']:
                                        self.wagers[wager['external_id']] = wager['id']

    def cancel_all_wagers(self):
        """
        Upon the event your side needs to hit a panic button and cancel all your open wagers,
         this api will help cancel all wagers without needing to providing individual wager ids.
        Only wagers placed prio this api call are cancelled, all new wagers after this call are not impacted.
        :return:
        """
        logging.info("CANCELLING ALL WAGERS")
        cancel_all_url = urljoin(self.base_url, config.URL['mm_cancel_all_wagers'])
        body = {}
        response = requests.post(cancel_all_url, json=body, headers=self.__get_auth_header())
        if response.status_code != 200:
            if response.status_code == 404:
                logging.info("already cancelled")
            else:
                logging.info("failed to cancel")
        else:
            logging.info("cancelled successfully")
            self.wagers = dict()

    def random_cancel_wager(self):
        """
        Example on how to cancel a single wager using cancel_wager endpoint
        :return:
        """
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
                response = requests.post(cancel_url, json=body, headers=self.__get_auth_header())
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
        """
        example on how to cancel a batch of wagers using cancel_multiple_wagers
        :return:
        """
        wager_keys = list(self.wagers.keys())
        batch_keys_to_cancel = random.choices(wager_keys, k=min(4, len(wager_keys)))
        batch_cancel_body = [{'wager_id': self.wagers[x],
                              'external_id': x} for x in batch_keys_to_cancel]
        batch_cancel_url = urljoin(self.base_url, config.URL['mm_batch_cancel'])
        response = requests.post(batch_cancel_url, json={'data': batch_cancel_body}, headers=self.__get_auth_header())
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

    def __run_forever_in_thread(self):
        while True:
            schedule.run_pending()
            time.sleep(1)

    def __auto_extend_session(self):
        """
         Renew the access token and reconnect the websocket.
         Security: To safeguard user sessions and system integrity,
                   access tokens are issued with a 20-minute expiration time.
         Continuous Connectivity: To ensure uninterrupted communication, a new access token can be obtained by
                                   utilizing the provided refresh token before the current token expires.
         Pusher Connection Handling Strategies:
            Automated Session Extension: For enhanced convenience, Pusher offers a headersProvider function
                      that automatically manages session extension for you.
                      Refer to the official documentation for details: https://pusher.com/docs/channels/using_channels/user-authentication/
            Dual Pusher Object Approach: Alternatively, you can implement a strategy utilizing two separate Pusher objects.
                 Before disconnecting the object associated with the expiring access token,
                 subscribe to the desired channels using a new Pusher object with the freshly acquired access token.
        :return:
        """
        refresh_url = urljoin(self.base_url, config.URL['mm_refresh'])
        response = requests.post(refresh_url, json={'refresh_token': self.mm_session['refresh_token']},
                                 headers=self.__get_auth_header())
        if response.status_code != 200:
            logging.info("Failed to call refresh endpoint")
        else:
            self.mm_session['access_token'] = response.json()['data']['access_token']
            if self.pusher is not None:
                self.pusher.disconnect()
                self.pusher = None
            self.subscribe()

    def auto_playing(self):
        logging.info("schedule to play every 10 seconds!")
        schedule.every(10).seconds.do(self.start_playing)
        schedule.every(9).seconds.do(self.random_cancel_wager)
        schedule.every(7).seconds.do(self.random_batch_cancel_wagers)
        schedule.every(8).minutes.do(self.__auto_extend_session)
        # schedule.every(60).seconds.do(self.cancel_all_wagers)

        child_thread = threading.Thread(target=self.__run_forever_in_thread, daemon=False)
        child_thread.start()

    def keep_alive(self):
        child_thread = threading.Thread(target=self.__run_forever_in_thread, daemon=False)
        child_thread.start()

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
